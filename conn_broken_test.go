package netlink_test

import (
	"errors"
	"iter"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mdlayher/netlink"
	"github.com/mdlayher/netlink/nltest"
)

// recvStep is one scripted datagram (or error) returned by fakeSocket on each
// receive from the socket.
type recvStep struct {
	msgs []netlink.Message
	err  error
}

// fakeSocket is a netlink.Socket which replays a scripted sequence of
// datagrams and/or errors, enabling deterministic reproduction of races such
// as read timeouts and interrupted multi-part exchanges.
type fakeSocket struct {
	mu    sync.Mutex
	steps []recvStep
}

func (s *fakeSocket) Close() error                         { return nil }
func (s *fakeSocket) Send(netlink.Message) error           { return nil }
func (s *fakeSocket) SendMessages([]netlink.Message) error { return nil }

func (s *fakeSocket) Receive() ([]netlink.Message, error) {
	var msgs []netlink.Message
	for m, err := range s.ReceiveIter() {
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (s *fakeSocket) ReceiveIter() iter.Seq2[netlink.Message, error] {
	return func(yield func(netlink.Message, error) bool) {
		s.mu.Lock()
		if len(s.steps) == 0 {
			s.mu.Unlock()
			return
		}
		step := s.steps[0]
		s.steps = s.steps[1:]
		s.mu.Unlock()

		if step.err != nil {
			yield(netlink.Message{}, step.err)
			return
		}

		for _, m := range step.msgs {
			if !yield(m, nil) {
				return
			}
		}
	}
}

// errTimeout simulates a read deadline exceeded error from recvmsg.
var errTimeout = os.NewSyscallError("recvmsg", syscall.ETIMEDOUT)

// mustBeBroken verifies that Execute, Send, and Receive on c all fail with
// ErrConnBroken.
func mustBeBroken(t *testing.T, c *netlink.Conn) {
	t.Helper()

	if _, err := c.Execute(netlink.Message{}); !errors.Is(err, netlink.ErrConnBroken) {
		t.Fatalf("expected Execute to fail with ErrConnBroken, got: %v", err)
	}
	if _, err := c.Send(netlink.Message{}); !errors.Is(err, netlink.ErrConnBroken) {
		t.Fatalf("expected Send to fail with ErrConnBroken, got: %v", err)
	}
	if _, err := c.Receive(); !errors.Is(err, netlink.ErrConnBroken) {
		t.Fatalf("expected Receive to fail with ErrConnBroken, got: %v", err)
	}
}

func TestConnExecuteTimeoutPoisonsConn(t *testing.T) {
	// Reproduce: a read timeout in the middle of a short request/response
	// exchange. A late reply may still arrive afterwards, so the connection
	// must not be reused for further exchanges.
	sock := &fakeSocket{steps: []recvStep{
		{err: errTimeout},
		// The late reply to the first request, which a buggy implementation
		// would misinterpret as the reply to the second request.
		{msgs: []netlink.Message{{Header: netlink.Header{Type: 0x10}}}},
	}}

	c := netlink.NewConn(sock, 1)
	defer c.Close()

	_, err := c.Execute(netlink.Message{
		Header: netlink.Header{Flags: netlink.Request | netlink.Acknowledge},
	})
	if err == nil {
		t.Fatal("expected an error, but none occurred")
	}

	// The error must be identifiable as a transport-level timeout.
	nerr, ok := err.(net.Error)
	if !ok || !nerr.Timeout() {
		t.Fatalf("expected a timeout net.Error, got: %v", err)
	}

	// The connection must now refuse further work rather than read the
	// stale reply above.
	mustBeBroken(t, c)
}

func TestConnExecuteMultipartInterruptedPoisonsConn(t *testing.T) {
	// Reproduce: a multi-part response is interrupted (timeout, signal)
	// before its terminating Done message arrives, leaving the remaining
	// parts queued in the socket buffer.
	sock := &fakeSocket{steps: []recvStep{
		{msgs: []netlink.Message{{
			Header: netlink.Header{Type: 0x10, Flags: netlink.Multi},
			Data:   []byte{0x01},
		}}},
		{err: errTimeout},
		// The leftover tail of the interrupted multi-part response.
		{msgs: []netlink.Message{{
			Header: netlink.Header{Type: netlink.Done, Flags: netlink.Multi},
		}}},
	}}

	c := netlink.NewConn(sock, 1)
	defer c.Close()

	_, err := c.Execute(netlink.Message{
		Header: netlink.Header{Flags: netlink.Request | netlink.Dump},
	})
	if err == nil {
		t.Fatal("expected an error, but none occurred")
	}

	// The next exchange must not read the leftover Done message.
	mustBeBroken(t, c)
}

func TestConnExecuteMismatchedSequencePoisonsConn(t *testing.T) {
	// A reply whose sequence number does not match the request must surface
	// as a validation error, never as a successful exchange.
	c := nltest.Dial(func(req []netlink.Message) ([]netlink.Message, error) {
		res := req[0]
		res.Header.Type = 0x10
		res.Header.Sequence++
		return []netlink.Message{res}, nil
	})
	defer c.Close()

	_, err := c.Execute(netlink.Message{
		Header: netlink.Header{Flags: netlink.Request},
	})
	if err == nil || !strings.Contains(err.Error(), "mismatched sequence") {
		t.Fatalf("expected mismatched sequence error, got: %v", err)
	}

	mustBeBroken(t, c)
}

func TestConnExecuteMismatchedPIDPoisonsConn(t *testing.T) {
	c := nltest.Dial(func(req []netlink.Message) ([]netlink.Message, error) {
		res := req[0]
		res.Header.Type = 0x10
		res.Header.PID = 9999
		return []netlink.Message{res}, nil
	})
	defer c.Close()

	_, err := c.Execute(netlink.Message{
		Header: netlink.Header{Flags: netlink.Request},
	})
	if err == nil || !strings.Contains(err.Error(), "mismatched PID") {
		t.Fatalf("expected mismatched PID error, got: %v", err)
	}

	mustBeBroken(t, c)
}

func TestConnExecutePeerErrorDoesNotPoisonConn(t *testing.T) {
	// An NLMSG_ERROR from the peer is a complete, terminal response to the
	// request: no residue remains in the socket buffer, so the connection
	// must remain usable for subsequent exchanges.
	c := nltest.Dial(func(req []netlink.Message) ([]netlink.Message, error) {
		return nltest.Error(int(syscall.ENOENT), req)
	})
	defer c.Close()

	for i := 0; i < 2; i++ {
		_, err := c.Execute(netlink.Message{
			Header: netlink.Header{Flags: netlink.Request | netlink.Acknowledge},
		})
		if err == nil {
			t.Fatal("expected an error, but none occurred")
		}
		if errors.Is(err, netlink.ErrConnBroken) {
			t.Fatalf("peer NLMSG_ERROR must not poison the connection: %v", err)
		}

		var oerr *netlink.OpError
		if !errors.As(err, &oerr) || oerr.Op != "receive" {
			t.Fatalf("expected receive OpError, got: %#v", err)
		}
	}
}

func TestConnReceiveIterEarlyExitDoesNotPoisonConn(t *testing.T) {
	// Stopping iteration of a multi-part response early drains the remaining
	// messages, so the connection stays usable for normal short requests.
	var sent bool
	c := nltest.Dial(func(req []netlink.Message) ([]netlink.Message, error) {
		if !sent {
			// Multicast-style receive: a multi-part response.
			return nltest.Multipart([]netlink.Message{
				{Data: []byte{0x01}},
				{Data: []byte{0x02}},
				{},
			})
		}

		// Normal short request: echo the request back as its own reply.
		return req, nil
	})
	defer c.Close()

	for range c.ReceiveIter() {
		break
	}

	sent = true
	if _, err := c.Execute(netlink.Message{
		Header: netlink.Header{Flags: netlink.Request},
	}); err != nil {
		t.Fatalf("expected Execute to succeed after early exit, got: %v", err)
	}
}
