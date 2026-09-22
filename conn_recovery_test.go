package netlink

import (
	"errors"
	"io"
	"iter"
	"os"
	"sync"
	"testing"

	"github.com/mdlayher/netlink/nlenc"
)

type scriptReceiveStep struct {
	msgs []Message
	err  error
}

type scriptSocket struct {
	mu sync.Mutex

	sends int
	recv  int
	steps []scriptReceiveStep
}

func (s *scriptSocket) Close() error { return nil }

func (s *scriptSocket) Send(Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	return nil
}

func (s *scriptSocket) SendMessages([]Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	return nil
}

func (s *scriptSocket) Receive() ([]Message, error) {
	var msgs []Message
	for msg, err := range s.ReceiveIter() {
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (s *scriptSocket) ReceiveIter() iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		s.mu.Lock()
		i := s.recv
		s.recv++
		s.mu.Unlock()

		if i >= len(s.steps) {
			yield(Message{}, io.ErrUnexpectedEOF)
			return
		}

		step := s.steps[i]
		if step.err != nil {
			yield(Message{}, step.err)
			return
		}

		for _, msg := range step.msgs {
			if !yield(msg, nil) {
				return
			}
		}
	}
}

func TestConnExecuteReadTimeoutLeavesConnectionUnusable(t *testing.T) {
	sock := &scriptSocket{steps: []scriptReceiveStep{
		{err: os.ErrDeadlineExceeded},
		{msgs: []Message{{Header: Header{Sequence: 2, PID: 1}}}},
	}}
	c := NewConn(sock, 1)

	req := Message{Header: Header{Sequence: 1, PID: 1}}
	_, err := c.Execute(req)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}

	var opErr *OpError
	if !errors.As(err, &opErr) || !opErr.Timeout() {
		if opErr == nil {
			t.Fatalf("expected OpError, got %T", err)
		}
		t.Fatalf("expected transport timeout, got OpError timeout=%v: %v", opErr.Timeout(), err)
	}

	_, err = c.Execute(Message{Header: Header{Sequence: 2, PID: 1}})
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("expected unusable connection, got %v", err)
	}

	if sock.recv != 1 {
		t.Fatalf("residual response was read %d times, want 0", sock.recv-1)
	}
}

func TestConnExecuteHalfPacketLeavesConnectionUnusable(t *testing.T) {
	sock := &scriptSocket{steps: []scriptReceiveStep{
		{msgs: []Message{{Header: Header{Length: 32}}}},
		{msgs: []Message{{Header: Header{Sequence: 2, PID: 1}}}},
	}}
	c := NewConn(sock, 1)

	_, err := c.Execute(Message{Header: Header{Sequence: 1, PID: 1}})
	if !errors.Is(err, errShortMessage) {
		t.Fatalf("expected short message error, got %v", err)
	}

	_, err = c.Execute(Message{Header: Header{Sequence: 2, PID: 1}})
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("expected unusable connection, got %v", err)
	}

	if sock.recv != 1 {
		t.Fatalf("residual response was read %d times, want 0", sock.recv-1)
	}
}

func TestConnReceiveIterCanceledMultipartLeavesConnectionUnusable(t *testing.T) {
	sock := &scriptSocket{steps: []scriptReceiveStep{
		{msgs: []Message{{Header: Header{Length: 16, Sequence: 1, PID: 1, Flags: Multi}}}},
		{msgs: []Message{
			{Header: Header{Length: 16, Sequence: 1, PID: 1, Flags: Multi}},
			{Header: Header{Length: 16, Sequence: 1, PID: 1, Type: Done, Flags: Multi}},
		}},
	}}
	c := NewConn(sock, 1)

	for range c.ReceiveIter() {
		break
	}

	_, err := c.Execute(Message{Header: Header{Sequence: 2, PID: 1}})
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("expected unusable connection, got %v", err)
	}

	if sock.recv != 1 {
		t.Fatalf("multipart tail was read %d times, want 0", sock.recv-1)
	}
}

func TestConnExecuteStaleAcknowledgementFails(t *testing.T) {
	sock := &scriptSocket{steps: []scriptReceiveStep{
		{msgs: []Message{{
			Header: Header{Length: 16, Type: Error, Sequence: 99, PID: 1},
			Data:   make([]byte, 4),
		}}},
	}}
	c := NewConn(sock, 1)

	_, err := c.Execute(Message{Header: Header{Sequence: 1, PID: 1}})
	if !errors.Is(err, errMismatchedSequence) {
		t.Fatalf("expected mismatched sequence, got %v", err)
	}

	_, err = c.Execute(Message{Header: Header{Sequence: 2, PID: 1}})
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("expected unusable connection, got %v", err)
	}
}

func TestConnExecuteDistinguishesTransportAndPeerErrors(t *testing.T) {
	peerError := Message{
		Header: Header{Length: 16, Type: Error, Sequence: 1, PID: 1},
		Data:   nlenc.Int32Bytes(-1),
	}

	sock := &scriptSocket{steps: []scriptReceiveStep{
		{msgs: []Message{peerError}},
		{msgs: []Message{{
			Header: Header{Length: 16, Type: Error, Sequence: 2, PID: 1},
			Data:   make([]byte, 4),
		}}},
	}}
	c := NewConn(sock, 1)

	_, err := c.Execute(Message{Header: Header{Sequence: 1, PID: 1}})
	var opErr *OpError
	if !errors.As(err, &opErr) || !opErr.NLMSGError || opErr.Timeout() {
		t.Fatalf("expected peer NLMSG_ERROR, got %v", err)
	}

	if _, err := c.Execute(Message{Header: Header{Sequence: 2, PID: 1}}); err != nil {
		t.Fatalf("peer error should not poison the connection: %v", err)
	}
}
