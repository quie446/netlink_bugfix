//go:build linux

package netlink_test

import (
	"errors"
	"net"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/mdlayher/netlink"
	"github.com/mdlayher/netlink/nltest"
)

func TestConnExecuteErrorDistinctionLinux(t *testing.T) {
	// A transport-level timeout and a peer-sent NLMSG_ERROR must be
	// distinguishable by the caller.
	t.Run("timeout", func(t *testing.T) {
		sock := &fakeSocket{steps: []recvStep{{err: errTimeout}}}
		c := netlink.NewConn(sock, 1)
		defer c.Close()

		_, err := c.Execute(netlink.Message{
			Header: netlink.Header{Flags: netlink.Request | netlink.Acknowledge},
		})

		var nerr net.Error
		if !errors.As(err, &nerr) || !nerr.Timeout() {
			t.Fatalf("expected timeout net.Error, got: %v", err)
		}
		if !errors.Is(err, unix.ETIMEDOUT) {
			t.Fatalf("expected ETIMEDOUT, got: %v", err)
		}
		if errors.Is(err, unix.ENOENT) {
			t.Fatalf("timeout must not match a netlink errno: %v", err)
		}
	})

	t.Run("nlmsg error", func(t *testing.T) {
		c := nltest.Dial(func(req []netlink.Message) ([]netlink.Message, error) {
			return nltest.Error(int(unix.ENOENT), req)
		})
		defer c.Close()

		_, err := c.Execute(netlink.Message{
			Header: netlink.Header{Flags: netlink.Request | netlink.Acknowledge},
		})

		if !errors.Is(err, unix.ENOENT) {
			t.Fatalf("expected ENOENT, got: %v", err)
		}
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			t.Fatalf("NLMSG_ERROR must not report as a timeout: %v", err)
		}
	})
}
