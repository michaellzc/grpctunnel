package tunnel

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/peer"

	tpb "github.com/openconfig/grpctunnel/proto/tunnel"
)

// These are regression tests for a registration-leak deadlock: a session
// response delivered after its handleSession waiter abandoned retCh (context
// cancelation) must never block the sender. The senders are the client's
// Register goroutine and Tunnel stream handlers; blocking the Register
// goroutine prevents its deferred cleanup from ever running, so the client's
// target registrations outlive the connection and permanently lock the
// target ID out of re-registration.

const deadlockTimeout = 5 * time.Second

// abandonedConnection registers a connection whose buffered response slot is
// already taken and which has no reader, emulating a waiter that gave up
// after a response was delivered.
func abandonedConnection(t *testing.T, s *Server, tag int32, addr net.Addr) {
	t.Helper()
	retCh := make(chan ioOrErr, 1)
	retCh <- ioOrErr{}
	if err := s.addConnection(tag, addr, retCh); err != nil {
		t.Fatalf("failed to add connection to test server: %v", err)
	}
}

func TestNewClientSessionErrorDoesNotBlockWithoutReceiver(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatalf("NewServer(ServerConfig{}) failed: %v", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45000")
	if err != nil {
		t.Fatalf("failed to resolve address: %v", err)
	}
	abandonedConnection(t, s, 1, addr)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.newClientSession(context.Background(), &tpb.Session{Tag: 1, Error: "session failed"}, addr, &regSafeStream{})
	}()
	select {
	case <-done:
	case <-time.After(deadlockTimeout):
		t.Fatal("newClientSession blocked delivering a session error with no receiver; this wedges the client's Register goroutine")
	}
}

func TestServerTunnelDoesNotBlockWithoutReceiver(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatalf("NewServer(ServerConfig{}) failed: %v", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45000")
	if err != nil {
		t.Fatalf("failed to resolve address: %v", err)
	}
	abandonedConnection(t, s, 1, addr)

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
	done := make(chan error, 1)
	go func() {
		done <- s.Tunnel(&testDataStream{ctx: ctx, data: &tpb.Data{Tag: 1}})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Tunnel() want error for abandoned session, got nil")
		}
	case <-time.After(deadlockTimeout):
		t.Fatal("Tunnel blocked delivering a stream with no receiver")
	}
}

func TestHandleSessionAbandonReleasesConnection(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatalf("NewServer(ServerConfig{}) failed: %v", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45000")
	if err != nil {
		t.Fatalf("failed to resolve address: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.handleSession(ctx, 7, addr, Target{ID: "target1"}, &registerTestStream{maxSends: 10}); err == nil {
		t.Fatal("handleSession() want context error, got nil")
	}
	if ch := s.connection(7, addr); ch != nil {
		t.Fatal("connection for abandoned session still registered; late responses would be orphaned")
	}
}
