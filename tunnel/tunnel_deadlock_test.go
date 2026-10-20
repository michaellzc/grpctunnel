package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// Hold an acknowledgement until its caller has canceled the session request.
// This exercises cancellation over the real Register and Tunnel RPCs.
type delayedSessionAckServer struct {
	*Server
	once    sync.Once
	held    chan struct{}
	release chan struct{}
}

func (s *delayedSessionAckServer) Register(stream tpb.Tunnel_RegisterServer) error {
	return s.Server.Register(&delayedSessionAckStream{Tunnel_RegisterServer: stream, server: s})
}

type delayedSessionAckStream struct {
	tpb.Tunnel_RegisterServer
	server *delayedSessionAckServer
}

func (s *delayedSessionAckStream) Send(op *tpb.RegisterOp) error {
	if session := op.GetSession(); session != nil && session.Accept && session.Target == "cancel" {
		s.server.once.Do(func() {
			close(s.server.held)
			select {
			case <-s.server.release:
			case <-s.Context().Done():
			}
		})
	}
	return s.Tunnel_RegisterServer.Send(op)
}

func TestClientSessionAfterCanceledRequestWithLateAck(t *testing.T) {
	server, err := NewServer(ServerConfig{
		RegisterHandler: func(ServerSession) error { return nil },
		Handler: func(_ ServerSession, stream io.ReadWriteCloser) error {
			_, err := io.Copy(stream, stream)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	delayed := &delayedSessionAckServer{Server: server, held: make(chan struct{}), release: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	tpb.RegisterTunnelServer(grpcServer, delayed)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client, err := NewClient(tpb.NewTunnelClient(conn), ClientConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Register(ctx); err != nil {
		t.Fatal(err)
	}
	go client.Start(ctx)

	requestCtx, cancelRequest := context.WithCancel(ctx)
	t.Cleanup(cancelRequest)
	result := make(chan error, 1)
	go func() {
		stream, err := client.NewSessionContext(requestCtx, Target{ID: "cancel"})
		if stream != nil {
			stream.Close()
		}
		result <- err
	}()
	select {
	case <-delayed.held:
	case <-time.After(deadlockTimeout):
		t.Fatal("server did not acknowledge the session request")
	}
	cancelRequest()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled request: got %v, want context.Canceled", err)
		}
	case <-time.After(deadlockTimeout):
		t.Fatal("session request did not honor cancellation")
	}
	close(delayed.release)

	// The same registration must still open and carry traffic on new sessions.
	nextCtx, cancelNext := context.WithTimeout(ctx, deadlockTimeout)
	defer cancelNext()
	stream, err := client.NewSessionContext(nextCtx, Target{ID: "echo"})
	if err != nil {
		t.Fatalf("session after canceled request: %v", err)
	}
	defer stream.Close()
	go func() {
		const payload = "session after late acknowledgement"
		if _, err := io.WriteString(stream, payload); err != nil {
			result <- err
			return
		}
		buf := make([]byte, len(payload))
		_, err := io.ReadFull(stream, buf)
		if err == nil && string(buf) != payload {
			err = errors.New("echoed payload did not match")
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("new session did not carry traffic: %v", err)
		}
	case <-time.After(deadlockTimeout):
		t.Fatal("new session stalled while carrying traffic")
	}
}
