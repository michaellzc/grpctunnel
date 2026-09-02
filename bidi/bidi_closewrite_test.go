package bidi

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// halfCloser is an io.ReadWriteCloser with a CloseWrite that returns a fixed
// error, and a Read that blocks until released. It models one end of a tunnelled
// TCP connection.
type halfCloser struct {
	readBuf       *bytes.Buffer
	writeBuf      *bytes.Buffer
	closeWriteErr error

	block     chan struct{} // Read blocks on this before reporting EOF.
	closed    atomic.Bool
	closeOnce sync.Once
}

func newHalfCloser(payload string, closeWriteErr error) *halfCloser {
	return &halfCloser{
		readBuf:       bytes.NewBufferString(payload),
		writeBuf:      new(bytes.Buffer),
		closeWriteErr: closeWriteErr,
		block:         make(chan struct{}),
	}
}

func (h *halfCloser) Read(p []byte) (int, error) {
	if h.readBuf.Len() == 0 {
		<-h.block
		return 0, io.EOF
	}
	return h.readBuf.Read(p)
}

func (h *halfCloser) Write(p []byte) (int, error) {
	if h.closed.Load() {
		return 0, net.ErrClosed
	}
	return h.writeBuf.Write(p)
}

func (h *halfCloser) Close() error {
	h.closed.Store(true)
	h.closeOnce.Do(func() { close(h.block) })
	return nil
}

func (h *halfCloser) CloseWrite() error { return h.closeWriteErr }

func (h *halfCloser) release() { h.closeOnce.Do(func() { close(h.block) }) }

// TestCopySpentCloseWriteDoesNotTruncate covers the truncation that reaches
// tunnelled HTTPS clients as "unexpected eof while reading": one direction
// finishes and its CloseWrite reports the peer is already gone, which must not
// tear down the opposite direction while it is still streaming.
func TestCopySpentCloseWriteDoesNotTruncate(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"enotconn", syscall.ENOTCONN},
		{"wrapped enotconn", &net.OpError{Op: "close", Err: syscall.ENOTCONN}},
		{"epipe", syscall.EPIPE},
		{"net closed", net.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// spent drains immediately; its CloseWrite reports a dead peer.
			spent := newHalfCloser("", tc.err)
			spent.release()
			// streaming is still delivering a response body.
			streaming := newHalfCloser("the whole response body", nil)

			done := make(chan error, 1)
			go func() { done <- Copy(spent, streaming) }()

			// Copy must still be running: the streaming side has not finished.
			select {
			case err := <-done:
				t.Fatalf("Copy returned early with %v, truncating the stream", err)
			case <-time.After(150 * time.Millisecond):
			}

			streaming.release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Copy() = %v, want nil", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Copy did not return")
			}

			if got := spent.writeBuf.String(); got != "the whole response body" {
				t.Fatalf("body truncated: got %q", got)
			}
		})
	}
}

// TestCopyRealCloseWriteErrorIsTerminal keeps the teardown behaviour for a
// CloseWrite failure that is not a spent connection.
func TestCopyRealCloseWriteErrorIsTerminal(t *testing.T) {
	boom := errors.New("boom")
	// drained EOFs immediately, so the copy into other reaches CloseWrite.
	drained := newHalfCloser("", nil)
	drained.release()
	// other never finishes reading, and its CloseWrite genuinely fails.
	other := newHalfCloser("", boom)
	defer other.release()

	done := make(chan error, 1)
	go func() { done <- Copy(drained, other) }()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("Copy() = %v, want %v", err, boom)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Copy did not return on a terminal error")
	}
}

func TestIsSpentConn(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"enotconn", syscall.ENOTCONN, true},
		{"epipe", syscall.EPIPE, true},
		{"net closed", net.ErrClosed, true},
		{"wrapped enotconn", &net.OpError{Op: "close", Err: syscall.ENOTCONN}, true},
		{"real failure", errors.New("boom"), false},
		{"econnrefused", syscall.ECONNREFUSED, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSpentConn(tc.err); got != tc.want {
				t.Fatalf("isSpentConn(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
