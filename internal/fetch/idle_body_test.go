package fetch

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type stallReader struct {
	n    atomic.Int32
	wait time.Duration
}

func (s *stallReader) Read(_ []byte) (int, error) {
	if s.n.Add(1) == 1 {
		// First read stalls longer than the idle timeout.
		time.Sleep(s.wait)
		return 0, io.EOF
	}
	return 0, io.EOF
}

func (s *stallReader) Close() error { return nil }

func TestIdleBodyTimeout(t *testing.T) {
	r := &stallReader{wait: 200 * time.Millisecond}
	ctx := context.Background()
	body := newIdleBody(ctx, r, 50*time.Millisecond)
	buf := make([]byte, 16)
	n, err := body.Read(buf)
	if n != 0 {
		t.Fatalf("n=%d, want 0", n)
	}
	if !errors.Is(err, errBodyIdle) {
		t.Fatalf("err=%v, want errBodyIdle", err)
	}
	_ = body.Close()
}

func TestIdleBodySuccessResets(t *testing.T) {
	ctx := context.Background()
	body := newIdleBody(ctx, io.NopCloser(strings.NewReader("hello")), 200*time.Millisecond)
	buf := make([]byte, 16)
	n, err := body.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("got %q", buf[:n])
	}
	_ = body.Close()
}

func TestIsTransientBodyIdle(t *testing.T) {
	if !isTransient(errBodyIdle) {
		t.Fatal("errBodyIdle should be transient")
	}
}

// fakeConn is an io.Reader that also implements connDeadliner, so
// tryConnDeadline routes idleBody onto the SetReadDeadline fast path
// (useGo=false) instead of the helper goroutine.
type fakeConn struct {
	readFn func([]byte) (int, error)
}

func (f *fakeConn) Read(p []byte) (int, error)      { return f.readFn(p) }
func (f *fakeConn) SetReadDeadline(time.Time) error { return nil }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIdleBodyDeadlinePathSuccess(t *testing.T) {
	f := &fakeConn{readFn: func(p []byte) (int, error) { return copy(p, "hello"), io.EOF }}
	body := newIdleBody(context.Background(), f, 200*time.Millisecond)
	buf := make([]byte, 16)
	n, err := body.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Errorf("got %q, want %q", buf[:n], "hello")
	}
	if err != io.EOF {
		t.Errorf("err = %v, want io.EOF", err)
	}
	_ = body.Close()
}

func TestIdleBodyDeadlinePathTimeout(t *testing.T) {
	f := &fakeConn{readFn: func([]byte) (int, error) { return 0, timeoutErr{} }}
	body := newIdleBody(context.Background(), f, 200*time.Millisecond)
	if _, err := body.Read(make([]byte, 16)); !errors.Is(err, errBodyIdle) {
		t.Errorf("err = %v, want errBodyIdle (deadline path)", err)
	}
	_ = body.Close()
}

func TestIdleBodyDeadlinePathPlainError(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeConn{readFn: func([]byte) (int, error) { return 0, boom }}
	body := newIdleBody(context.Background(), f, 200*time.Millisecond)
	if _, err := body.Read(make([]byte, 16)); !errors.Is(err, boom) {
		t.Errorf("err = %v, want %v", err, boom)
	}
	_ = body.Close()
}

func TestIdleBodyDeadlinePathCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeConn{readFn: func([]byte) (int, error) { return 0, io.EOF }}
	body := newIdleBody(ctx, f, 200*time.Millisecond)
	cancel()
	if _, err := body.Read(make([]byte, 16)); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	_ = body.Close()
}
