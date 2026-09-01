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
