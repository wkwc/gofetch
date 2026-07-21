package fetch

import (
	"context"
	"errors"
	"io"
	"time"
)

// defaultBodyIdle is how long a range body may stall without any
// bytes before we treat the connection as dead (retryable).
const defaultBodyIdle = 60 * time.Second

// errBodyIdle is treated as transient by isTransient so workers retry.
var errBodyIdle = errors.New("body idle timeout: no data received")

// idleBody wraps an io.Reader with a per-read idle deadline. Each
// successful Read resets the timer. If no data arrives within idle,
// Read returns a transient error so the worker can requeue.
//
// Reads go into a private buffer so a timed-out goroutine cannot
// race with the caller's reuse of p.
type idleBody struct {
	r     io.Reader
	ctx   context.Context
	idle  time.Duration
	timer *time.Timer
}

func newIdleBody(ctx context.Context, r io.Reader, idle time.Duration) *idleBody {
	if idle <= 0 {
		idle = defaultBodyIdle
	}
	t := time.NewTimer(idle)
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(idle)
	return &idleBody{r: r, ctx: ctx, idle: idle, timer: t}
}

func (b *idleBody) Read(p []byte) (int, error) {
	type result struct {
		n   int
		err error
		buf []byte
	}
	// Private buffer avoids data race if we time out while the
	// underlying Read is still running.
	tmp := make([]byte, len(p))
	ch := make(chan result, 1)
	go func() {
		n, err := b.r.Read(tmp)
		ch <- result{n: n, err: err, buf: tmp}
	}()
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-b.timer.C:
		return 0, errBodyIdle
	case res := <-ch:
		if res.n > 0 {
			copy(p, res.buf[:res.n])
			if !b.timer.Stop() {
				select {
				case <-b.timer.C:
				default:
				}
			}
			b.timer.Reset(b.idle)
		}
		return res.n, res.err
	}
}

func (b *idleBody) Close() error {
	if b.timer != nil {
		b.timer.Stop()
	}
	if c, ok := b.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
