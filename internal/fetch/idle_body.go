package fetch

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// defaultBodyIdle is how long a range body may stall without any
// bytes before we treat the connection as dead (retryable).
const defaultBodyIdle = 60 * time.Second

// errBodyIdle is treated as transient by isTransient so workers retry.
var errBodyIdle = errors.New("body idle timeout: no data received")

// connDeadliner is implemented by net.Conn (and some transport bodies).
type connDeadliner interface {
	SetReadDeadline(t time.Time) error
}

// idleBody wraps an io.Reader with a per-read idle deadline.
//
// Prefer SetReadDeadline on an underlying net.Conn when available — that
// path has no per-Read goroutine. Fall back to a single long-lived helper
// goroutine + buffer for bodies that do not expose a deadline (most
// http.Response.Body types still do via the transport connection when
// unwrapped; see tryConnDeadline).
type idleBody struct {
	r         io.Reader
	ctx       context.Context
	idle      time.Duration
	closeOnce sync.Once
	closeErr  error

	// deadline path
	conn connDeadliner

	// goroutine path (fallback)
	mu    sync.Mutex
	timer *time.Timer
	useGo bool
	// long-lived reader
	started bool
	reqCh   chan readReq
	stopCh  chan struct{}
	// drainTimer bounds how long forceClose waits for a mid-flight helper
	// read after ctx/timer cancellation, so no per-Read time.After churn.
	drainTimer *time.Timer
}

type readReq struct {
	buf []byte
	res chan readResult
}

type readResult struct {
	n   int
	err error
}

func tryConnDeadline(r io.Reader) connDeadliner {
	// Common unwrap patterns for net/http bodies.
	type unwrapper interface{ Unwrap() io.Reader }
	seen := map[io.Reader]struct{}{}
	cur := r
	for i := 0; i < 8 && cur != nil; i++ {
		if _, ok := seen[cur]; ok {
			break
		}
		seen[cur] = struct{}{}
		if c, ok := cur.(connDeadliner); ok {
			return c
		}
		if c, ok := cur.(net.Conn); ok {
			return c
		}
		if u, ok := cur.(unwrapper); ok {
			cur = u.Unwrap()
			continue
		}
		// *io.LimitedReader
		if lr, ok := cur.(*io.LimitedReader); ok {
			cur = lr.R
			continue
		}
		break
	}
	return nil
}

// stopDrain stops timer t, draining any already-fired value so the
// timer can be Reset or reused without a stale fire firing later.
func stopDrain(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func newIdleBody(ctx context.Context, r io.Reader, idle time.Duration) *idleBody {
	if idle <= 0 {
		idle = defaultBodyIdle
	}
	b := &idleBody{r: r, ctx: ctx, idle: idle, conn: tryConnDeadline(r)}
	if b.conn == nil {
		b.useGo = true
		b.timer = time.NewTimer(idle)
		stopDrain(b.timer)
		b.timer.Reset(idle)
		b.reqCh = make(chan readReq, 1)
		b.stopCh = make(chan struct{})
		b.drainTimer = time.NewTimer(2 * time.Second)
		stopDrain(b.drainTimer)
	}
	return b
}

func (b *idleBody) forceClose() {
	b.closeOnce.Do(func() {
		if c, ok := b.r.(io.Closer); ok {
			b.closeErr = c.Close()
		}
		if b.useGo && b.stopCh != nil {
			select {
			case <-b.stopCh:
			default:
				close(b.stopCh)
			}
		}
	})
}

func (b *idleBody) startHelper() {
	if b.started {
		return
	}
	b.started = true
	go func() {
		for {
			select {
			case <-b.stopCh:
				return
			case <-b.ctx.Done():
				return
			case req, ok := <-b.reqCh:
				if !ok {
					return
				}
				n, err := b.r.Read(req.buf)
				req.res <- readResult{n: n, err: err}
			}
		}
	}()
}

func (b *idleBody) Read(p []byte) (int, error) {
	if !b.useGo {
		// Deadline path: no goroutine.
		if err := b.ctx.Err(); err != nil {
			return 0, err
		}
		_ = b.conn.SetReadDeadline(time.Now().Add(b.idle))
		n, err := b.r.Read(p)
		// Clear deadline so Close / later ops are not cut short.
		_ = b.conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				b.forceClose()
				return 0, errBodyIdle
			}
			if b.ctx.Err() != nil {
				return 0, b.ctx.Err()
			}
		}
		return n, err
	}

	// Fallback: one long-lived reader goroutine (not per-Read).
	b.mu.Lock()
	defer b.mu.Unlock()
	b.startHelper()

	tmp := acquireBuf(len(p))
	resCh := make(chan readResult, 1)
	select {
	case <-b.ctx.Done():
		releaseBuf(tmp)
		b.forceClose()
		return 0, b.ctx.Err()
	case b.reqCh <- readReq{buf: tmp, res: resCh}:
	case <-b.stopCh:
		releaseBuf(tmp)
		return 0, io.ErrClosedPipe
	}

	stopDrain(b.timer)
	b.timer.Reset(b.idle)

	select {
	case <-b.ctx.Done():
		b.forceClose()
		if !b.drainResult(resCh, tmp) {
			return 0, b.ctx.Err()
		}
		return 0, b.ctx.Err()
	case <-b.timer.C:
		b.forceClose()
		if !b.drainResult(resCh, tmp) {
			return 0, errBodyIdle
		}
		return 0, errBodyIdle
	case res := <-resCh:
		if res.n > 0 {
			copy(p, tmp[:res.n])
		}
		releaseBuf(tmp)
		return res.n, res.err
	}
}

// drainResult waits up to 2s (reusing b.drainTimer) for the helper
// goroutine's response to a cancelled read, then releases tmp. It
// returns false when the timeout won, meaning the caller should stop
// the read rather than trust resCh.
func (b *idleBody) drainResult(resCh chan readResult, tmp []byte) bool {
	stopDrain(b.drainTimer)
	b.drainTimer.Reset(2 * time.Second)
	select {
	case <-resCh:
		releaseBuf(tmp)
		return true
	case <-b.drainTimer.C:
		releaseBuf(tmp)
		return false
	}
}

func (b *idleBody) Close() error {
	if b.useGo {
		b.mu.Lock()
		defer b.mu.Unlock()
	}
	b.forceClose()
	if b.timer != nil {
		b.timer.Stop()
	}
	if b.drainTimer != nil {
		b.drainTimer.Stop()
	}
	return b.closeErr
}
