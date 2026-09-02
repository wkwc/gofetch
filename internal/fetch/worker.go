package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// retryAfterDefault is the backoff applied when a retryable HTTP
// response omits a usable Retry-After header.
const retryAfterDefault = 5 * time.Second

// errRangeNotSupported is returned when a Range GET receives a non-partial
// response (typically HTTP 200 OK) despite the server advertising
// Accept-Ranges. Callers fall back to single-stream rather than treating
// the failure as fatal. Partial range writes must not be marked complete.
var errRangeNotSupported = errors.New("server does not honor Range requests")

// workerState is the live state of one worker goroutine.
// All fields are atomic for lock-free reads from the monitor.
type workerState struct {
	curTask   atomic.Pointer[Task]
	bytesDone atomic.Int64
	startedAt atomic.Int64
	taskGen   atomic.Uint64
	cancelFn  atomic.Pointer[context.CancelFunc]
	stealFlag atomic.Bool
	errVal    atomic.Pointer[error]
}

func newWorkerState() *workerState { return &workerState{} }

func (ws *workerState) setErr(err error) {
	if err == nil {
		return
	}
	e := err
	ws.errVal.CompareAndSwap(nil, &e)
}

func (ws *workerState) err() (error, bool) {
	p := ws.errVal.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}

func (ws *workerState) reset(task Task) {
	taskRef := task
	ws.curTask.Store(&taskRef)
	ws.bytesDone.Store(0)
	ws.stealFlag.Store(false)
	ws.startedAt.Store(time.Now().UnixNano())
	// bytesDone is stored BEFORE taskGen so the steal plan's re-check
	// at monitor.stealPlan cannot observe a stale (old task) bytesDone
	// paired with a new taskGen: either both old or both new are visible.
	ws.taskGen.Add(1)
}

func isTransient(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errBodyIdle) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	switch {
	case errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.ECONNABORTED),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.ETIMEDOUT),
		errors.Is(err, syscall.EAGAIN),
		errors.Is(err, syscall.EWOULDBLOCK),
		errors.Is(err, syscall.ENOBUFS),
		errors.Is(err, syscall.ENOMEM):
		return true
	}
	return false
}

func isRetryableHTTP(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusServiceUnavailable,
		http.StatusBadGateway,
		http.StatusGatewayTimeout,
		http.StatusRequestTimeout,
		http.StatusInternalServerError,
		http.StatusInsufficientStorage:
		return true
	}
	return false
}

// isIdentity treats anything other than the empty token or "identity" as
// real compression that would corrupt Range past-the-end math.
func isIdentity(enc string) bool {
	return enc == "" || enc == "identity"
}

// parseRetryAfter parses the RFC 7231 Retry-After header, which is either
// integer seconds or an HTTP-date (IMF-fixdate, RFC 850, or asctime).
// Returns 0 if the header is missing; retryAfterDefault for a value that
// is unparseable, in the past, or out of range (max 300s). Callers apply
// their own default for the missing case.
func parseRetryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 && secs <= 300 {
		return time.Duration(secs) * time.Second
	}
	// HTTP-date form: "Retry-After: Wed, 21 Oct 2015 07:28:00 GMT".
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 && d <= 300*time.Second {
			return d
		}
	}
	return retryAfterDefault
}

func (d *Downloader) workerLoop(ctx context.Context, url string, ws *workerState, queue *Queue, f fileWriter, saveC chan<- struct{}) {
	for {
		if err := ctx.Err(); err != nil {
			ws.setErr(err)
			return
		}
		task, ok := queue.Pop()
		if !ok {
			return
		}
		err := d.runTask(ctx, url, ws, task, f)
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			// Steal/retry may leave a durable written prefix; commit it
			// before skipping/requeueing so resume does not re-fetch it.
			if ws.stealFlag.Swap(false) {
				d.recordWrittenPrefix(ws, task)
				continue
			}
			d.requeueUnfinished(ctx, ws, task, queue)
		case err != nil:
			if isTransient(err) {
				// The transient branch must honor the same stealFlag guard
				// the ctx-cancel branch uses, otherwise a cancelled read
				// that surfaces as io.ErrUnexpectedEOF or errBodyIdle
				// (rather than context.Canceled) after the monitor already
				// pushed leftover+cancel results in two pushers of
				// overlapping ranges onto the queue. stealFlag exists to
				// make this requeue skip (see monitor.go).
				if ws.stealFlag.Swap(false) {
					d.recordWrittenPrefix(ws, task)
					continue
				}
				d.vlog("task %d-%d transient error: %v", task.Start, task.End, err)
				d.requeueUnfinished(ctx, ws, task, queue)
			} else {
				ws.setErr(err)
				return
			}
		default:
			// Task fully written — record for resume before the worker
			// pops the next task and resets its live state.
			if d.manifest != nil {
				if err := d.verifyTaskRange(task, f); err != nil {
					ws.setErr(err)
					return
				}
			}
			d.recordCompleted(task)
			if saveC != nil {
				select {
				case saveC <- struct{}{}:
				default:
				}
			}
		}
	}
}

// recordWrittenPrefix commits the durable byte span already written for
// task into the resume accumulator. Called when steal or retry abandons
// the remainder of a task so a later resume does not re-fetch those bytes.
// No-op when resume is disabled or nothing has been written yet.
func (d *Downloader) recordWrittenPrefix(ws *workerState, task Task) {
	done := ws.bytesDone.Load()
	if done <= 0 {
		return
	}
	end := task.Start + done - 1
	if end > task.End {
		end = task.End
	}
	if end < task.Start {
		return
	}
	d.recordCompleted(Task{Start: task.Start, End: end})
}

func (d *Downloader) requeueUnfinished(ctx context.Context, ws *workerState, task Task, queue *Queue) {
	// Commit the written prefix first so crash/resume skips it even if
	// the remainder never makes it back onto the queue.
	d.recordWrittenPrefix(ws, task)
	remaining := Task{Start: task.Start + ws.bytesDone.Load(), End: task.End}
	if remaining.Start > remaining.End {
		return
	}
	// Keyed by the full Task value: large-file offsets (>4 GiB) never
	// collide the way a 32-bit packed key would, and Task is comparable.
	d.retryMu.Lock()
	if d.retryCount == nil {
		d.retryCount = make(map[Task]int)
	}
	n := d.retryCount[task]
	if d.autoConfig.RetryMax > 0 && n >= d.autoConfig.RetryMax {
		d.retryMu.Unlock()
		ws.setErr(fmt.Errorf("task %d-%d retried %d times", remaining.Start, remaining.End, n))
		return
	}
	d.retryCount[task] = n + 1
	d.retryMu.Unlock()
	wait := d.autoConfig.Backoff(n)
	d.vlog("task %d-%d retry %d (backoff %s)", remaining.Start, remaining.End, n+1, wait)

	if ctx.Err() != nil {
		queue.Push(remaining)
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		ws.setErr(ctx.Err())
	case <-timer.C:
		queue.Push(remaining)
	}
}

// runTask performs the HTTP range request for one task, writing bytes
// to f. ws may be nil for the single-stream fallback path.
func (d *Downloader) runTask(ctx context.Context, url string, ws *workerState, task Task, f fileWriter) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ws != nil {
		ws.reset(task)
		ws.cancelFn.Store(&cancel)
		defer ws.cancelFn.Store(nil)
	}

	d.vlog("task %d-%d started", task.Start, task.End)

	// Pre-compute Range header once per task (avoids per-attempt allocations).
	rangeHeader := "bytes=" + strconv.FormatInt(task.Start, 10) + "-" + strconv.FormatInt(task.End, 10)

	var lastErr error
	for attempt := 0; attempt <= d.autoConfig.RetryMax; attempt++ {
		req, err := d.newRequest(rctx, http.MethodGet, url, rangeHeader)
		if err != nil {
			return err
		}

		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}

		if isRetryableHTTP(resp.StatusCode) {
			wait := parseRetryAfter(resp.Header)
			if wait == 0 {
				wait = retryAfterDefault
			}
			drainAndClose(resp.Body)
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			d.vlog("HTTP %d on %d-%d, retrying after %s (attempt %d)",
				resp.StatusCode, task.Start, task.End, wait, attempt+1)
			if !sleepCtx(rctx, wait) {
				return rctx.Err()
			}
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			// Accept-Ranges was advertised (or probe thought so) but the
			// Range GET returned the full object. Do not write or complete
			// this partial task — downloadFromMirror falls back to single.
			drainAndClose(resp.Body)
			return errRangeNotSupported
		case http.StatusRequestedRangeNotSatisfiable:
			drainAndClose(resp.Body)
			// Never treat 416 as success: that would recordCompleted an
			// unwritten range and leave permanent holes on resume.
			return fmt.Errorf("range %d-%d: HTTP 416 range not satisfiable", task.Start, task.End)
		case http.StatusPartialContent:
			// fall through to read body
		default:
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: status %d", task.Start, task.End, resp.StatusCode)
		}

		// Rangeable downloads do not negotiate compression; if a server
		// still sends Content-Encoding we reject rather than corrupt the file.
		if enc := resp.Header.Get("Content-Encoding"); !isIdentity(enc) {
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: unexpected Content-Encoding %q", task.Start, task.End, enc)
		}
		// Require Content-Range to match the requested task so a CDN
		// cannot hand us a different slice written at task.Start.
		cr := resp.Header.Get("Content-Range")
		if cr == "" {
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: 206 missing Content-Range", task.Start, task.End)
		}
		start, end, _, ok := parseContentRange(cr)
		if !ok || start != task.Start || end != task.End {
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: Content-Range mismatch %q", task.Start, task.End, cr)
		}
		return d.readBody(rctx, task, f, ws, resp.Body)
	}
	if lastErr != nil {
		return fmt.Errorf("range %d-%d: exhausted %d retries on retryable HTTP status (%v)", task.Start, task.End, d.autoConfig.RetryMax, lastErr)
	}
	return fmt.Errorf("range %d-%d: exhausted retries on retryable HTTP status with no last error (invariant violation)", task.Start, task.End)
}

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
// Uses a stopped timer so the runtime does not retain the heap entry
// on the cancelled path.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
