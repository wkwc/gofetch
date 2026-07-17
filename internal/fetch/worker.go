package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// maxHTTPStatusRetries caps the number of times a single runTask call retries
// the same Range request in response to 429/503/etc. Prevents unbounded retries
// when a server is misbehaving.
const maxHTTPStatusRetries = 10

// workerState is the live state of one worker goroutine.
// All fields are atomic for lock-free reads from the monitor.
type workerState struct {
	curTask   atomic.Pointer[Task]
	bytesDone atomic.Int64
	startedAt atomic.Int64  // unix nano
	taskGen   atomic.Uint64 // incremented each reset; monitor uses to detect stale reads
	cancelFn  atomic.Pointer[context.CancelFunc]
	stealFlag atomic.Bool // set true when monitor preempts; tells workerLoop not to re-push
	errVal    atomic.Pointer[error]
}

func newWorkerState() *workerState { return &workerState{} }

// setErr records the first non-nil error.
func (ws *workerState) setErr(err error) {
	if err == nil {
		return
	}
	e := err
	ws.errVal.CompareAndSwap(nil, &e)
}

// err returns the recorded error (if any).
func (ws *workerState) err() (error, bool) {
	p := ws.errVal.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}

// reset initializes state for a fresh task run.
func (ws *workerState) reset(task Task) {
	taskRef := task
	ws.curTask.Store(&taskRef)
	ws.bytesDone.Store(0)
	ws.stealFlag.Store(false)
	ws.startedAt.Store(time.Now().UnixNano())
	ws.taskGen.Add(1)
}

// isTransient returns true for network-level errors worth retrying.
func isTransient(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
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
		errors.Is(err, syscall.ETIMEDOUT):
		return true
	}
	return false
}

// isRetryableHTTP returns true for HTTP status codes that warrant automatic retry.
func isRetryableHTTP(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusBadGateway ||
		code == http.StatusGatewayTimeout ||
		code == http.StatusRequestTimeout
}

// parseRetryAfter parses the RFC 7231 Retry-After header.
// Returns 0 if missing or unparseable as a seconds integer.
func parseRetryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 && secs <= 300 {
		return time.Duration(secs) * time.Second
	}
	// Non-integer value (e.g., HTTP-date): fall back to 5s.
	return 5 * time.Second
}

// chunkKey used to be a helper; inlined for hot-path inlining and
// less abstraction. Two Tasks belong to the same logical chunk iff
// their End offsets match the original seeded End. This survives
// partial downloads and steals (which preserve End) so that retry
// accounting accumulates correctly.

// workerLoop pops tasks from queue, runs them, signals saveC on success.
// On context cancellation it re-pushes the unfinished portion of the task
// (skipping when the cancel came from monitor's steal plan). Transient
// network errors and HTTP 429/503 are retried with exponential backoff.
func (d *Downloader) workerLoop(ctx context.Context, ws *workerState, queue *Queue, f fileWriter, saveC chan<- struct{}) {
	for {
		if err := ctx.Err(); err != nil {
			ws.setErr(err)
			return
		}
		task, ok := queue.Pop()
		if !ok {
			return
		}
		err := d.runTask(ctx, ws, task, f)
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			if ws.stealFlag.Swap(false) {
				continue
			}
			d.requeueUnfinished(ctx, ws, task, queue)
		case err != nil:
			if isTransient(err) {
				d.vlog("task %d-%d transient error: %v", task.Start, task.End, err)
				d.requeueUnfinished(ctx, ws, task, queue)
			} else {
				ws.setErr(err)
				return
			}
		default:
			if saveC != nil {
				select {
				case saveC <- struct{}{}:
				default:
				}
			}
		}
	}
}

// requeueUnfinished pushes the unfinished remainder of an aborted task
// back to the queue with bounded retries and exponential backoff.
// The retry budget is keyed on the original chunk's End so the count
// accumulates across partial failures of the same logical chunk.
func (d *Downloader) requeueUnfinished(ctx context.Context, ws *workerState, task Task, queue *Queue) {
	remaining := Task{Start: task.Start + ws.bytesDone.Load(), End: task.End}
	if remaining.Start >= remaining.End {
		return
	}
	key := task.End // stable identity = original chunk's End (was chunkKey)
	d.retryMu.Lock()
	if d.retryCount == nil {
		d.retryCount = make(map[int64]int)
	}
	n := d.retryCount[key]
	if d.autoConfig.RetryMax > 0 && n >= d.autoConfig.RetryMax {
		d.retryMu.Unlock()
		ws.setErr(fmt.Errorf("task %d-%d retried %d times", remaining.Start, remaining.End, n))
		return
	}
	d.retryCount[key] = n + 1
	d.retryMu.Unlock()
	backoff := d.autoConfig.Backoff(n)
	d.vlog("task %d-%d retry %d (backoff %s)", remaining.Start, remaining.End, n+1, backoff)

	// If we're already shutting down, push immediately (no point waiting).
	if ctx.Err() != nil {
		queue.Push(remaining)
		return
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		// Don't push back into a closing pipeline; just record the ctx error.
		ws.setErr(ctx.Err())
	case <-timer.C:
		queue.Push(remaining)
	}
}

// runTask performs the HTTP range request for one task, writing bytes
// to f. ws may be nil for the single-stream fallback.
func (d *Downloader) runTask(ctx context.Context, ws *workerState, task Task, f fileWriter) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ws != nil {
		cf := context.CancelFunc(cancel)
		ws.cancelFn.Store(&cf)
		defer ws.cancelFn.Store(nil)
		ws.reset(task)
	}

	d.vlog("task %d-%d started", task.Start, task.End)

	// Pre-compute Range header once per task (avoid 2 strconv.FormatInt
	// allocations per HTTP attempt).
	rangeHeader := "bytes=" + strconv.FormatInt(task.Start, 10) + "-" + strconv.FormatInt(task.End, 10)

	// Pre-compute User-Agent and Accept-Encoding if needed.
	acceptEncoding := ""
	if d.autoConfig.Compress {
		acceptEncoding = "gzip"
	}

	var lastErr error
	// Bound the inner retry loop so a persistently-broken server cannot
	// hold a worker forever.
	for attempt := 0; attempt <= maxHTTPStatusRetries; attempt++ {
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, d.url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("User-Agent", userAgent)
		if acceptEncoding != "" {
			req.Header.Set("Accept-Encoding", acceptEncoding)
		}

		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}

		// Server-retryable status (429, 503, etc.).
		if isRetryableHTTP(resp.StatusCode) {
			wait := parseRetryAfter(resp.Header)
			if wait == 0 {
				wait = 5 * time.Second
			}
			drainAndClose(resp.Body)
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			d.vlog("HTTP %d on %d-%d, retrying after %s (attempt %d)",
				resp.StatusCode, task.Start, task.End, wait, attempt+1)
			select {
			case <-rctx.Done():
				return rctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		// 200 OK means the server does not support Range requests at all.
		if resp.StatusCode == http.StatusOK {
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: server returned 200 OK (does not support range requests)", task.Start, task.End)
		}

		// 416 Range Not Satisfiable — treat as already complete.
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			drainAndClose(resp.Body)
			d.vlog("HTTP 416 on %d-%d: range unsatisfiable, skipping", task.Start, task.End)
			return nil
		}

		if resp.StatusCode != http.StatusPartialContent {
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: status %d", task.Start, task.End, resp.StatusCode)
		}

		// On success path: read body. drainAndClose must be called here,
		// not via defer (defer would accumulate across retry iterations).
		return d.readBody(rctx, resp, task, f, ws)
	}
	if lastErr != nil {
		return fmt.Errorf("range %d-%d: exhausted %d retries on retryable HTTP status (%v)", task.Start, task.End, maxHTTPStatusRetries, lastErr)
	}
	return fmt.Errorf("range %d-%d: exhausted %d retries on retryable HTTP status", task.Start, task.End, maxHTTPStatusRetries)
}

// readBody reads the response body into f with chunked writes and
// optional per-chunk verification. Called once per successful request.
func (d *Downloader) readBody(ctx context.Context, resp *http.Response, task Task, f fileWriter, ws *workerState) error {
	defer drainAndClose(resp.Body)

	// If the writer exposes the underlying bytes (mmap) and the slice
	// is non-empty, we go zero-copy. Slice being nil indicates a
	// fallback (raw pwrite) — we use the buffered path there.
	if wb, ok := f.(mmapWriterBytes); ok {
		if data := wb.Bytes(); data != nil {
			return d.readBodyDirect(resp, task, wb, ws, data)
		}
	}

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	cursor, end := task.Start, task.End
	manifest := d.manifest // hoist out of per-Read hot loop
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			// Clamp n to remaining bytes; last partial read from server.
			if remaining := end - cursor + 1; int64(n) > remaining {
				n = int(remaining)
			}
			if _, err := f.WriteAt(buf[:n], cursor); err != nil {
				return err
			}
			cursor += int64(n)
			// Progress is derived from ws.bytesDone via progress.snapshot().
			if ws != nil {
				ws.bytesDone.Store(cursor - task.Start)
			}
			if manifest != nil {
				if err := manifest.VerifyChunk(cursor-int64(n), cursor-1, buf[:n]); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
		if cursor > end {
			return nil
		}
	}
}

// readBodyDirect is the zero-copy path: it reads HTTP response bytes
// directly into the mmap'd output slice at task.Start, skipping the
// intermediate buffer+memcpy.
func (d *Downloader) readBodyDirect(resp *http.Response, task Task, wb mmapWriterBytes, ws *workerState, data []byte) error {
	taskEnd := task.End
	if taskEnd+1 > int64(len(data)) {
		return fmt.Errorf("mmap slice short: end=%d len=%d", taskEnd, len(data))
	}
	taskStart := task.Start
	taskSize := taskEnd - taskStart + 1
	bufCap := int64(d.bufSize)
	manifest := d.manifest // hoist out
	cursor := int64(0)
	for {
		remaining := taskSize - cursor
		if remaining <= 0 {
			return nil
		}
		want := bufCap
		if want > remaining {
			want = remaining
		}
		buf := data[taskStart+cursor : taskStart+cursor+want]
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			cursor += int64(n)
			if ws != nil {
				ws.bytesDone.Store(cursor)
			}
			if manifest != nil {
				if err := manifest.VerifyChunk(
					taskStart+cursor-int64(n), taskStart+cursor-1, buf[:n]); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
	}
}

// bufPool recycles per-worker read buffers. Allocates at the largest
// expected size (256 KiB for files >100 MiB); smaller sizes reuse the
// same pool by slicing.
var bufPool = sync.Pool{
	New: func() any { b := make([]byte, 256*1024); return &b },
}

// acquireBuf returns a buffer of at least length n, drawing from the pool.
func acquireBuf(n int) []byte {
	bp := bufPool.Get().(*[]byte)
	if cap(*bp) >= n {
		return (*bp)[:n]
	}
	// Pool buffer too small; allocate fresh.
	return make([]byte, n)
}

// releaseBuf returns b to the pool, dropping oversized buffers.
func releaseBuf(b []byte) {
	if cap(b) > 1<<20 || cap(b) == 0 {
		return
	}
	bp := b[:cap(b):cap(b)]
	bufPool.Put(&bp)
}
