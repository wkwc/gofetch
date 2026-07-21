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

const maxHTTPStatusRetries = 10

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
	return 5 * time.Second
}

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

func (d *Downloader) requeueUnfinished(ctx context.Context, ws *workerState, task Task, queue *Queue) {
	remaining := Task{Start: task.Start + ws.bytesDone.Load(), End: task.End}
	if remaining.Start > remaining.End {
		return
	}
	// Key by the full [Start,End] pair so large-file offsets (>4 GiB)
	// never collide the way a 32-bit packed int64 key would.
	key := [2]int64{task.Start, task.End}
	d.retryMu.Lock()
	if d.retryCount == nil {
		d.retryCount = make(map[[2]int64]int)
	}
	n := d.retryCount[key]
	if d.autoConfig.RetryMax > 0 && n >= d.autoConfig.RetryMax {
		d.retryMu.Unlock()
		ws.setErr(fmt.Errorf("task %d-%d retried %d times", remaining.Start, remaining.End, n))
		return
	}
	d.retryCount[key] = n + 1
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
func (d *Downloader) runTask(ctx context.Context, ws *workerState, task Task, f fileWriter) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ws != nil {
		ws.reset(task)
		cf := context.CancelFunc(cancel)
		ws.cancelFn.Store(&cf)
		defer ws.cancelFn.Store(nil)
	}

	d.vlog("task %d-%d started", task.Start, task.End)

	// Pre-compute Range header once per task (avoids per-attempt allocations).
	rangeHeader := "bytes=" + strconv.FormatInt(task.Start, 10) + "-" + strconv.FormatInt(task.End, 10)

	var lastErr error
	for attempt := 0; attempt <= maxHTTPStatusRetries; attempt++ {
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, d.url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("User-Agent", userAgent)
		// Explicit identity + DisableCompression on the transport:
		// never let a proxy inject gzip that would misalign Range offsets.
		req.Header.Set("Accept-Encoding", "identity")

		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}

		if isRetryableHTTP(resp.StatusCode) {
			wait := parseRetryAfter(resp.Header)
			if wait == 0 {
				wait = 5 * time.Second
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
			drainAndClose(resp.Body)
			return fmt.Errorf("range %d-%d: server returned 200 OK (does not support range requests)", task.Start, task.End)
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
		return fmt.Errorf("range %d-%d: exhausted %d retries on retryable HTTP status (%v)", task.Start, task.End, maxHTTPStatusRetries, lastErr)
	}
	return fmt.Errorf("range %d-%d: exhausted %d retries on retryable HTTP status", task.Start, task.End, maxHTTPStatusRetries)
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

func (d *Downloader) readBody(ctx context.Context, task Task, f fileWriter, ws *workerState, body io.ReadCloser) error {
	// Idle-timeout wrapper so a stalled body after headers is retryable
	// instead of hanging forever (steal only fires after bytesDone ≥ 1).
	idle := newIdleBody(ctx, body, defaultBodyIdle)
	defer drainAndClose(idle)

	// Zero-copy fast path when the writer exposes the underlying mmap slice.
	if wb, ok := f.(mmapWriterBytes); ok {
		if data := wb.Bytes(); data != nil {
			return d.readBodyDirect(idle, task, wb, ws, data)
		}
	}

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	cursor, end := task.Start, task.End
	manifest := d.manifest
	for {
		n, rerr := idle.Read(buf)
		if n > 0 {
			if remaining := end - cursor + 1; int64(n) > remaining {
				n = int(remaining)
			}
			if _, err := f.WriteAt(buf[:n], cursor); err != nil {
				return err
			}
			cursor += int64(n)
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
				// Inclusive End: full range requires cursor == end+1.
				if cursor <= end {
					return fmt.Errorf("server closed connection mid-range: got %d of expected %d bytes",
						cursor-task.Start, task.Len())
				}
				return nil
			}
			return rerr
		}
		if cursor > end {
			return nil
		}
	}
}

func (d *Downloader) readBodyDirect(body io.Reader, task Task, wb mmapWriterBytes, ws *workerState, data []byte) error {
	_ = wb
	taskEnd := task.End
	if taskEnd+1 > int64(len(data)) {
		return fmt.Errorf("mmap slice short: end=%d len=%d", taskEnd, len(data))
	}
	taskStart := task.Start
	taskSize := taskEnd - taskStart + 1
	bufCap := int64(d.bufSize)
	manifest := d.manifest
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
		n, rerr := body.Read(buf)
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
				if cursor < taskSize {
					return fmt.Errorf("server closed connection mid-range: got %d of expected %d bytes", cursor, taskSize)
				}
				return nil
			}
			return rerr
		}
	}
}

// verifyTaskRange checks any fully-contained manifest chunks for the
// completed task. Mmap path is zero-copy; raw path reads via WriteAt-
// compatible SectionReader-style ReadAt when available.
func (d *Downloader) verifyTaskRange(task Task, f fileWriter) error {
	if d.manifest == nil {
		return nil
	}
	size := task.Len()
	if size <= 0 {
		return nil
	}
	if wb, ok := f.(mmapWriterBytes); ok {
		if data := wb.Bytes(); data != nil {
			if task.End+1 > int64(len(data)) {
				return fmt.Errorf("manifest: task %d-%d past mmap len %d", task.Start, task.End, len(data))
			}
			return d.manifest.VerifyRange(task.Start, task.End, data[task.Start:task.End+1])
		}
	}
	// Non-mmap: re-read the span for VerifyRange when the writer is a
	// raw file (os.File.ReadAt).
	type readerAt interface {
		ReadAt([]byte, int64) (int, error)
	}
	ra, ok := f.(readerAt)
	if !ok {
		return nil // defer to VerifyFull
	}
	buf := make([]byte, size)
	if _, err := ra.ReadAt(buf, task.Start); err != nil {
		return fmt.Errorf("manifest: re-read task %d-%d: %w", task.Start, task.End, err)
	}
	return d.manifest.VerifyRange(task.Start, task.End, buf)
}

// bufSizeSmall and bufSizeLarge bound the pooled buffer size so the pool
// always returns reusable buffers and oversized requests allocate fresh.
const (
	bufSizeSmall = 64 * 1024
	bufSizeLarge = 256 * 1024
)

// bufPool recycles per-worker read buffers at bufSizeLarge.
var bufPool = sync.Pool{
	New: func() any { b := make([]byte, bufSizeLarge); return &b },
}

// acquireBuf returns a buffer of length n. Buffers up to bufSizeLarge are
// served from the pool; larger requests allocate a fresh (non-pooled) slice
// the caller releases by simply letting it be GC'd.
func acquireBuf(n int) []byte {
	if n <= bufSizeLarge {
		bp := bufPool.Get().(*[]byte)
		if cap(*bp) < n {
			// Undersized pool entry (should not happen) — allocate fresh.
			return make([]byte, n)
		}
		return (*bp)[:n]
	}
	return make([]byte, n)
}

// releaseBuf returns a buffer to the pool only when capacity is exactly
// bufSizeLarge so the next acquireBuf(bufSizeLarge) never panics on a
// short slice.
func releaseBuf(b []byte) {
	if cap(b) != bufSizeLarge {
		return
	}
	bp := b[:bufSizeLarge:bufSizeLarge]
	bufPool.Put(&bp)
}
