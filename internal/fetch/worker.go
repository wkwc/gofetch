package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

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
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	return false
}

// workerLoop pops tasks from queue, runs them, signals saveC on success.
// On context cancellation it re-pushes the unfinished portion of the task
// (skipping when the cancel came from monitor's steal plan). Transient
// network errors are retried with exponential backoff.
func (d *Downloader) workerLoop(ctx context.Context, ws *workerState, queue *Queue, prog *progress, f *os.File, saveC chan<- struct{}) {
	for {
		if err := ctx.Err(); err != nil {
			ws.setErr(err)
			return
		}
		task, ok := queue.Pop()
		if !ok {
			return
		}
		err := d.runTask(ctx, ws, task, prog, f)
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			if ws.stealFlag.Swap(false) {
				continue
			}
			d.requeueUnfinished(ctx, ws, task, queue)
		case err != nil:
			if isTransient(err) {
				d.requeueUnfinished(ctx, ws, task, queue)
			} else {
				ws.setErr(err)
				return
			}
		default:
			select {
			case saveC <- struct{}{}:
			default:
			}
		}
	}
}

// requeueUnfinished pushes the unfinished remainder of an aborted task
// back to the queue with retry-cap + exponential backoff.
func (d *Downloader) requeueUnfinished(ctx context.Context, ws *workerState, task Task, queue *Queue) {
	remaining := Task{Start: task.Start + ws.bytesDone.Load(), End: task.End}
	if remaining.Start >= remaining.End {
		return
	}
	d.retryMu.Lock()
	if d.retryCount == nil {
		d.retryCount = make(map[Task]int)
	}
	n := d.retryCount[remaining]
	if n >= maxTaskRetries {
		d.retryMu.Unlock()
		ws.setErr(fmt.Errorf("task %d-%d retried %d times", remaining.Start, remaining.End, n))
		return
	}
	d.retryCount[remaining] = n + 1
	d.retryMu.Unlock()
	backoff := time.Duration(50+rand.N(20)) * time.Millisecond * time.Duration(n+1)
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		ws.setErr(ctx.Err())
	case <-timer.C:
		queue.Push(remaining)
	}
}

// runTask performs the HTTP range request for one task, writing bytes
// to f. ws may be nil for the single-stream fallback.
func (d *Downloader) runTask(ctx context.Context, ws *workerState, task Task, prog *progress, f *os.File) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ws != nil {
		cf := context.CancelFunc(cancel)
		ws.cancelFn.Store(&cf)
		defer ws.cancelFn.Store(nil)
		ws.reset(task)
	}

	req, err := http.NewRequestWithContext(rctx, http.MethodGet, d.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", "bytes="+strconv.FormatInt(task.Start, 10)+"-"+strconv.FormatInt(task.End, 10))
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		drainAndClose(resp.Body)
		return fmt.Errorf("range %d-%d: server returned 200 OK (does not support range requests)", task.Start, task.End)
	}
	if resp.StatusCode != http.StatusPartialContent {
		drainAndClose(resp.Body)
		return fmt.Errorf("range %d-%d: status %d", task.Start, task.End, resp.StatusCode)
	}
	defer drainAndClose(resp.Body)

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	cursor, end := task.Start, task.End
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if remaining := end - cursor + 1; int64(n) > remaining {
				n = int(remaining)
				if n <= 0 {
					return nil
				}
			}
			if _, err := f.WriteAt(buf[:n], cursor); err != nil {
				return err
			}
			cursor += int64(n)
			prog.add(int64(n))
			if ws != nil {
				ws.bytesDone.Store(cursor - task.Start)
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
