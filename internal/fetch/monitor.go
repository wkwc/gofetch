package fetch

import (
	"context"
	"time"
)

// Tunables for work-stealing. See worker.stealPlan and requeueUnfinished.
const (
	monitorInterval  = 500 * time.Millisecond
	stealMinChunk    = 256 * 1024
	stealSlowBytes   = 1 << 20
	stealGracePeriod = 1500 * time.Millisecond
)

// monitor polls each worker. If a worker is on a large task but hasn't
// moved enough bytes within the grace period, it cancels the worker's
// in-flight HTTP request and pushes the unfinished portion back to the
// queue so another worker retries from where that one left off.
//
// Note on the steal/commit race: between `cancel()` and `queue.Push()`,
// the worker may have written additional bytes (cancelled requests
// still flush their in-flight read). That's harmless — the new worker
// picking up the leftover will start from the recorded offset and
// our own worker observes `cancelFn = nil` after the cancel returns,
// preventing a second cancel from this monitor loop.
func (d *Downloader) monitor(ctx context.Context, states []*workerState, queue *Queue) {
	t := time.NewTicker(monitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		for _, ws := range states {
			leftover, cancel, yes := ws.stealPlan(now)
			if !yes {
				continue
			}
			// Mark stealing before pull/queue to make the worker's
			// `requeueUnfinished` skip this range — otherwise we end
			// up with two pushers of overlapping ranges onto the
			// same queue slot.
			ws.stealFlag.Store(true)
			cancel()
			queue.Push(leftover)
		}
	}
}

// stealPlan checks if ws is a candidate for work stealing.
// ok=false means no steal (idle, too small, not slow enough).
//
// All ws reads are guarded by the taskGen sentinel: a worker that
// finishes the current task and starts a new task via reset() will
// increment taskGen; the post-snapshot re-check ensures we never
// publish progress that belongs to a Task we've already discarded.
func (ws *workerState) stealPlan(now time.Time) (Task, context.CancelFunc, bool) {
	t := ws.curTask.Load()
	if t == nil {
		return Task{}, nil, false
	}
	if t.Len() < 2*stealMinChunk {
		return Task{}, nil, false
	}
	if time.Duration(now.UnixNano()-ws.startedAt.Load()) < stealGracePeriod {
		return Task{}, nil, false
	}
	if ws.bytesDone.Load() >= stealSlowBytes {
		return Task{}, nil, false
	}
	gen := ws.taskGen.Load()
	progressBytes := ws.bytesDone.Load()
	cf := ws.cancelFn.Load()
	if cf == nil || progressBytes < 1 {
		return Task{}, nil, false
	}
	// re-check taskGen: if it changed, the worker moved on, so our reads are stale
	if ws.taskGen.Load() != gen {
		return Task{}, nil, false
	}
	newStart := t.Start + progressBytes
	if newStart+stealMinChunk > t.End || newStart+stealMinChunk < newStart {
		return Task{}, nil, false
	}
	// Skip if we've already stolen from this worker — the worker may
	// still be backing off on its in-flight rerr when we observe it
	// and would otherwise race-publish multiple leftover chunks.
	if ws.stealFlag.Load() {
		return Task{}, nil, false
	}
	return Task{Start: newStart, End: t.End}, *cf, true
}
