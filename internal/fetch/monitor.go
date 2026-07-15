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
	maxTaskRetries   = 5
)

// monitor polls each worker. If a worker is on a large task but hasn't
// moved enough bytes within the grace period, it cancels the worker's
// in-flight HTTP request and pushes the unfinished portion back to the
// queue so another worker retries from where that one left off.
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
			ws.stealFlag.Store(true)
			cancel()
			queue.Push(leftover)
		}
	}
}

// stealPlan computes what to do if ws is slow. ok=false means it's
// not a candidate (no task, not slow, or too small to split).
func (ws *workerState) stealPlan(now time.Time) (Task, context.CancelFunc, bool) {
	t := ws.curTask.Load()
	if t == nil {
		return Task{}, nil, false
	}
	if t.Len() < 2*stealMinChunk {
		return Task{}, nil, false
	}
	if now.Sub(time.Unix(0, ws.startedAt.Load())) < stealGracePeriod {
		return Task{}, nil, false
	}
	if ws.bytesDone.Load() >= stealSlowBytes {
		return Task{}, nil, false
	}
	progressBytes := ws.bytesDone.Load()
	cf := ws.cancelFn.Load()
	if cf == nil || progressBytes < 1 {
		return Task{}, nil, false
	}
	newStart := t.Start + progressBytes
	if newStart+stealMinChunk > t.End {
		return Task{}, nil, false
	}
	return Task{Start: newStart, End: t.End}, *cf, true
}
