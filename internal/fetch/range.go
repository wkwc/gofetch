package fetch

import (
	"context"
	"sync"
	"time"
)

// resumeResumeSeeds seeds the progress tracker with bytes that were
// already on disk when we resumed. preDone survives worker.reset()
// whereas per-worker bytesDone does not, so this avoids the
// startup-progress-shrink race.
func seedResumeBytes(prog *progress, completed []Task) {
	if prog == nil {
		return
	}
	var n int64
	for _, t := range completed {
		n += t.Len()
	}
	if n > 0 {
		prog.add(n)
	}
}

// rangeDownload fans the file out across N workers with a stealing monitor.
// It seeds the work queue from ~1 MiB chunks of the uncompleted gaps,
// runs workers until the queue drains, and signals completion.
// The file writer f is managed by the caller (already open, will be closed by caller).
func (d *Downloader) rangeDownload(ctx context.Context, total int64, completed []Task, f fileWriter) error {
	states := make([]*workerState, d.workersN)
	for i := range states {
		states[i] = newWorkerState()
	}
	prog := newProgress(total, states)
	seedResumeBytes(prog, completed)

	if total <= 0 {
		return d.finalize(f, prog)
	}

	// Try to load manifest
	manifestPath := d.outFile + ".gofetch.manifest"
	if m, err := LoadManifest(manifestPath); err == nil {
		d.manifest = m
		d.vlog("loaded manifest with %d chunks", len(m.Chunks))
	}

	const chunkSize = 1 << 20 // 1 MiB
	seeds := make([]Task, 0, (total+chunkSize-1)/chunkSize)
	for _, gap := range uncompleted(Task{Start: 0, End: total - 1}, completed) {
		seeds = append(seeds, splitRange(gap.Start, gap.Len(), chunkSize)...)
	}
	queue := NewQueue(len(seeds))
	queue.PushMany(seeds)

	var workers sync.WaitGroup
	workers.Add(d.workersN)

	var saveC chan struct{}
	if d.resumePath != "" {
		saveC = make(chan struct{}, d.workersN)
	}
	for _, ws := range states {
		go func(ws *workerState) {
			defer workers.Done()
			d.workerLoop(ctx, ws, queue, f, saveC)
		}(ws)
	}

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	var monitorWG sync.WaitGroup
	monitorWG.Add(1)
	go func() {
		defer monitorWG.Done()
		d.monitor(monitorCtx, states, queue)
	}()

	var progressC <-chan time.Time
	if !d.quiet {
		progressTicker := time.NewTicker(250 * time.Millisecond)
		defer progressTicker.Stop()
		progressC = progressTicker.C
	}

	var resumeC <-chan time.Time
	if d.resumePath != "" {
		resumeTicker := time.NewTicker(5 * time.Second)
		defer resumeTicker.Stop()
		resumeC = resumeTicker.C
	}

	done := make(chan struct{})
	go func() {
		workers.Wait()
		stopMonitor()
		monitorWG.Wait()
		if ctx.Err() == nil {
			for {
				task, ok := queue.Pop()
				if !ok {
					break
				}
				if err := d.runTask(ctx, nil, task, f); err != nil {
					for _, ws := range states {
						ws.setErr(err)
					}
					break
				}
			}
		}
		if saveC != nil {
			close(saveC)
		}
		close(done)
	}()

	if d.manifest != nil {
		d.vlog("integrity checking enabled: %s", d.outFile+".gofetch.manifest")
	}

	for {
		select {
		case <-ctx.Done():
			if d.resumePath != "" {
				d.maybeSaveResume(states)
			}
			<-done
			return ctx.Err()
		case <-done:
			for _, ws := range states {
				if err, ok := ws.err(); ok && err != nil {
					return err
				}
			}
			return d.finalize(f, prog)
		case <-saveC:
			d.maybeSaveResume(states)
		case <-resumeC:
			d.maybeSaveResume(states)
		case <-progressC:
			d.printProgress(prog, false)
		}
	}
}
