package fetch

import (
	"context"
	"sync"
	"time"
)

// rangeDownload fans the file out across N workers with a stealing monitor.
// It seeds the work queue from ~1 MiB chunks of the uncompleted gaps,
// runs workers until the queue drains, and signals completion.
func (d *Downloader) rangeDownload(ctx context.Context, total int64, completed []Task) error {
	f, err := allocateSparse(d.outFile, total, d.resumeEnabled)
	if err != nil {
		return err
	}
	defer f.Close()

	states := make([]*workerState, d.workersN)
	for i := range states {
		states[i] = newWorkerState()
	}
	prog := newProgress(total, states)

	if total <= 0 {
		return d.finalize(f, prog)
	}
	// Seed completed bytes into the workers' counters so the initial
	// progress is accurate without an extra CAS on the global counter.
	seedCompleted(states, completed)

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
	// Only allocate the save channel when resume is enabled.
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

	// Always update progress bar so user sees activity, even with -q.
	var progressC <-chan time.Time
	if !d.quiet {
		progressTicker := time.NewTicker(250 * time.Millisecond)
		defer progressTicker.Stop()
		progressC = progressTicker.C
	}

	// Only save resume state periodically when resume is enabled.
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

	// Print initial manifest path if verbose
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

// seedCompleted primes a few worker states with synthetic byte counts from
// resumed Tasks so that progress.snapshot() shows the initial bytesDone
// without needing a CAS loop in the hot path.
func seedCompleted(states []*workerState, completed []Task) {
	if len(completed) == 0 {
		return
	}
	var totalBytes int64
	for _, t := range completed {
		totalBytes += t.Len()
	}
	if totalBytes == 0 {
		return
	}
	// Spread completed bytes roughly evenly across workers. The last
	// worker absorbs any remainder so snapshot() reproduces totalBytes.
	n := int64(len(states))
	for i, ws := range states {
		v := totalBytes / n
		if i == len(states)-1 {
			v = totalBytes - v*(n-1)
		}
		if v > 0 {
			ws.bytesDone.Store(v)
		}
	}
}
