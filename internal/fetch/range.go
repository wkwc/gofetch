package fetch

import (
	"context"
	"sync"
	"time"
)

// seedResumeBytes seeds the progress tracker with bytes that were
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
func (d *Downloader) rangeDownload(ctx context.Context, url string, total int64, completed []Task, f fileWriter) error {
	if total <= 0 {
		// Ranges with unknown size cannot seed tasks safely — fall back
		// to single-stream rather than "succeeding" with an empty file.
		// No worker states exist here: there's no parallel work to monitor
		// and resume has no per-range byte counts to persist.
		return d.singleDownload(ctx, url, total, completed, f)
	}

	states := make([]*workerState, d.workerCount)
	for i := range states {
		states[i] = newWorkerState()
	}

	prog := newProgress(total, states)
	seedResumeBytes(prog, completed)

	// Adaptive chunk size: prefer 1 MiB, but grow so the seed task count
	// stays ≤ maxSeedTasks even for multi-TiB / hostile Content-Length.
	// splitRange also enforces the bound as defense in depth.
	chunkSize := seedChunkSize(total)
	// seedCap is only a make() hint; the actual count is produced by
	// splitRange over uncompleted() gaps. Cap the hint the same way so
	// a hostile total cannot panic makeslice.
	seedCap := total / chunkSize
	if total%chunkSize != 0 {
		seedCap++
	}
	if seedCap > maxSeedTasks {
		seedCap = maxSeedTasks
	} else if seedCap < 0 {
		seedCap = 0
	}
	seeds := make([]Task, 0, seedCap)
	for _, gap := range uncompleted(Task{Start: 0, End: total - 1}, completed) {
		seeds = append(seeds, splitRange(gap.Start, gap.Len(), chunkSize)...)
	}
	// Unbounded queue: steal/retry must never drop or livelock at a cap.
	queue := NewQueue(len(seeds), 0)
	queue.PushMany(seeds)

	var workers sync.WaitGroup
	workers.Add(d.workerCount)

	var saveC chan struct{}
	if d.resumePath != "" {
		saveC = make(chan struct{}, d.workerCount)
	}
	for _, ws := range states {
		go func(ws *workerState) {
			defer workers.Done()
			d.workerLoop(ctx, url, ws, queue, f, saveC)
		}(ws)
	}

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	var monitorWG sync.WaitGroup
	monitorWG.Add(1)
	go func() {
		defer monitorWG.Done()
		monitor(monitorCtx, states, queue)
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
				if err := d.runTask(ctx, url, nil, task, f); err != nil {
					for _, ws := range states {
						ws.setErr(err)
					}
					break
				}
				if d.manifest != nil {
					if err := d.verifyTaskRange(task, f); err != nil {
						for _, ws := range states {
							ws.setErr(err)
						}
						break
					}
				}
				d.recordCompleted(task)
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
				d.maybeSaveResume(true, url, states)
			}
			<-done
			return ctx.Err()
		case <-done:
			for _, ws := range states {
				if err, ok := ws.err(); ok && err != nil {
					return err
				}
			}
			return nil
		case <-saveC:
			d.maybeSaveResume(false, url, states)
		case <-resumeC:
			d.maybeSaveResume(false, url, states)
		case <-progressC:
			d.printProgress(prog, false)
		}
	}
}
