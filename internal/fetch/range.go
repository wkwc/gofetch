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
	f, err := allocateSparse(d.OutFile, total, d.ResumeEnabled)
	if err != nil {
		return err
	}
	defer f.Close()

	prog := newProgress(total)
	if total <= 0 {
		return d.finalize(f, prog)
	}
	for _, t := range completed {
		prog.add(t.Len())
	}

	const chunkSize = 1 << 20 // 1 MiB
	var seeds []Task
	for _, gap := range uncompleted(Task{Start: 0, End: total - 1}, completed) {
		seeds = append(seeds, splitRange(gap.Start, gap.Len(), chunkSize)...)
	}
	queue := &Queue{}
	queue.PushMany(seeds)

	states := make([]*workerState, d.WorkersN)
	for i := range states {
		states[i] = newWorkerState()
	}

	var workers sync.WaitGroup
	workers.Add(d.WorkersN)
	saveC := make(chan struct{}, d.WorkersN)
	for _, ws := range states {
		go func(ws *workerState) {
			defer workers.Done()
			d.workerLoop(ctx, ws, queue, prog, f, saveC)
		}(ws)
	}

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	var monitorWG sync.WaitGroup
	monitorWG.Add(1)
	go func() {
		defer monitorWG.Done()
		d.monitor(monitorCtx, states, queue)
	}()

	resumeTicker := time.NewTicker(5 * time.Second)
	defer resumeTicker.Stop()
	progressTicker := time.NewTicker(250 * time.Millisecond)
	defer progressTicker.Stop()

	done := make(chan struct{})
	go func() {
		workers.Wait()
		stopMonitor()
		monitorWG.Wait()
		// Drain any tasks pushed by the monitor after workers exited. Skip
		// if the parent context is already cancelled — the download is
		// failing, extra bytes won't help.
		if ctx.Err() == nil {
			for {
				task, ok := queue.Pop()
				if !ok {
					break
				}
				if err := d.runTask(ctx, nil, task, prog, f); err != nil {
					for _, ws := range states {
						ws.setErr(err)
					}
					break
				}
			}
		}
		close(saveC)
		close(done)
	}()

	for {
		select {
		case <-ctx.Done():
			// Save final resume state before exiting, so the next run can
			// pick up where we left off. Then wait for workers + drain
			// goroutine so we never close f underneath an in-flight WriteAt.
			d.maybeSaveResume(states)
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
		case <-resumeTicker.C:
			d.maybeSaveResume(states)
		case <-progressTicker.C:
			d.printProgress(prog, false)
		}
	}
}
