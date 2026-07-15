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
	f, err := allocateSparse(d.OutFile, total)
	if err != nil {
		return err
	}
	defer f.Close()
	if total <= 0 {
		return nil
	}

	prog := newProgress(total)
	for _, t := range completed {
		prog.add(t.Len())
	}

	const chunkSize = 1 << 20 // 1 MiB
	var seeds []Task
	for _, gap := range uncompleted(Task{Start: 0, End: total - 1}, completed) {
		seeds = append(seeds, seedTasks(gap.Start, gap.Len(), d.WorkersN, chunkSize)...)
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

	done := make(chan struct{})
	go func() {
		workers.Wait()
		stopMonitor()
		monitorWG.Wait()
		close(saveC)
		close(done)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			d.printProgress(prog, true)
			for _, ws := range states {
				if err, ok := ws.err(); ok && err != nil {
					return err
				}
			}
			_ = d.saveResume(collectCompleted(states))
			return d.finalize(ctx, f, prog)
		case <-saveC:
			d.maybeSaveResume(states)
		case <-resumeTicker.C:
			_ = d.saveResume(collectCompleted(states))
		case <-time.After(250 * time.Millisecond):
			d.printProgress(prog, false)
		}
	}
}
