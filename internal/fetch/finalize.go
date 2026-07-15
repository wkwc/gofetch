package fetch

import (
	"context"
	"fmt"
	"os"
	"time"
)

// finalize verifies the file's SHA256 hash if one was configured,
// prints a final status line, and clears any resume state.
func (d *Downloader) finalize(_ context.Context, f *os.File, prog *progress) error {
	if d.expectedHash != "" {
		d.printProgress(prog, true)
		fmt.Fprint(os.Stderr, "  verifying SHA256... ")
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := verifyFileHash(d.OutFile, d.expectedHash); err != nil {
			return err
		}
		fmt.Fprint(os.Stderr, "OK\n")
	}
	clearResume(d.resumePath)
	return nil
}

// maybeSaveResume writes the resume state if at least 1 second has
// passed since the last save. Cheap throttle to avoid I/O storms.
func (d *Downloader) maybeSaveResume(states []*workerState) {
	last := d.lastResumeSave.Load()
	if last != 0 && time.Since(time.Unix(0, last)) < time.Second {
		return
	}
	if d.saveResume(collectCompleted(states)) == nil {
		d.lastResumeSave.Store(time.Now().UnixNano())
	}
}

// collectCompleted gathers all finished ranges from worker states.
func collectCompleted(states []*workerState) []Task {
	var out []Task
	for _, ws := range states {
		t := ws.curTask.Load()
		if t == nil {
			continue
		}
		if ws.bytesDone.Load() >= t.Len() {
			out = append(out, *t)
		}
	}
	return out
}

// allocateSparse opens path read/write and truncates it to size,
// yielding a sparse file when supported by the filesystem.
func allocateSparse(path string, size int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncate %d: %w", size, err)
		}
	}
	return f, nil
}
