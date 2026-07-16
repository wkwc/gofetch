package fetch

import (
	"fmt"
	"os"
	"time"
)

// finalize prints the final progress, verifies hash if configured,
// and clears any resume state.
func (d *Downloader) finalize(f *os.File, prog *progress) error {
	d.printProgress(prog, true)
	if d.expectedHash != "" {
		fmt.Fprint(os.Stderr, "  verifying SHA256... ")
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := verifyFileHash(d.outFile, d.expectedHash); err != nil {
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

// collectCompleted gathers all fully-completed, non-stolen ranges.
func collectCompleted(states []*workerState) []Task {
	var out []Task
	for _, ws := range states {
		t := ws.curTask.Load()
		if t == nil {
			continue
		}
		done := ws.bytesDone.Load()
		// re-check that curTask hasn't been reassigned (worker moved on)
		if ws.curTask.Load() != t {
			continue
		}
		if done >= t.Len() && !ws.stealFlag.Load() {
			out = append(out, *t)
		}
	}
	return out
}

// allocateSparse opens path read/write and truncates it to size,
// yielding a sparse file when supported by the filesystem.
// If resume is enabled and the file already has the target size,
// it is opened without truncation to preserve existing bytes.
func allocateSparse(path string, size int64, resume bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE
	if !resume {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	if size > 0 {
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		if info.Size() < size {
			if err := f.Truncate(size); err != nil {
				f.Close()
				return nil, fmt.Errorf("truncate %d: %w", size, err)
			}
		}
	}
	return f, nil
}
