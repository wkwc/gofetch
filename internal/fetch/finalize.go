package fetch

import (
	"fmt"
	"os"
	"time"
)

// finalize prints the download summary, verifies hashes, and clears resume state.
// f may be nil if already closed; prog is built here when nil so speed/bytes
// reflect a fresh progress snapshot rather than dropping to (0,total).
func (d *Downloader) finalize(f fileWriter, prog *progress) error {
	if prog == nil {
		states := make([]*workerState, max(d.workersN, 1))
		for i := range states {
			states[i] = newWorkerState()
		}
		prog = newProgress(d.totalSize, states)
		// Reflect we have everything (downloader only calls finalize on
		// success — the EOF path) so the snapshot reads the real total.
		if d.totalSize > 0 {
			prog.add(d.totalSize)
		}
	}
	d.printProgress(prog, true)

	if d.quiet && d.expectedHash == "" {
		clearResume(d.resumePath)
		return nil
	}

	if f != nil {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	if d.quiet {
		clearResume(d.resumePath)
		return nil
	}

	elapsed := time.Since(d.startTime)
	done, _ := prog.snapshot()

	speed := float64(0)
	if elapsed > 0 {
		speed = float64(done) / elapsed.Seconds()
	}

	fmt.Fprintln(os.Stderr, "\n  download complete")
	fmt.Fprintf(os.Stderr, "  bytes:   %s\n", humanBytes(done))
	fmt.Fprintf(os.Stderr, "  time:    %s\n", formatDuration(elapsed))
	fmt.Fprintf(os.Stderr, "  speed:   %s/s\n", humanBytes(int64(speed)))
	fmt.Fprintf(os.Stderr, "  workers: %d\n", d.workersN)

	if d.manifest != nil {
		fmt.Fprint(os.Stderr, "  manifest: verifying... ")
		if err := d.manifest.VerifyFull(d.outFile); err != nil {
			fmt.Fprint(os.Stderr, "FAILED\n")
			return err
		}
		fmt.Fprint(os.Stderr, "OK\n")
	}

	if d.expectedHash != "" {
		fmt.Fprintf(os.Stderr, "  hash %s: verifying... ", d.hashAlgo)
		if err := verifyFileHash(d.outFile, d.hashAlgo, d.expectedHash); err != nil {
			fmt.Fprint(os.Stderr, "FAILED\n")
			return err
		}
		fmt.Fprint(os.Stderr, "OK\n")
	}

	clearResume(d.resumePath)
	return nil
}

// formatDuration formats a duration in human-readable form.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) - m*60
	return fmt.Sprintf("%dm%ds", m, s)
}

// maybeSaveResume writes the resume state if at least 1 second has
// passed since the last save. Cheap throttle to avoid I/O storms.
// No-op when resume is disabled.
func (d *Downloader) maybeSaveResume(states []*workerState) {
	if d.resumePath == "" {
		return
	}
	last := d.lastResumeSave.Load()
	if last != 0 && time.Duration(time.Now().UnixNano()-last) < time.Second {
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
		if ws.curTask.Load() != t {
			continue
		}
		if done >= t.Len() && !ws.stealFlag.Load() {
			out = append(out, *t)
		}
	}
	return out
}

// allocateSparse opens path write-only and truncates it to size,
// yielding a sparse file when supported by the filesystem.
// If resume is enabled and the file already has the target size,
// it is opened without truncation to preserve existing bytes.
func allocateSparse(path string, size int64, resume bool) (*os.File, error) {
	flags := os.O_WRONLY | os.O_CREATE
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
		if info.Size() != size {
			if err := f.Truncate(size); err != nil {
				f.Close()
				return nil, fmt.Errorf("truncate %d: %w", size, err)
			}
		}
	}
	return f, nil
}
