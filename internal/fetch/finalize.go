package fetch

import (
	"fmt"
	"os"
	"time"
)

// finalize prints the download summary, verifies hashes, and clears resume state.
// f may be nil if already closed; prog is built here when nil so speed/bytes
// reflect a fresh progress snapshot rather than dropping to (0,total).
//
// Ownership: only Download (or a leaf that is the sole exit path) should call
// finalize. Always Sync+Close before integrity checks so verification reads
// durable bytes. Quiet mode suppresses UI only — never skips Sync/Close/hash.
func (d *Downloader) finalize(f fileWriter, prog *progress) (err error) {
	// Always clear the resume sidecar on exit, success OR failure —
	// leaving stale state on disk lets the next run silently skip
	// ranges, so we err on the side of forcing a full re-download.
	if d.resumePath != "" {
		defer func() { _ = clearResume(d.resumePath) }()
	}
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
	if !d.quiet {
		d.printProgress(prog, true)
	}

	if f != nil {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	// Integrity checks always run (quiet only suppresses messages).
	if d.manifest != nil {
		if !d.quiet {
			fmt.Fprint(os.Stderr, "  manifest: verifying... ")
		}
		if err := d.manifest.VerifyFull(d.outFile); err != nil {
			if !d.quiet {
				fmt.Fprint(os.Stderr, "FAILED\n")
			}
			return err
		}
		if !d.quiet {
			fmt.Fprint(os.Stderr, "OK\n")
		}
	}

	if d.expectedHash != "" {
		if !d.quiet {
			fmt.Fprintf(os.Stderr, "  hash %s: verifying... ", d.hashAlgo)
		}
		if err := verifyFileHash(d.outFile, d.hashAlgo, d.expectedHash); err != nil {
			if !d.quiet {
				fmt.Fprint(os.Stderr, "FAILED\n")
			}
			return err
		}
		if !d.quiet {
			fmt.Fprint(os.Stderr, "OK\n")
		}
	}

	if d.quiet {
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
//
// force=true bypasses the throttle: the caller (cancel path, run-end)
// must always emit a final sidecar otherwise the last recordCompleted
// of the run is silently dropped if it landed within the throttle
// window — next run would re-fetch the bytes the loop already wrote.
func (d *Downloader) maybeSaveResume(force bool) {
	if d.resumePath == "" {
		return
	}
	if !force {
		last := d.lastResumeSave.Load()
		if last != 0 && time.Duration(time.Now().UnixNano()-last) < time.Second {
			return
		}
	}
	if d.saveResume(d.snapshotCompleted()) == nil {
		d.lastResumeSave.Store(time.Now().UnixNano())
	}
}

// recordCompleted appends a fully finished range to the durable
// accumulator used by resume sidecars. Seeded from loadResume at
// start so prior progress is never dropped when workers reset.
//
// Deliberately does not dedup on every call: hot path is one append
// per completed chunk. saveResume and seedCompleted merge ranges.
func (d *Downloader) recordCompleted(t Task) {
	if d.resumePath == "" {
		return
	}
	d.completedMu.Lock()
	d.completed = append(d.completed, t)
	d.completedMu.Unlock()
}

// seedCompleted replaces the accumulator (used after loading a resume file).
func (d *Downloader) seedCompleted(tasks []Task) {
	d.completedMu.Lock()
	d.completed = dedupTasks(append([]Task(nil), tasks...))
	d.completedMu.Unlock()
}

// snapshotCompleted returns a copy of accumulated completed ranges.
// Compacts the accumulator (merge overlapping/adjacent) so repeated
// resume saves do not retain one entry per historical chunk forever.
func (d *Downloader) snapshotCompleted() []Task {
	d.completedMu.Lock()
	defer d.completedMu.Unlock()
	d.completed = dedupTasks(d.completed)
	out := make([]Task, len(d.completed))
	copy(out, d.completed)
	return out
}
