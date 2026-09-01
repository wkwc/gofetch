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
	// Resume sidecar lifecycle on the way out:
	//   - success                       → clear (no longer needed)
	//   - failure WITH a per-chunk manifest → KEEP: finalize already
	//     surgically trimmed the corrupt byte ranges (BadChunks) so the
	//     next run re-fetches only those. Self-correcting: a missed
	//     corruption gets caught next run and re-trimmed.
	//   - failure WITHOUT a manifest    → clear: corruption can't be
	//     localized to a chunk, so keeping `completed` would make the
	//     next run skip the bad ranges and fail forever. Force a full
	//     re-download to guarantee recovery.
	if d.resumePath != "" {
		defer func() {
			switch {
			case err == nil:
				_ = clearResume(d.resumePath)
			case d.manifest != nil:
				// keep trimmed sidecar (see comment above)
			default:
				_ = clearResume(d.resumePath)
			}
		}()
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
			// Surgical resume trim: identify which chunk(s) actually hash
			// mismatch and drop just those byte ranges from the
			// accumulator, then persist a trimmed sidecar. The next run
			// re-fetches only the corrupt spans instead of the whole
			// file. (Manifest-less hash failures fall through to the
			// "no manifest" branch of the clear-defer above, which clears
			// entirely — the only safe option without per-chunk locality.)
			if d.resumePath != "" {
				if bad := d.manifest.BadChunks(d.outFile); len(bad) > 0 {
					d.dropCompletedOverlapping(bad)
					// finalize only runs on the primary URL (never a
					// mirror), and workers have drained, so no states.
					_ = d.saveResume(d.url, d.snapshotCompleted(), nil)
				}
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
func (d *Downloader) maybeSaveResume(force bool, url string, states []*workerState) {
	if d.resumePath == "" {
		return
	}
	if !force {
		last := d.lastResumeSave.Load()
		if last != 0 && time.Duration(time.Now().UnixNano()-last) < time.Second {
			return
		}
	}
	if d.saveResume(url, d.snapshotCompleted(), states) == nil {
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

// dropCompletedOverlapping surgically trims the resume accumulator so
// that the byte ranges corresponding to corrupt manifest chunks are no
// longer claimed as completed — the next run re-fetches just those
// spans instead of the entire file. Because `completed` is stored
// merged (dedupTasks unions adjacent/overlapping ranges into one big
// Task), a naive "drop any task that overlaps a bad chunk" would
// discard the whole merged range and be no better than clearing. We
// instead SUBTRACT the union of bad byte ranges from each completed
// task, keeping the surviving fragments.
//
// bad chunks are [c.Start, c.End] inclusive; converted to Task
// internally. Returns the trimmed accumulator via d.completed.
func (d *Downloader) dropCompletedOverlapping(bad []ChunkHash) {
	if len(bad) == 0 {
		return
	}
	// Build sorted + merged bad byte ranges.
	badRanges := make([]Task, 0, len(bad))
	for _, c := range bad {
		badRanges = append(badRanges, Task{Start: c.Start, End: c.End})
	}
	badRanges = dedupTasks(badRanges) // sorts + merges, O(n log n)

	d.completedMu.Lock()
	kept := d.completed[:0]
	for _, t := range d.completed {
		// Subtract every bad range that intersects [t.Start, t.End];
		// badRanges is sorted by Start so we can stop early.
		cursor := t.Start
		droppedAny := false
		for _, b := range badRanges {
			if b.End < cursor {
				continue // bad range entirely before our cursor
			}
			if b.Start > t.End {
				break // no further bad range touches this task
			}
			// b intersects [cursor, t.End] (and b.Start >= cursor, b.End <= t.End region)
			if b.Start > cursor {
				kept = append(kept, Task{Start: cursor, End: b.Start - 1})
			}
			cursor = b.End + 1
			droppedAny = true
			if cursor > t.End {
				break
			}
		}
		if cursor <= t.End {
			kept = append(kept, Task{Start: cursor, End: t.End})
		} else if !droppedAny && cursor == t.Start {
			// Defensive: cursor never advanced AND nothing dropped → t untouched.
			kept = append(kept, t)
		}
	}
	d.completed = dedupTasks(kept)
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
