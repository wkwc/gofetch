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
		// No live workers to sum (downloader only calls finalize on the
		// success path, after workers drained), so pass nil states and
		// reflect the whole file via preDone.
		prog = newProgress(d.totalSize, nil)
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
	fmt.Fprintf(os.Stderr, "  bytes:   %s\n", HumanBytes(done))
	fmt.Fprintf(os.Stderr, "  time:    %s\n", formatDuration(elapsed))
	fmt.Fprintf(os.Stderr, "  speed:   %s/s\n", HumanBytes(int64(speed)))
	fmt.Fprintf(os.Stderr, "  workers: %d\n", d.workerCount)

	return nil
}
