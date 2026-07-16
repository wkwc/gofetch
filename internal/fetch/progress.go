package fetch

import (
	"fmt"
	"os"
	"sync/atomic"
)

// progress tracks total bytes downloaded across all workers.
// Uses atomic CAS for lock-free increments; the cap at total
// prevents overshoot when stolen tasks re-download bytes already
// counted by another worker.
type progress struct {
	total int64
	done  atomic.Int64
}

func newProgress(total int64) *progress { return &progress{total: total} }

// add increments done by n, capped at total to prevent overshoot.
func (p *progress) add(n int64) {
	for {
		cur := p.done.Load()
		remaining := p.total - cur
		if n > remaining {
			n = remaining
		}
		if n <= 0 {
			return
		}
		if p.done.CompareAndSwap(cur, cur+n) {
			return
		}
	}
}

// snapshot returns (done, total).
func (p *progress) snapshot() (int64, int64) {
	return p.done.Load(), p.total
}

// progressBar is a reusable buffer for the progress bar display.
// Allocated once and reused across all printProgress calls.
var progressBar = [24]byte{}

// printProgress renders the progress bar. Final=true emits a newline
// and clears via ANSI CSI-K.
func (d *Downloader) printProgress(p *progress, final bool) {
	done, total := p.snapshot()
	w := len(progressBar)
	if total <= 0 {
		if final {
			fmt.Fprint(os.Stderr, "\r  ? / ?\033[K\n")
		} else {
			fmt.Fprint(os.Stderr, "\r  ? / ?   ")
		}
		return
	}
	pct := clamp(float64(done)/float64(total), 0, 1)
	filled := int(pct*float64(w) + 0.5)
	if filled > w {
		filled = w
	}
	for i := range progressBar {
		if i < filled {
			progressBar[i] = '#'
		} else {
			progressBar[i] = '.'
		}
	}
	if final {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s / %s\033[K\n", progressBar[:], pct*100, humanBytes(done), humanBytes(total))
	} else {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s / %s   ", progressBar[:], pct*100, humanBytes(done), humanBytes(total))
	}
}

// clamp returns v clamped to [lo, hi].
func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
