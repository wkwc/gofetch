package fetch

import (
	"fmt"
	"os"
	"sync"
)

// progress tracks total bytes downloaded across all workers.
// The mutex is needed because add + snapshot is a read-modify-write
// pair, but the contention rate is one increment per ~64 KiB buffered.
type progress struct {
	mu    sync.Mutex
	total int64
	done  int64
}

func newProgress(total int64) *progress { return &progress{total: total} }

// add increments done by n, capped at total to prevent overshoot
// when stolen tasks re-download bytes already counted by another worker.
func (p *progress) add(n int64) {
	p.mu.Lock()
	if remaining := p.total - p.done; n > remaining {
		n = remaining
	}
	if n > 0 {
		p.done += n
	}
	p.mu.Unlock()
}

// snapshot returns (done, total).
func (p *progress) snapshot() (int64, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done, p.total
}

// progressBar is a reusable buffer for the progress bar display.
// Allocated once and reused across all printProgress calls.
var progressBar = [24]byte{}

// printProgress renders the progress bar. Final=true emits a newline
// and clears via ANSI CSI-K.
func (d *Downloader) printProgress(p *progress, final bool) {
	done, total := p.snapshot()
	const w = 24
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
