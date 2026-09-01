package fetch

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// stderrIsTTY reports whether stderr is attached to a terminal. The live
// progress bar is \r-based and would spam ANSI escape codes into logs and
// pipes, so gofetch only renders it to a real terminal; redirected stderr
// gets a single clean final line instead.
var (
	stderrTTYOnce sync.Once
	stderrTTYVal  bool
)

func stderrIsTTY() bool {
	stderrTTYOnce.Do(func() {
		fi, err := os.Stderr.Stat()
		stderrTTYVal = err == nil && fi.Mode()&os.ModeCharDevice != 0
	})
	return stderrTTYVal
}

// humanBytes returns a short IEC-formatted byte count.
func humanBytes(n int64) string {
	const k = 1024
	if n < k {
		return strconv.FormatInt(n, 10) + " B"
	}
	switch {
	case n < k*k:
		return formatFixed(float64(n)/k, 1) + " KB"
	case n < k*k*k:
		return formatFixed(float64(n)/k/k, 1) + " MB"
	case n < k*k*k*k:
		return formatFixed(float64(n)/k/k/k, 2) + " GB"
	case n < k*k*k*k*k:
		// TiB arm — gofetch targets large mirrors (1 TiB+ downloads per
		// range.go:73); without this arm a 1 TiB file reported as 1099.51 GB.
		return formatFixed(float64(n)/k/k/k/k, 2) + " TB"
	default:
		return formatFixed(float64(n)/k/k/k/k/k, 2) + " PB"
	}
}

// formatFixed formats f with digits decimal places, rounding half away
// from zero and zero-padding the fraction.
func formatFixed(f float64, digits int) string {
	scale := 1.0
	for i := 0; i < digits; i++ {
		scale *= 10
	}
	f += 0.5 / scale
	whole := int64(f)
	frac := strconv.FormatInt(int64((f-float64(whole))*scale), 10)
	for len(frac) < digits {
		frac = "0" + frac
	}
	return strconv.FormatInt(whole, 10) + "." + frac
}

// progress tracks total bytes downloaded across all workers.
// On demand, snapshot sums per-worker bytesDone (cheap — only called
// ~4 times/sec for the progress bar, plus once at finalize).
type progress struct {
	total     int64
	states    []*workerState
	prevDone  int64
	prevTime  int64
	ewmaSpeed float64
	// preDone tracks bytes completed before workers started (e.g., from resume).
	preDone atomic.Int64
}

func newProgress(total int64, states []*workerState) *progress {
	now := time.Now().UnixNano()
	return &progress{total: total, states: states, prevTime: now}
}

func (p *progress) add(n int64) {
	p.preDone.Add(n)
}

func (p *progress) snapshot() (int64, int64) {
	var sum int64
	for _, ws := range p.states {
		sum += ws.bytesDone.Load()
	}
	// Add bytes completed in previous sessions (preDone)
	sum += p.preDone.Load()
	if sum > p.total {
		sum = p.total
	}
	return sum, p.total
}

// speedAndETA computes the current EWMA download speed and estimated
// time remaining from the given current done count. NOT goroutine-safe:
// must be called only from printProgress (the main goroutine).
func (p *progress) speedAndETA(done int64) (bytesPerSec float64, eta time.Duration) {
	now := time.Now().UnixNano()
	dt := float64(now-p.prevTime) / float64(time.Second)
	if dt > 0.5 {
		dd := float64(done - p.prevDone)
		instant := dd / dt
		if p.ewmaSpeed == 0 {
			p.ewmaSpeed = instant
		} else {
			p.ewmaSpeed = 0.3*instant + 0.7*p.ewmaSpeed
		}
		p.prevDone = done
		p.prevTime = now
	}
	bytesPerSec = p.ewmaSpeed
	if bytesPerSec > 0 {
		remaining := p.total - done
		eta = time.Duration(float64(time.Second) * float64(remaining) / bytesPerSec)
	}
	return
}

func (d *Downloader) printProgress(p *progress, final bool) {
	if d.quiet {
		return
	}
	// Headless-clean when piped/redirected: only animate the \r bar on a
	// terminal. Redirected stderr prints the final state once, plainly.
	tty := stderrIsTTY()
	if !final && !tty {
		return
	}
	var bar [24]byte
	done, total := p.snapshot()
	w := len(bar)
	if total <= 0 {
		if final {
			if tty {
				fmt.Fprint(os.Stderr, "\r  ? / ?\033[K\n")
			} else {
				fmt.Fprintln(os.Stderr, "  ? / ?")
			}
		} else {
			fmt.Fprint(os.Stderr, "\r  ? / ?   ")
		}
		return
	}
	pct := min(max(float64(done)/float64(total), 0), 1)
	filled := int(pct*float64(w) + 0.5)
	if filled > w {
		filled = w
	}
	for i := range bar {
		if i < filled {
			bar[i] = '#'
		} else {
			bar[i] = '.'
		}
	}
	speed, eta := p.speedAndETA(done)
	speedStr := humanBytes(int64(speed)) + "/s"
	etaStr := ""
	if speed > 0 && !final {
		etaStr = "  ETA " + formatDuration(eta)
	}
	if final {
		line := fmt.Sprintf("  %s %5.1f%%  %s / %s  %s",
			bar[:], pct*100, humanBytes(done), humanBytes(total), speedStr)
		if tty {
			fmt.Fprintln(os.Stderr, "\r"+line+"\033[K")
		} else {
			fmt.Fprintln(os.Stderr, line)
		}
	} else {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s / %s  %s%s   ",
			bar[:], pct*100, humanBytes(done), humanBytes(total), speedStr, etaStr)
	}
}

// verbose logging — set on Downloader, not a global.
func (d *Downloader) vlog(format string, args ...any) {
	if d.verbose {
		fmt.Fprintf(os.Stderr, "gofetch: "+format+"\n", args...)
	}
}
