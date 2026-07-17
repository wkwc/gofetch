package fetch

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// humanBytes returns a short IEC-formatted byte count.
func humanBytes(n int64) string {
	const k = 1024
	if n < k {
		return strconv.FormatInt(n, 10) + " B"
	}
	switch {
	case n < k*k:
		return formatFixed1(float64(n)/k) + " KB"
	case n < k*k*k:
		return formatFixed1(float64(n)/k/k) + " MB"
	default:
		return formatFixed2(float64(n)/k/k/k) + " GB"
	}
}

func formatFixed1(f float64) string {
	f += 0.05
	whole := int64(f)
	frac := int64((f - float64(whole)) * 10)
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10)
}

func formatFixed2(f float64) string {
	f += 0.005
	whole := int64(f)
	frac := int64((f - float64(whole)) * 100)
	if frac < 10 {
		return strconv.FormatInt(whole, 10) + ".0" + strconv.FormatInt(frac, 10)
	}
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10)
}

// progress tracks total bytes downloaded across all workers.
// On demand, snapshot sums per-worker bytesDone (cheap — only called
// ~4 times/sec for the progress bar, plus once at finalize).
type progress struct {
	total     int64
	states    []*workerState
	startTime int64
	prevDone  int64
	prevTime  int64
	ewmaSpeed float64
}

func newProgress(total int64, states []*workerState) *progress {
	now := time.Now().UnixNano()
	return &progress{total: total, states: states, startTime: now, prevTime: now}
}

func (p *progress) snapshot() (int64, int64) {
	var sum int64
	for _, ws := range p.states {
		sum += ws.bytesDone.Load()
	}
	if sum > p.total {
		sum = p.total
	}
	return sum, p.total
}

func (p *progress) speedAndETA() (bytesPerSec float64, eta time.Duration) {
	now := time.Now().UnixNano()
	curDone, _ := p.snapshot()
	dt := float64(now-p.prevTime) / float64(time.Second)
	if dt > 0.5 {
		dd := float64(curDone - p.prevDone)
		instant := dd / dt
		if p.ewmaSpeed == 0 {
			p.ewmaSpeed = instant
		} else {
			p.ewmaSpeed = 0.3*instant + 0.7*p.ewmaSpeed
		}
		p.prevDone = curDone
		p.prevTime = now
	}
	bytesPerSec = p.ewmaSpeed
	if bytesPerSec > 0 {
		remaining := p.total - curDone
		eta = time.Duration(float64(time.Second) * float64(remaining) / bytesPerSec)
	}
	return
}

func (d *Downloader) printProgress(p *progress, final bool) {
	if d.quiet {
		return
	}
	var bar [24]byte
	done, total := p.snapshot()
	w := len(bar)
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
	for i := range bar {
		if i < filled {
			bar[i] = '#'
		} else {
			bar[i] = '.'
		}
	}
	speed, eta := p.speedAndETA()
	speedStr := humanBytes(int64(speed)) + "/s"
	etaStr := ""
	if speed > 0 && !final {
		etaStr = "  ETA " + formatDuration(eta)
	}
	if final {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s / %s  %s\033[K\n",
			bar[:], pct*100, humanBytes(done), humanBytes(total), speedStr)
	} else {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s / %s  %s%s   ",
			bar[:], pct*100, humanBytes(done), humanBytes(total), speedStr, etaStr)
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// verbose logging — set on Downloader, not a global.
func (d *Downloader) vlog(format string, args ...any) {
	if d.verbose {
		fmt.Fprintf(os.Stderr, "gofetch: "+format+"\n", args...)
	}
}
