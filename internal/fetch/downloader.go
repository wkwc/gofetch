// Package fetch implements a streaming, range-aware HTTP downloader with
// adaptive work-stealing, multi-mirror support, resume capability, and
// integrity verification.
package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Task is a half-open byte range. Both ends are inclusive.
type Task struct {
	Start int64
	End   int64
}

func (t Task) Len() int64 { return t.End - t.Start + 1 }

// Queue is a FIFO (push-back, pop-front) work queue.
type Queue struct {
	mu    sync.Mutex
	tasks []Task
}

func (q *Queue) Push(t Task) {
	q.mu.Lock()
	q.tasks = append(q.tasks, t)
	q.mu.Unlock()
}

func (q *Queue) Pop() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.tasks)
	if n == 0 {
		return Task{}, false
	}
	t := q.tasks[0]
	copy(q.tasks, q.tasks[1:])
	q.tasks = q.tasks[:n-1]
	return t, true
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *Queue) Snapshot() []Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Task, len(q.tasks))
	copy(out, q.tasks)
	return out
}

// ResumeState persists download progress for resume capability.
type ResumeState struct {
	URL            string    `json:"url"`
	OutFile        string    `json:"out_file"`
	TotalSize      int64     `json:"total_size"`
	ExpectedSHA256 string    `json:"expected_sha256,omitempty"`
	Completed      []Task    `json:"completed"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ResumeStatePath returns the path for the resume state file.
func ResumeStatePath(outFile string) string {
	return outFile + ".gofetch.resume"
}

// SaveResumeState writes the current progress to disk.
func (d *Downloader) SaveResumeState(completed []Task) error {
	state := ResumeState{
		URL:            d.URL,
		OutFile:        d.OutFile,
		TotalSize:      d.totalSize,
		ExpectedSHA256: d.expectedSHA256,
		Completed:      completed,
		CreatedAt:      d.startTime,
		UpdatedAt:      time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(d.resumePath, data, 0o644)
}

// LoadResumeState reads and validates a resume state file.
func LoadResumeState(resumePath string, url string, totalSize int64) (*ResumeState, error) {
	data, err := os.ReadFile(resumePath)
	if err != nil {
		return nil, err
	}
	var state ResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	// Validate that state matches current download
	if state.URL != url || state.TotalSize != totalSize {
		return nil, errors.New("resume state mismatch (URL or size changed)")
	}
	return &state, nil
}

// ClearResumeState removes the resume state file.
func ClearResumeState(resumePath string) {
	_ = os.Remove(resumePath)
}

// Mirror represents a download source with its measured latency.
type Mirror struct {
	URL     string
	Latency time.Duration
	Healthy bool
}

// Downloader drives the parallel download of a single URL (or mirror set)
// to a single file.
type Downloader struct {
	URL            string   // primary URL (first mirror)
	Mirrors        []string // additional mirrors
	OutFile        string
	WorkersN       int
	BufSize        int
	Timeout        time.Duration
	UserAgent      string
	ExpectedSHA256 string       // optional integrity check (deprecated, use VerifyConfig)
	VerifyConfig   VerifyConfig // integrity verification config
	PreferQUIC     bool         // prefer QUIC/HTTP3 transport
	ResumeEnabled  bool         // enable resume capability

	client *http.Client

	// runtime state
	totalSize      int64
	expectedSHA256 string
	resumePath     string
	startTime      time.Time
}

// HashType identifies the hash algorithm for integrity verification.
type HashType string

const (
	// HashSHA256 is the SHA-256 hash algorithm.
	HashSHA256 HashType = "sha256"
)

// VerifyConfig describes what to verify.
type VerifyConfig struct {
	HashType   HashType
	Expected   string // hex-encoded
	Sidecar    bool   // if true, try to fetch hash from SidecarURL
	SidecarURL string // URL to fetch hash from (e.g., url + ".sha256")
}

type Options struct {
	WorkerCount    int
	BufSize        int
	Timeout        time.Duration
	Mirrors        []string     // additional mirrors (URLs)
	ExpectedSHA256 string       // expected SHA256 hex string
	Resume         bool         // enable resume capability
	PreferQUIC     bool         // prefer QUIC/HTTP3 transport
	VerifyConfig   VerifyConfig // integrity verification config
}

func NewDownloader(rawURL, outPath string, opt Options) *Downloader {
	if opt.WorkerCount < 1 {
		opt.WorkerCount = 4
	}
	if opt.BufSize < 1 {
		opt.BufSize = 64 * 1024
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 30 * time.Second
	}
	allMirrors := append([]string{rawURL}, opt.Mirrors...)
	d := &Downloader{
		URL:            rawURL,
		Mirrors:        allMirrors,
		OutFile:        outPath,
		WorkersN:       opt.WorkerCount,
		BufSize:        opt.BufSize,
		Timeout:        opt.Timeout,
		UserAgent:      "gofetch/0.1",
		ExpectedSHA256: opt.ExpectedSHA256,
		VerifyConfig:   opt.VerifyConfig,
		PreferQUIC:     opt.PreferQUIC,
		ResumeEnabled:  opt.Resume,
		resumePath:     ResumeStatePath(outPath),
	}
	d.client = &http.Client{
		Timeout: opt.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        opt.WorkerCount * 2,
			MaxIdleConnsPerHost: opt.WorkerCount,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		},
	}
	return d
}

// Download fetches d.URL (with mirror fallback) into d.OutFile.
// Returns nil on success or the first worker error encountered.
func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	// Probe the best mirror
	mirror, probe, err := d.selectMirror(ctx)
	if err != nil {
		return fmt.Errorf("mirror selection: %w", err)
	}
	d.URL = mirror.URL

	// Check for resume
	var completed []Task
	if probe.total > 0 && d.ResumeEnabled {
		d.totalSize = probe.total
		// Prefer VerifyConfig.Expected over legacy ExpectedSHA256
		if d.VerifyConfig.Expected != "" {
			d.expectedSHA256 = d.VerifyConfig.Expected
		} else {
			d.expectedSHA256 = d.ExpectedSHA256
		}
		if state, err := LoadResumeState(d.resumePath, d.URL, d.totalSize); err == nil {
			completed = state.Completed
		}
	}

	if !probe.supportsRanges {
		return d.singleDownload(ctx, probe.total, completed)
	}
	return d.rangeDownload(ctx, probe.total, completed)
}

func (d *Downloader) selectMirror(ctx context.Context) (*Mirror, probeInfo, error) {
	type result struct {
		mirror *Mirror
		info   probeInfo
		err    error
	}
	ch := make(chan result, len(d.Mirrors))
	for _, u := range d.Mirrors {
		go func(rawURL string) {
			m := &Mirror{URL: rawURL}
			info, err := d.probeURL(ctx, rawURL)
			m.Healthy = err == nil && info.total >= 0
			if m.Healthy {
				// measure latency with a tiny range request
				start := time.Now()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
				req.Header.Set("Range", "bytes=0-0")
				req.Header.Set("User-Agent", d.UserAgent)
				resp, err := d.client.Do(req)
				if err == nil {
					resp.Body.Close()
					m.Latency = time.Since(start)
				}
			}
			ch <- result{m, info, err}
		}(u)
	}
	var best *result
	for i := 0; i < len(d.Mirrors); i++ {
		r := <-ch
		if r.err == nil && r.mirror.Healthy {
			if best == nil || r.mirror.Latency < best.mirror.Latency {
				best = &r
			}
		}
	}
	if best == nil {
		return nil, probeInfo{}, fmt.Errorf("all mirrors failed")
	}
	return best.mirror, best.info, nil
}

func (d *Downloader) probeURL(ctx context.Context, rawURL string) (probeInfo, error) {
	if info, ok, err := d.probeHeadURL(ctx, rawURL); ok || err != nil {
		return info, err
	}
	return d.probeRangeGetURL(ctx, rawURL)
}

func (d *Downloader) probeHeadURL(ctx context.Context, rawURL string) (probeInfo, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return probeInfo{}, false, err
	}
	req.Header.Set("User-Agent", d.UserAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusBadRequest {
		return probeInfo{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return probeInfo{}, false, fmt.Errorf("HEAD %s: status %d", rawURL, resp.StatusCode)
	}
	ar := resp.Header.Get("Accept-Ranges")
	supports := ar != "" && ar != "none"
	return probeInfo{supportsRanges: supports, total: resp.ContentLength}, true, nil
}

func (d *Downloader) probeRangeGetURL(ctx context.Context, rawURL string) (probeInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeInfo{}, err
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", d.UserAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
		cr := resp.Header.Get("Content-Range")
		start, end, total, ok := parseContentRange(cr)
		if !ok {
			return probeInfo{}, fmt.Errorf("malformed Content-Range: %q", cr)
		}
		_ = start
		_ = end
		return probeInfo{supportsRanges: true, total: total}, nil
	case http.StatusOK:
		return probeInfo{supportsRanges: false, total: resp.ContentLength}, nil
	default:
		return probeInfo{}, fmt.Errorf("range GET %s: status %d", rawURL, resp.StatusCode)
	}
}

// probeInfo captures what we learned from the server about the target resource.
type probeInfo struct {
	supportsRanges bool
	total          int64
}

// singleDownload is used when the server does not support Range.
func (d *Downloader) singleDownload(ctx context.Context, total int64, completed []Task) error {
	f, err := allocateSparse(d.OutFile, total)
	if err != nil {
		return err
	}
	defer f.Close()
	if total <= 0 {
		return nil
	}
	progress := newProgress(total)
	// Filter out already-completed ranges
	remaining := subtractRanges(Task{Start: 0, End: total - 1}, completed)
	for _, t := range remaining {
		if err := d.runTask(ctx, nil, t, progress, f); err != nil {
			return err
		}
	}
	return d.finalize(ctx, f, progress)
}

// rangeDownload fans the file out across N workers with a stealing monitor.
func (d *Downloader) rangeDownload(ctx context.Context, total int64, completed []Task) error {
	f, err := allocateSparse(d.OutFile, total)
	if err != nil {
		return err
	}
	defer f.Close()
	if total <= 0 {
		return nil
	}

	progress := newProgress(total)
	// Mark completed ranges as done
	for _, t := range completed {
		progress.add(t.Len())
	}

	seed := subtractRangesFromList(splitRange(0, total-1, d.WorkersN), completed)
	queue := &Queue{}
	for _, t := range seed {
		queue.Push(t)
	}
	states := make([]*workerState, d.WorkersN)
	for i := range states {
		states[i] = newWorkerState()
	}

	var workers sync.WaitGroup
	workers.Add(d.WorkersN)
	for _, ws := range states {
		go func(ws *workerState) {
			defer workers.Done()
			d.workerLoop(ctx, ws, queue, progress, f)
		}(ws)
	}

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		d.monitor(monitorCtx, states, queue)
	}()

	// Periodic resume state save
	resumeTicker := time.NewTicker(5 * time.Second)
	defer resumeTicker.Stop()

	done := make(chan struct{})
	go func() {
		workers.Wait()
		stopMonitor()
		monitor.Wait()
		close(done)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			d.printProgress(progress, true)
			for _, ws := range states {
				if err, ok := ws.err(); ok && err != nil {
					return err
				}
			}
			return d.finalize(ctx, f, progress)
		case <-resumeTicker.C:
			completed := collectCompleted(states)
			_ = d.SaveResumeState(completed)
		case <-time.After(250 * time.Millisecond):
			d.printProgress(progress, false)
		}
	}
}

func (d *Downloader) finalize(ctx context.Context, f *os.File, progress *progress) error {
	// Verify SHA256 if expected
	if d.expectedSHA256 != "" {
		d.printProgress(progress, true)
		fmt.Fprint(os.Stderr, "  verifying SHA256... ")
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := verifySHA256(d.OutFile, d.expectedSHA256); err != nil {
			return fmt.Errorf("SHA256 mismatch: %w", err)
		}
		fmt.Fprint(os.Stderr, "OK\n")
	}
	ClearResumeState(d.resumePath)
	return nil
}

func verifySHA256(path, expectedHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expectedHex {
		return fmt.Errorf("expected %s, got %s", expectedHex, got)
	}
	return nil
}

// subtractRanges removes completed ranges from a single range.
func subtractRanges(full Task, completed []Task) []Task {
	if len(completed) == 0 {
		return []Task{full}
	}
	// Sort completed by start
	sorted := make([]Task, len(completed))
	copy(sorted, completed)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Start < sorted[i].Start {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	var out []Task
	cursor := full.Start
	for _, c := range sorted {
		if c.End < full.Start || c.Start > full.End {
			continue
		}
		if c.Start > cursor {
			out = append(out, Task{Start: cursor, End: c.Start - 1})
		}
		cursor = max(cursor, c.End+1)
	}
	if cursor <= full.End {
		out = append(out, Task{Start: cursor, End: full.End})
	}
	return out
}

func subtractRangesFromList(ranges []Task, completed []Task) []Task {
	var out []Task
	for _, r := range ranges {
		out = append(out, subtractRanges(r, completed)...)
	}
	return out
}

// collectCompleted gathers all finished ranges from worker states.
func collectCompleted(states []*workerState) []Task {
	var completed []Task
	for _, ws := range states {
		t := ws.curTask.Load()
		if t == nil {
			continue
		}
		// If bytesDone == task.Len(), it's complete
		if ws.bytesDone.Load() >= t.Len() {
			completed = append(completed, *t)
		}
	}
	return completed
}

func splitRange(lo, hi int64, n int) []Task {
	if n < 1 {
		n = 1
	}
	size := hi - lo + 1
	chunk := size / int64(n)
	if chunk < 1 {
		chunk = 1
	}
	out := make([]Task, 0, n)
	for i := 0; i < n; i++ {
		start := lo + int64(i)*chunk
		end := start + chunk - 1
		if i == n-1 {
			end = hi
		}
		out = append(out, Task{Start: start, End: end})
	}
	return out
}

// workerState is the live state of one worker.
type workerState struct {
	curTask   atomic.Pointer[Task]
	bytesDone atomic.Int64
	startedAt atomic.Int64
	cancelFn  atomic.Pointer[context.CancelFunc]
	errOnce   sync.Once
	errValue  error
}

func newWorkerState() *workerState { return &workerState{} }

func (ws *workerState) setErr(err error) {
	if err == nil {
		return
	}
	ws.errOnce.Do(func() { ws.errValue = err })
}

func (ws *workerState) err() (error, bool) {
	ws.errOnce.Do(func() {})
	return ws.errValue, ws.errValue != nil
}

// progress atomically increments a counter protected by a mutex.
type progress struct {
	mu    sync.Mutex
	total int64
	done  int64
}

func newProgress(total int64) *progress { return &progress{total: total} }

func (p *progress) add(n int64) {
	p.mu.Lock()
	p.done += n
	p.mu.Unlock()
}

func (p *progress) snapshot() (int64, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done, p.total
}

// workerLoop spins a worker: pop a task, run it, repeat until queue empty.
func (d *Downloader) workerLoop(ctx context.Context, ws *workerState, queue *Queue, prog *progress, f *os.File) {
	for {
		if err := ctx.Err(); err != nil {
			ws.setErr(err)
			return
		}
		task, ok := queue.Pop()
		if !ok {
			return
		}
		err := d.runTask(ctx, ws, task, prog, f)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			ws.setErr(err)
			return
		}
		if queue.Len() == 0 {
			return
		}
	}
}

// runTask performs the HTTP request for one task and writes the body into f.
func (d *Downloader) runTask(ctx context.Context, ws *workerState, task Task, prog *progress, f *os.File) error {
	taskRef := task
	if ws != nil {
		ws.curTask.Store(&taskRef)
		ws.bytesDone.Store(0)
		ws.startedAt.Store(time.Now().UnixNano())
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ws != nil {
		cf := context.CancelFunc(cancel)
		ws.cancelFn.Store(&cf)
		defer ws.cancelFn.Store(nil)
	}

	req, err := http.NewRequestWithContext(rctx, http.MethodGet, d.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", task.Start, task.End))
	req.Header.Set("User-Agent", d.UserAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("range %d-%d: status %d", task.Start, task.End, resp.StatusCode)
	}

	buf := d.acquireBuf()
	defer d.releaseBuf(buf)
	cursor := task.Start
	end := task.End

	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if cursor+int64(n) > end+1 {
				n = int(end - cursor + 1)
				if n <= 0 {
					return nil
				}
			}
			if _, err := f.WriteAt(buf[:n], cursor); err != nil {
				return err
			}
			cursor += int64(n)
			prog.add(int64(n))
			if ws != nil {
				ws.bytesDone.Store(cursor - task.Start)
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
		if cursor > end {
			return nil
		}
	}
}

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

func (d *Downloader) acquireBuf() []byte {
	bp := bufPool.Get().(*[]byte)
	if cap(*bp) < d.BufSize {
		*bp = make([]byte, d.BufSize)
		return *bp
	}
	return (*bp)[:d.BufSize]
}

func (d *Downloader) releaseBuf(b []byte) {
	if cap(b) < 1 || cap(b) > 1<<20 {
		return
	}
	bp := b[:cap(b):cap(b)]
	bufPool.Put(&bp)
}

const (
	monitorInterval   = 500 * time.Millisecond
	stealMinChunk     = 256 * 1024
	stealSlowBytes    = 1 << 20
	stealGracePeriod  = 1500 * time.Millisecond
	stealMinBytesDone = 0
)

func (d *Downloader) monitor(ctx context.Context, states []*workerState, queue *Queue) {
	t := time.NewTicker(monitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		for _, ws := range states {
			leftover, cancelFn, yes := ws.stealPlan(now)
			if !yes {
				continue
			}
			cancelFn()
			queue.Push(leftover)
		}
	}
}

func (ws *workerState) stealPlan(now time.Time) (Task, context.CancelFunc, bool) {
	t := ws.curTask.Load()
	if t == nil {
		return Task{}, nil, false
	}
	if t.Len() < 2*stealMinChunk {
		return Task{}, nil, false
	}
	elapsed := now.Sub(time.Unix(0, ws.startedAt.Load()))
	if elapsed < stealGracePeriod {
		return Task{}, nil, false
	}
	if ws.bytesDone.Load() >= stealSlowBytes {
		return Task{}, nil, false
	}
	progressBytes := ws.bytesDone.Load()
	if progressBytes < stealMinBytesDone {
		return Task{}, nil, false
	}
	cf := ws.cancelFn.Load()
	if cf == nil {
		return Task{}, nil, false
	}
	newStart := t.Start + progressBytes
	if newStart+stealMinChunk > t.End {
		return Task{}, nil, false
	}
	return Task{Start: newStart, End: t.End}, *cf, true
}

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

func (d *Downloader) printProgress(p *progress, final bool) {
	done, total := p.snapshot()
	const barWidth = 24
	if total <= 0 {
		if final {
			fmt.Fprint(os.Stderr, "\r  ? / ?\n")
		} else {
			fmt.Fprint(os.Stderr, "\r  ? / ?   ")
		}
		return
	}
	pct := float64(done) / float64(total)
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	filled := int(pct*float64(barWidth) + 0.5)
	if filled < 0 {
		filled = 0
	}
	if filled > barWidth {
		filled = barWidth
	}
	bar := make([]byte, 0, barWidth)
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar = append(bar, '#')
		} else {
			bar = append(bar, '.')
		}
	}
	if final {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s/%d\n", bar, pct*100, humanBytes(done), total)
	} else {
		fmt.Fprintf(os.Stderr, "\r  %s %5.1f%%  %s/%d   ", bar, pct*100, humanBytes(done), total)
	}
}

func humanBytes(n int64) string {
	const k = 1024
	if n < k {
		return fmt.Sprintf("%d B", n)
	}
	if n < k*k {
		return fmt.Sprintf("%.1f KB", float64(n)/k)
	}
	if n < k*k*k {
		return fmt.Sprintf("%.1f MB", float64(n)/k/k)
	}
	return fmt.Sprintf("%.2f GB", float64(n)/k/k/k)
}

func parseContentRange(v string) (start, end, total int64, ok bool) {
	const prefix = "bytes "
	if len(v) < len(prefix) || v[:len(prefix)] != prefix {
		return 0, 0, 0, false
	}
	rest := v[len(prefix):]
	dash := -1
	slash := -1
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '-':
			dash = i
		case '/':
			slash = i
		}
		if dash >= 0 && slash >= 0 {
			break
		}
	}
	if dash < 0 || slash < 0 || slash < dash {
		return 0, 0, 0, false
	}
	var err error
	if start, err = parseInt(rest[:dash]); err != nil {
		return 0, 0, 0, false
	}
	if end, err = parseInt(rest[dash+1 : slash]); err != nil {
		return 0, 0, 0, false
	}
	if total, err = parseInt(rest[slash+1:]); err != nil {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

func parseInt(s string) (int64, error) {
	var n int64
	if s == "" {
		return 0, errors.New("empty")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, errors.New("non-digit")
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}
