package fetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestWorkerStateError(t *testing.T) {
	ws := newWorkerState()

	if err, ok := ws.err(); ok {
		t.Fatalf("unexpected initial error: %v", err)
	}

	ws.setErr(errors.New("boom"))
	err, ok := ws.err()
	if !ok || err.Error() != "boom" {
		t.Fatalf("err = %v ok = %v, want boom/true", err, ok)
	}

	// Second setErr should not overwrite
	ws.setErr(errors.New("second"))
	err, _ = ws.err()
	if err.Error() != "boom" {
		t.Fatalf("err should not be overwritten: got %v", err)
	}

	// nil is a no-op
	ws2 := newWorkerState()
	ws2.setErr(nil)
	if _, ok := ws2.err(); ok {
		t.Fatal("nil error should not set anything")
	}
}

func TestReset(t *testing.T) {
	ws := newWorkerState()
	task := Task{100, 200}
	ws.reset(task)

	if got := ws.curTask.Load(); got == nil || *got != task {
		t.Fatalf("curTask = %v, want %v", got, task)
	}
	if got := ws.bytesDone.Load(); got != 0 {
		t.Errorf("bytesDone = %d, want 0", got)
	}
	if ws.stealFlag.Load() {
		t.Error("stealFlag should reset to false")
	}
	if ws.startedAt.Load() == 0 {
		t.Error("startedAt should be set")
	}
}

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"ECONNRESET", syscall.ECONNRESET, true},
		{"ECONNREFUSED", syscall.ECONNREFUSED, true},
		{"ETIMEDOUT", syscall.ETIMEDOUT, true},
		{"EPIPE", syscall.EPIPE, true},
		{"ENOENT", syscall.ENOENT, false},
		{"EACCES", syscall.EACCES, false},
		{"os.PathError ENOENT", &os.PathError{Op: "open", Path: "/nope", Err: syscall.ENOENT}, false},
		{"generic", errors.New("something"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransient(tt.err); got != tt.want {
				t.Errorf("isTransient(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		want   time.Duration
	}{
		{name: "empty header", header: http.Header{}, want: 0},
		{name: "valid seconds", header: http.Header{"Retry-After": []string{"5"}}, want: 5 * time.Second},
		{name: "zero seconds", header: http.Header{"Retry-After": []string{"0"}}, want: 5 * time.Second},
		{name: "negative seconds", header: http.Header{"Retry-After": []string{"-1"}}, want: 5 * time.Second},
		{name: "over max (301)", header: http.Header{"Retry-After": []string{"301"}}, want: 5 * time.Second},
		{name: "at max (300)", header: http.Header{"Retry-After": []string{"300"}}, want: 300 * time.Second},
		{name: "non-numeric", header: http.Header{"Retry-After": []string{"Mon, 01 Jan 2024"}}, want: 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRetryAfter(tt.header)
			if got != tt.want {
				t.Errorf("parseRetryAfter = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("http-date in future", func(t *testing.T) {
		when := time.Now().Add(30 * time.Second).UTC()
		h := http.Header{"Retry-After": []string{when.Format(http.TimeFormat)}}
		got := parseRetryAfter(h)
		if got <= 25*time.Second || got > 35*time.Second {
			t.Errorf("parseRetryAfter(http-date) = %v, want ~30s", got)
		}
	})

	t.Run("http-date in past", func(t *testing.T) {
		when := time.Now().Add(-time.Hour).UTC()
		h := http.Header{"Retry-After": []string{when.Format(http.TimeFormat)}}
		if got := parseRetryAfter(h); got != retryAfterDefault {
			t.Errorf("parseRetryAfter(past http-date) = %v, want default", got)
		}
	})
}

func TestSleepCtx(t *testing.T) {
	t.Run("immediate cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if sleepCtx(ctx, time.Second) {
			t.Error("expected false for cancelled context")
		}
	})

	t.Run("zero duration", func(t *testing.T) {
		if !sleepCtx(context.Background(), 0) {
			t.Error("expected true for zero duration")
		}
	})

	t.Run("negative duration", func(t *testing.T) {
		if !sleepCtx(context.Background(), -1) {
			t.Error("expected true for negative duration")
		}
	})

	t.Run("normal sleep", func(t *testing.T) {
		start := time.Now()
		if !sleepCtx(context.Background(), 10*time.Millisecond) {
			t.Error("expected true for normal sleep")
		}
		if time.Since(start) < 5*time.Millisecond {
			t.Error("sleep returned too quickly")
		}
	})
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "0ms"},
		{500 * time.Millisecond, "500ms"},
		{999 * time.Millisecond, "999ms"},
		{time.Second, "1.0s"},
		{5500 * time.Millisecond, "5.5s"},
		{time.Minute, "1m0s"},
		{90 * time.Second, "1m30s"},
		{3661 * time.Second, "61m1s"},
	}
	for _, tt := range tests {
		if got := formatDuration(tt.input); got != tt.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestStealPlanIdle(t *testing.T) {
	ws := newWorkerState()
	// No task → no steal
	_, _, ok := ws.stealPlan(time.Now())
	if ok {
		t.Error("expected no steal for idle worker")
	}
}

func TestStealPlanSmallTask(t *testing.T) {
	ws := newWorkerState()
	// Task smaller than 2*stealMinChunk → no steal
	ws.reset(Task{Start: 0, End: stealMinChunk})
	_, _, ok := ws.stealPlan(time.Now().Add(2 * time.Second))
	if ok {
		t.Error("expected no steal for small task")
	}
}

func TestStealPlanWithinGracePeriod(t *testing.T) {
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	// Started now → within grace period
	_, _, ok := ws.stealPlan(time.Now())
	if ok {
		t.Error("expected no steal within grace period")
	}
}

func TestStealPlanSlowWorker(t *testing.T) {
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	ws.bytesDone.Store(stealSlowBytes + 1) // made enough progress
	cf := context.CancelFunc(func() {})
	ws.cancelFn.Store(&cf)

	// Enough time has passed, but enough bytes transferred
	_, _, ok := ws.stealPlan(time.Now().Add(2 * time.Second))
	if ok {
		t.Error("expected no steal for worker that made sufficient progress")
	}
}

func TestStealPlanStealCandidate(t *testing.T) {
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	ws.bytesDone.Store(100) // barely any progress
	cf := context.CancelFunc(func() {})
	ws.cancelFn.Store(&cf)

	// Enough time has passed, not enough bytes → steal candidate
	newTask, cancel, ok := ws.stealPlan(time.Now().Add(2 * time.Second))
	if !ok {
		t.Fatal("expected steal candidate")
	}
	if cancel == nil {
		t.Error("expected non-nil cancel function")
	}
	if newTask.Start != 100 {
		t.Errorf("newTask.Start = %d, want 100", newTask.Start)
	}
	if newTask.End != stealMinChunk*4 {
		t.Errorf("newTask.End = %d, want %d", newTask.End, stealMinChunk*4)
	}
}

func TestStealPlanCancelFnNil(t *testing.T) {
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	ws.bytesDone.Store(100)
	// cancelFn is nil (not yet stored by workerLoop)

	_, _, ok := ws.stealPlan(time.Now().Add(2 * time.Second))
	if ok {
		t.Error("expected no steal when cancelFn is nil")
	}
}

func TestStealPlanZeroProgress(t *testing.T) {
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	// bytesDone stays 0, cancelFn is set
	cf := context.CancelFunc(func() {})
	ws.cancelFn.Store(&cf)

	_, _, ok := ws.stealPlan(time.Now().Add(2 * time.Second))
	if ok {
		t.Error("expected no steal when bytesDone is 0")
	}
}

// TestReadBodyMidRangeEOFIsTransient pins the G6 fix: readBody wrapping
// a plain io.EOF received mid-range (cursor < end) as io.ErrUnexpectedEOF
// so that isTransient() classifies the failure as retryable and the
// worker requeues the range. A bytes.Reader surfaces plain io.EOF (not
// ErrUnexpectedEOF), which is exactly the branch that previously
// returned a non-transient fmt.Errorf and fatally aborted downloads.
func TestReadBodyMidRangeEOFIsTransient(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mid.bin")
	fw, err := allocateFileWriter(path, 4096, false, false)
	if err != nil {
		t.Fatalf("allocateFileWriter: %v", err)
	}
	defer func() { _ = fw.Close() }()

	d := &Downloader{autoConfig: AutoConfig{BufSize: 4096}}
	// Task wants bytes 0-1023, but the body only has 100 bytes then EOF.
	task := Task{Start: 0, End: 1023}
	ws := newWorkerState()
	ws.reset(task)
	body := io.NopCloser(bytes.NewReader(make([]byte, 100)))

	err = d.readBody(context.Background(), task, fw, ws, body)
	if err == nil {
		t.Fatal("expected an error for mid-range EOF, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("mid-range EOF must wrap io.ErrUnexpectedEOF so isTransient retries; got %v", err)
	}
	if !isTransient(err) {
		t.Fatalf("isTransient must classify mid-range EOF as retryable; got false for %v", err)
	}
}

// TestRecordWrittenPrefixOnRequeue pins G-F4: when steal/retry leaves a
// partial written prefix, that span is committed to the resume accumulator
// before the remainder is requeued, so a crash+resume does not re-fetch it.
func TestRecordWrittenPrefixOnRequeue(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	d := &Downloader{
		resumePath: resumePath(out),
		// Zero RetryBase/Cap → Backoff is ~0 so requeue is immediate.
		autoConfig: AutoConfig{RetryMax: 10},
	}
	ws := newWorkerState()
	task := Task{Start: 1000, End: 9999}
	ws.reset(task)
	const written int64 = 500
	ws.bytesDone.Store(written)

	q := NewQueue(1, 0)
	ctx := context.Background()
	d.requeueUnfinished(ctx, ws, task, q)

	// Written prefix must be in completed.
	got := d.snapshotCompleted()
	wantPrefix := Task{Start: 1000, End: 1000 + written - 1}
	if len(got) != 1 || got[0] != wantPrefix {
		t.Fatalf("completed = %v, want [%v]", got, wantPrefix)
	}

	// Remainder only must be requeued.
	rem, ok := q.Pop()
	if !ok {
		t.Fatal("expected remainder on queue")
	}
	wantRem := Task{Start: 1000 + written, End: 9999}
	if rem != wantRem {
		t.Fatalf("remainder = %v, want %v", rem, wantRem)
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("queue should have only the remainder")
	}
}

// TestRecordWrittenPrefixOnStealSkip covers the stealFlag path: monitor
// already pushed the leftover, so the worker skips requeueUnfinished but
// must still commit its written prefix for resume.
func TestRecordWrittenPrefixOnStealSkip(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	d := &Downloader{resumePath: resumePath(out)}
	ws := newWorkerState()
	task := Task{Start: 0, End: 1 << 20}
	ws.reset(task)
	const written int64 = 777
	ws.bytesDone.Store(written)
	ws.stealFlag.Store(true)

	// Mirror the cancel-branch contract in workerLoop.
	if !ws.stealFlag.Swap(false) {
		t.Fatal("stealFlag should have been set")
	}
	d.recordWrittenPrefix(ws, task)

	got := d.snapshotCompleted()
	want := Task{Start: 0, End: written - 1}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("completed = %v, want [%v]", got, want)
	}
}

// TestRecordWrittenPrefixNoProgress is a no-op when nothing was written.
func TestRecordWrittenPrefixNoProgress(t *testing.T) {
	d := &Downloader{resumePath: "/tmp/unused.gofetch.resume"}
	ws := newWorkerState()
	task := Task{Start: 0, End: 100}
	ws.reset(task)
	d.recordWrittenPrefix(ws, task)
	if got := d.snapshotCompleted(); len(got) != 0 {
		t.Fatalf("expected empty completed, got %v", got)
	}
}

func TestStealPlanClaimsOnce(t *testing.T) {
	// The first stealPlan claims the cancel fn via CAS; a second call on
	// the same worker must refuse (otherwise the monitor could publish two
	// overlapping leftover ranges for one worker).
	ws := newWorkerState()
	ws.reset(Task{Start: 0, End: stealMinChunk * 4})
	ws.bytesDone.Store(512 << 10)
	cf := context.CancelFunc(func() {})
	ws.cancelFn.Store(&cf)

	now := time.Now().Add(2 * time.Second)
	if leftover, cancel, ok := ws.stealPlan(now); !ok {
		t.Fatal("expected first steal to succeed")
	} else if cancel == nil {
		t.Fatal("expected a cancel func")
	} else if leftover.Start != 512<<10 {
		t.Errorf("leftover.Start = %d, want %d", leftover.Start, 512<<10)
	}

	if _, _, ok := ws.stealPlan(now); ok {
		t.Error("stealPlan must not claim a cancel fn twice")
	}
}
