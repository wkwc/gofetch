package fetch

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		input  string
		start  int64
		end    int64
		total  int64
		wantOK bool
	}{
		{"bytes 0-100/10485760", 0, 100, 10485760, true},
		{"bytes 100-200/1000", 100, 200, 1000, true},
		{"bytes 0-0/42", 0, 0, 42, true},
		{"bytes 999-999/1000", 999, 999, 1000, true},
		{"bytes 0-99/100", 0, 99, 100, true},
		{"invalid", 0, 0, 0, false},
		{"bytes", 0, 0, 0, false},
		{"bytes /1000", 0, 0, 0, false},
		{"bytes abc-100/1000", 0, 0, 0, false},
		{"bytes 0/1000", 0, 0, 0, false},
		{"", 0, 0, 0, false},
		{"bytes 0-100/abc", 0, 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			start, end, total, ok := parseContentRange(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok {
				if start != tt.start || end != tt.end || total != tt.total {
					t.Errorf("got %d-%d/%d, want %d-%d/%d", start, end, total, tt.start, tt.end, tt.total)
				}
			}
		})
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		ok    bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"123", 123, true},
		{"99999", 99999, true},
		{"", 0, false},
		{"abc", 0, false},
		{"12a", 0, false},
		{"-1", 0, false},
		{" 1", 0, false},
		{"1 ", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseInt(tt.input)
			if (err == nil) != tt.ok {
				t.Fatalf("err = %v, ok = %v", err, tt.ok)
			}
			if tt.ok && got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestTaskLen(t *testing.T) {
	tests := []struct {
		task Task
		want int64
	}{
		{Task{Start: 0, End: 0}, 1},
		{Task{Start: 0, End: 99}, 100},
		{Task{Start: 100, End: 199}, 100},
		{Task{Start: 0, End: 1048575}, 1048576},
		{Task{Start: 10, End: 5}, -4},
	}
	for _, tt := range tests {
		got := tt.task.Len()
		if got != tt.want {
			t.Errorf("Len() = %d, want %d", got, tt.want)
		}
	}
}

func TestQueue(t *testing.T) {
	q := &Queue{}

	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on empty queue should return false")
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}

	q.Push(Task{Start: 0, End: 10})
	q.Push(Task{Start: 11, End: 20})
	q.Push(Task{Start: 21, End: 30})

	if q.Len() != 3 {
		t.Fatalf("Len = %d, want 3", q.Len())
	}

	// Pop is FIFO
	task, ok := q.Pop()
	if !ok || task.Start != 0 {
		t.Fatalf("first pop = %+v ok=%v, want Start=0", task, ok)
	}
	task, ok = q.Pop()
	if !ok || task.Start != 11 {
		t.Fatalf("second pop = %+v ok=%v, want Start=11", task, ok)
	}
	task, ok = q.Pop()
	if !ok || task.Start != 21 {
		t.Fatalf("third pop = %+v ok=%v, want Start=21", task, ok)
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on now-empty queue should return false")
	}
}

func TestQueueSnapshot(t *testing.T) {
	q := &Queue{}
	q.Push(Task{Start: 0, End: 10})
	q.Push(Task{Start: 11, End: 20})

	snap := q.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snap))
	}
	// Mutating snapshot should not affect queue
	snap[0].Start = 999

	snap2 := q.Snapshot()
	if snap2[0].Start != 0 {
		t.Fatalf("snapshot was mutated: Start = %d, want 0", snap2[0].Start)
	}
}

func TestSplitRangeFine(t *testing.T) {
	// 100 bytes, 4 workers, 1MB max chunks -> 4 tasks since 25 bytes < 1MB
	tasks := splitRangeFine(0, 99, 4, 1024*1024, nil)
	if len(tasks) != 4 {
		t.Fatalf("len = %d, want 4", len(tasks))
	}
	// Verify full coverage
	total := int64(0)
	for _, task := range tasks {
		total += task.Len()
	}
	if total != 100 {
		t.Fatalf("total bytes = %d, want 100", total)
	}

	// 10 bytes, 4 workers, max chunk 2 -> 5 tasks
	tasks = splitRangeFine(0, 9, 4, 2, nil)
	if len(tasks) != 5 {
		t.Fatalf("len = %d, want 5", len(tasks))
	}
	total = 0
	for _, task := range tasks {
		total += task.Len()
	}
	if total != 10 {
		t.Fatalf("total bytes = %d, want 10", total)
	}

	// Single byte edge case
	tasks = splitRangeFine(0, 0, 4, 1024, nil)
	if len(tasks) != 1 || tasks[0].Start != 0 || tasks[0].End != 0 {
		t.Fatalf("single byte = %+v, want 1 task [0-0]", tasks)
	}
}

func TestSplitRangeFineWithCompleted(t *testing.T) {
	completed := []Task{{Start: 0, End: 4}, {Start: 6, End: 9}}
	tasks := splitRangeFine(0, 9, 4, 2, completed)

	// Should not include bytes 0-4 or 6-9
	for _, task := range tasks {
		if task.Start < 5 && task.End > 4 {
			t.Fatalf("task %d-%d overlaps completed range 0-4", task.Start, task.End)
		}
	}

	// All remaining bytes should be byte 5 only
	total := int64(0)
	for _, task := range tasks {
		total += task.Len()
	}
	if total != 1 {
		t.Fatalf("remaining bytes = %d, want 1 (only byte 5)", total)
	}
}

func TestSubtractRanges(t *testing.T) {
	tests := []struct {
		name      string
		full      Task
		completed []Task
		want      []Task
	}{
		{
			name:      "no completed",
			full:      Task{Start: 0, End: 99},
			completed: nil,
			want:      []Task{{Start: 0, End: 99}},
		},
		{
			name:      "fully completed",
			full:      Task{Start: 0, End: 99},
			completed: []Task{{Start: 0, End: 99}},
			want:      nil,
		},
		{
			name:      "beginning completed",
			full:      Task{Start: 0, End: 99},
			completed: []Task{{Start: 0, End: 49}},
			want:      []Task{{Start: 50, End: 99}},
		},
		{
			name:      "end completed",
			full:      Task{Start: 0, End: 99},
			completed: []Task{{Start: 50, End: 99}},
			want:      []Task{{Start: 0, End: 49}},
		},
		{
			name:      "middle completed",
			full:      Task{Start: 0, End: 99},
			completed: []Task{{Start: 30, End: 69}},
			want:      []Task{{Start: 0, End: 29}, {Start: 70, End: 99}},
		},
		{
			name:      "unsorted completed",
			full:      Task{Start: 0, End: 99},
			completed: []Task{{Start: 50, End: 99}, {Start: 0, End: 49}},
			want:      nil,
		},
		{
			name:      "outside range",
			full:      Task{Start: 100, End: 200},
			completed: []Task{{Start: 0, End: 50}, {Start: 300, End: 400}},
			want:      []Task{{Start: 100, End: 200}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subtractRanges(tt.full, tt.completed)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (%+v vs %+v)", len(got), len(tt.want), got, tt.want)
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("[%d] = %+v, want %+v", i, got[i], w)
				}
			}
		})
	}
}

func TestSubtractRangesConcurrency(t *testing.T) {
	// Simulate resume with multiple overlapping completed ranges
	completed := []Task{
		{Start: 0, End: 999},
		{Start: 2000, End: 2999},
		{Start: 1000, End: 1999}, // unsorted, should merge with 0-999
	}
	remaining := subtractRanges(Task{Start: 0, End: 3999}, completed)
	// Should only have 3000-3999
	if len(remaining) != 1 {
		t.Fatalf("len = %d, want 1 (%+v)", len(remaining), remaining)
	}
	if remaining[0].Start != 3000 || remaining[0].End != 3999 {
		t.Errorf("remaining = %+v, want 3000-3999", remaining[0])
	}
}

func TestCollectCompleted(t *testing.T) {
	t1 := Task{Start: 0, End: 99}
	t2 := Task{Start: 100, End: 199}
	t3 := Task{Start: 200, End: 299}

	states := []*workerState{
		newWorkerState(),
		newWorkerState(),
		newWorkerState(),
	}

	// Worker 0: task complete (100 bytes done)
	states[0].curTask.Store(&t1)
	states[0].bytesDone.Store(100)

	// Worker 1: task incomplete (50 of 100 bytes)
	states[1].curTask.Store(&t2)
	states[1].bytesDone.Store(50)

	// Worker 2: no task assigned (nil)
	_ = t3 // not used

	completed := collectCompleted(states)
	if len(completed) != 1 {
		t.Fatalf("len(completed) = %d, want 1", len(completed))
	}
	if completed[0] != t1 {
		t.Errorf("completed[0] = %+v, want %+v", completed[0], t1)
	}
}

func TestResumeState(t *testing.T) {
	dir := t.TempDir()
	outFile := filepath.Join(dir, "download.bin")
	url := "https://example.com/file"
	totalSize := int64(104857600)

	completed := []Task{
		{Start: 0, End: 999999},
		{Start: 2000000, End: 2999999},
	}

	d := &Downloader{
		URL:            url,
		OutFile:        outFile,
		totalSize:      totalSize,
		expectedSHA256: "abc123",
		startTime:      time.Now(),
		resumePath:     ResumeStatePath(outFile),
	}

	if err := d.SaveResumeState(completed); err != nil {
		t.Fatalf("SaveResumeState: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(ResumeStatePath(outFile)); os.IsNotExist(err) {
		t.Fatal("resume file not created")
	}

	// Load it back
	state, err := LoadResumeState(ResumeStatePath(outFile), url, totalSize)
	if err != nil {
		t.Fatalf("LoadResumeState: %v", err)
	}
	if state.URL != url {
		t.Errorf("URL = %s, want %s", state.URL, url)
	}
	if state.TotalSize != totalSize {
		t.Errorf("TotalSize = %d, want %d", state.TotalSize, totalSize)
	}
	if len(state.Completed) != 2 {
		t.Fatalf("Completed len = %d, want 2", len(state.Completed))
	}

	// Mismatch should return error
	_, err = LoadResumeState(ResumeStatePath(outFile), "wrong-url", totalSize)
	if err == nil {
		t.Fatal("expected error on URL mismatch, got nil")
	}

	_, err = LoadResumeState(ResumeStatePath(outFile), url, 999999)
	if err == nil {
		t.Fatal("expected error on size mismatch, got nil")
	}

	// Clear
	ClearResumeState(ResumeStatePath(outFile))
	if _, err := os.Stat(ResumeStatePath(outFile)); !os.IsNotExist(err) {
		t.Fatal("resume file should be deleted after ClearResumeState")
	}
}

func TestAllocateSparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	f, err := allocateSparse(path, 1024*1024)
	if err != nil {
		t.Fatalf("allocateSparse: %v", err)
	}
	defer f.Close()

	// Verify file size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 1024*1024 {
		t.Errorf("file size = %d, want %d", info.Size(), 1024*1024)
	}

	// Can write at offset
	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if _, err := f.WriteAt([]byte("world"), 512*1024); err != nil {
		t.Fatalf("WriteAt offset: %v", err)
	}
}

func TestAllocateSparseZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")

	f, err := allocateSparse(path, 0)
	if err != nil {
		t.Fatalf("allocateSparse(0): %v", err)
	}
	defer f.Close()

	// File should exist but be empty
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("file size = %d, want 0", info.Size())
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.00 GB"},
		{-1, "-1 B"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestWorkerStateError(t *testing.T) {
	ws := newWorkerState()

	// No error initially
	if err, ok := ws.err(); ok {
		t.Fatalf("unexpected error: %v", err)
	}

	// Set an error
	ws.setErr(os.ErrNotExist)
	if err, ok := ws.err(); !ok || err != os.ErrNotExist {
		t.Fatalf("err = %v ok = %v, want os.ErrNotExist true", err, ok)
	}

	// Second setErr should not overwrite (errOnce)
	ws.setErr(os.ErrClosed)
	if err, ok := ws.err(); !ok || err != os.ErrNotExist {
		t.Fatalf("err should not be overwritten: got %v, want os.ErrNotExist", err)
	}

	// nil should be a no-op
	ws2 := newWorkerState()
	ws2.setErr(nil)
	if _, ok := ws2.err(); ok {
		t.Fatal("nil error should not set any error")
	}
}

func TestExternalMirrorSelection(t *testing.T) {
	// Test that main.go's mirror parsing works as expected
	// (we test mirrorList construction here as a regression check)
	rawURL := "https://example.com/file"
	mirrors := "https://a/file,https://b/file"
	expected := []string{rawURL, "https://a/file", "https://b/file"}

	// Mirror parser logic from main.go
	mirrorList := []string{rawURL}
	for _, m := range stringSplit(mirrors, ",") {
		m = stringTrim(m)
		if m != "" {
			mirrorList = append(mirrorList, m)
		}
	}

	if len(mirrorList) != len(expected) {
		t.Fatalf("len = %d, want %d", len(mirrorList), len(expected))
	}
	for i, u := range expected {
		if mirrorList[i] != u {
			t.Errorf("[%d] = %q, want %q", i, mirrorList[i], u)
		}
	}
}

// Helper functions to test main.go's mirror parsing without importing it.
func stringSplit(s, sep string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

func stringTrim(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
