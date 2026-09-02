package fetch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestResumeStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	outFile := filepath.Join(dir, "download.bin")
	url := "https://example.com/file"
	total := int64(104857600)

	d := &Downloader{
		url:          url,
		outFile:      outFile,
		totalSize:    total,
		expectedHash: "deadbeef",
		resumePath:   resumePath(outFile),
	}
	want := []Task{{0, 999999}, {2000000, 2999999}, {5000000, 6299999}}
	if err := d.saveResume(url, want, nil); err != nil {
		t.Fatalf("saveResume: %v", err)
	}
	// Verify the sidecar file exists at the canonical path
	if _, err := os.Stat(resumePath(outFile)); err != nil {
		t.Fatalf("sidecar file not created: %v", err)
	}
	st, err := loadResume(resumePath(outFile), url, total)
	if err != nil {
		t.Fatalf("loadResume: %v", err)
	}
	if st == nil {
		t.Fatal("expected state, got nil")
	}
	if len(st.Completed) != len(want) {
		t.Fatalf("Completed len = %d, want %d", len(st.Completed), len(want))
	}
	for i, w := range want {
		if st.Completed[i] != w {
			t.Errorf("Completed[%d] = %v, want %v", i, st.Completed[i], w)
		}
	}
	if st.URL != url || st.TotalSize != total {
		t.Errorf("URL/Size mismatch: %s/%d vs %s/%d", st.URL, st.TotalSize, url, total)
	}
}

func TestLoadResumeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.gofetch.resume")

	d := &Downloader{
		url:        "https://example.com/file",
		outFile:    "x",
		totalSize:  1000,
		resumePath: path,
	}
	if err := d.saveResume("https://example.com/file", []Task{{0, 9}}, nil); err != nil {
		t.Fatal(err)
	}
	// URL mismatch → nil state (different download, not an error)
	if st, err := loadResume(path, "https://other.com/file", 1000); err != nil || st != nil {
		t.Errorf("URL mismatch: st=%v err=%v, want both zero", st, err)
	}
	// Size mismatch → nil state
	if st, err := loadResume(path, "https://example.com/file", 9999); err != nil || st != nil {
		t.Errorf("size mismatch: st=%v err=%v, want both zero", st, err)
	}
	// Missing file → nil state
	if st, err := loadResume("/no/such/file", "https://example.com/file", 1000); err != nil || st != nil {
		t.Errorf("missing: st=%v err=%v, want both zero", st, err)
	}
}

func TestResumeClearAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.gofetch.resume")

	d := &Downloader{
		url:        "https://example.com/file",
		outFile:    "x",
		totalSize:  100,
		resumePath: path,
	}
	_ = d.saveResume("https://example.com/file", []Task{{0, 9}}, nil)

	if err := clearResume(path); err != nil {
		t.Fatalf("clearResume: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file should be deleted after clearResume")
	}

	// Clear on empty path is no-op
	if err := clearResume(""); err != nil {
		t.Fatalf("clearResume(''): %v", err)
	}

	// Corrupted JSON
	corrupt := filepath.Join(dir, "c.gofetch.resume")
	_ = os.WriteFile(corrupt, []byte("not json{{{"), 0o644)
	if _, err := loadResume(corrupt, "https://example.com/file", 100); err == nil {
		t.Error("corrupt JSON: expected error, got nil")
	}
}

func TestSaveResumeNoPath(t *testing.T) {
	// resumePath=="" → saveResume is a no-op
	d := &Downloader{resumePath: ""}
	if err := d.saveResume("", []Task{}, nil); err != nil {
		t.Errorf("empty resumePath should be no-op: %v", err)
	}
}

// TestInProgressResumeMarksWrittenNotRemaining is a regression test for the
// inverted in-progress restore bug: the already-written prefix must enter
// completed so uncompleted() seeds only the leftover span.
func TestInProgressResumeMarksWrittenNotRemaining(t *testing.T) {
	dir := t.TempDir()
	outFile := filepath.Join(dir, "partial.bin")
	resumeFile := resumePath(outFile)
	url := "https://example.com/file"
	const total int64 = 10 * 1024 * 1024

	// Simulate a crash mid-task: completed [0, 1MiB) and an in-progress
	// task [1MiB, 2MiB) with 400KiB already written.
	inProg := Task{Start: 1 << 20, End: (2 << 20) - 1}
	const done int64 = 400 << 10
	st := ResumeState{
		URL:            url,
		OutFile:        outFile,
		TotalSize:      total,
		Completed:      []Task{{Start: 0, End: (1 << 20) - 1}},
		InProgress:     &inProg,
		InProgressDone: done,
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resumeFile, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadResume(resumeFile, url, total)
	if err != nil || loaded == nil {
		t.Fatalf("loadResume: st=%v err=%v", loaded, err)
	}

	// Mirror the restore logic from Downloader.Download.
	completed := loaded.Completed
	if loaded.InProgress != nil && loaded.InProgressDone > 0 {
		writtenEnd := loaded.InProgress.Start + loaded.InProgressDone - 1
		if writtenEnd >= loaded.InProgress.Start && writtenEnd <= loaded.InProgress.End {
			completed = append(completed, Task{
				Start: loaded.InProgress.Start,
				End:   writtenEnd,
			})
		}
	}
	completed = dedupTasks(completed)

	// Written prefix must be complete.
	wantWrittenEnd := inProg.Start + done - 1
	foundWritten := false
	for _, c := range completed {
		if c.Start <= inProg.Start && c.End >= wantWrittenEnd {
			foundWritten = true
			break
		}
	}
	if !foundWritten {
		t.Fatalf("written span %d-%d not in completed: %v", inProg.Start, wantWrittenEnd, completed)
	}

	// Remaining span must NOT be marked complete.
	remainStart := inProg.Start + done
	for _, c := range completed {
		if c.Start <= remainStart && c.End >= inProg.End {
			t.Fatalf("remaining span %d-%d incorrectly covered by completed %v", remainStart, inProg.End, c)
		}
	}

	// Gaps must include the leftover of the in-progress task.
	gaps := uncompleted(Task{Start: 0, End: total - 1}, completed)
	foundRemain := false
	for _, g := range gaps {
		if g.Start <= remainStart && g.End >= inProg.End {
			foundRemain = true
			break
		}
	}
	if !foundRemain {
		t.Fatalf("expected remaining gap covering %d-%d, gaps=%v", remainStart, inProg.End, gaps)
	}
}

func TestDedupTasks(t *testing.T) {
	tests := []struct {
		name string
		in   []Task
		want []Task
	}{
		{
			name: "nil",
			in:   nil,
			want: nil,
		},
		{
			name: "single",
			in:   []Task{{0, 9}},
			want: []Task{{0, 9}},
		},
		{
			name: "disjoint",
			in:   []Task{{0, 9}, {20, 29}},
			want: []Task{{0, 9}, {20, 29}},
		},
		{
			name: "overlapping",
			in:   []Task{{0, 9}, {5, 15}},
			want: []Task{{0, 15}},
		},
		{
			name: "adjacent",
			in:   []Task{{0, 9}, {10, 19}},
			want: []Task{{0, 19}},
		},
		{
			name: "unsorted overlaps",
			in:   []Task{{20, 29}, {0, 9}, {10, 19}, {5, 15}},
			want: []Task{{0, 29}},
		},
		{
			name: "subset",
			in:   []Task{{0, 99}, {10, 20}, {50, 60}},
			want: []Task{{0, 99}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedupTasks(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (%v vs %v)", len(got), len(tt.want), got, tt.want)
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("[%d] = %v, want %v", i, got[i], w)
				}
			}
		})
	}
}

// TestDropCompletedOverlapping exercises the G2 surgical-trim helper:
// only the actual bad byte ranges are removed from `completed`, with
// the remaining good fragments kept. Because completed is stored merged
// (adjacent/overlapping ranges unioned to one Task), the helper must
// SUBTRACT bad ranges, not drop whole merged tasks.
func TestDropCompletedOverlapping(t *testing.T) {
	tests := []struct {
		name      string
		completed []Task
		bad       []ChunkHash
		want      []Task
	}{
		{"nil bad (merged unchanged)",
			[]Task{{0, 9}, {10, 19}}, nil,
			[]Task{{0, 19}}}, // dedupTasks unions the adjacent ranges
		{"drop the one bad chunk from a contiguous file",
			[]Task{{0, 9}, {10, 19}, {20, 29}},
			[]ChunkHash{{Start: 10, End: 19, Hash: "x"}},
			[]Task{{0, 9}, {20, 29}}},
		{"partial overlap keeps the good fragments on both sides",
			[]Task{{5, 25}},
			[]ChunkHash{{Start: 15, End: 19, Hash: "x"}},
			[]Task{{5, 14}, {20, 25}}},
		{"two bad chunks drop two spans from a contiguous file",
			[]Task{{0, 9}, {10, 19}, {20, 29}, {30, 39}},
			[]ChunkHash{{Start: 10, End: 19, Hash: "x"}, {Start: 30, End: 39, Hash: "y"}},
			[]Task{{0, 9}, {20, 29}}},
		{"disjoint completed task fully survives",
			[]Task{{50, 59}},
			[]ChunkHash{{Start: 0, End: 9, Hash: "x"}},
			[]Task{{50, 59}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Downloader{}
			d.seedCompleted(tt.completed)
			d.dropCompletedOverlapping(tt.bad)
			got := d.snapshotCompleted()
			if !tasksEqualUnordered(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// tasksEqualUnordered compares two []Task ignoring order (dropCompleted-
// Overlapping preserves order, but being order-tolerant makes the test
// robust to future dedupTasks subtle changes).
func tasksEqualUnordered(a, b []Task) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// TestFinalizeSurgicalTrimOnCorruptChunk is the G2 fix end-to-end-style
// regression: a completed download whose on-disk bytes for ONE manifest
// chunk were corrupted (e.g. a torn write) must, on finalize, trim the
// resume sidecar to delete only that chunk's byte range — leaving the
// surrounding good ranges marked completed so the next run re-fetches
// just the bad span. Before the fix finalize cleared the ENTIRE sidecar
// on any integrity failure, forcing a full re-download.
func TestFinalizeSurgicalTrimOnCorruptChunk(t *testing.T) {
	// 4 chunks of 10 bytes: 0-9, 10-19, 20-29, 30-39.
	payload := makePayload(40)
	chunkHash := func(s, e int64) string { return sha256Hex(payload[s : e+1]) }
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks: []ChunkHash{
			{0, 9, chunkHash(0, 9)},
			{10, 19, chunkHash(10, 19)},
			{20, 29, chunkHash(20, 29)},
			{30, 39, chunkHash(30, 39)},
		},
	}
	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")
	if err := os.WriteFile(outFile, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	url := "https://example.com/file"

	d := &Downloader{
		url:           url,
		outFile:       outFile,
		totalSize:     40,
		manifest:      m,
		resumePath:    resumePath(outFile),
		resumeEnabled: true,
		quiet:         true,
	}
	// The whole file is recorded as completed (the download finished).
	d.seedCompleted([]Task{{0, 39}})

	// Corrupt chunk 10-19 on disk (a torn write / bit rot).
	corrupt := append([]byte(nil), payload...)
	copy(corrupt[10:20], "ZZZZZZZZZZ")
	if err := os.WriteFile(outFile, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	// finalize must fail verification (corruption present) …
	if err := d.finalize(nil, nil); err == nil {
		t.Fatal("expected finalize to fail on corrupted chunk, got nil")
	}
	// … and must NOT clear the sidecar (manifest localizes corruption).
	if _, err := os.Stat(d.resumePath); err != nil {
		t.Fatalf("resume sidecar should survive manifest integrity failure: %v", err)
	}
	st, err := loadResume(d.resumePath, url, 40)
	if err != nil || st == nil {
		t.Fatalf("loadResume: st=%v err=%v", st, err)
	}
	// Sidecar's completed must EXCLUDE the corrupt chunk's span (10-19)
	// and retain the good spans (0-9, 20-39).
	for _, c := range st.Completed {
		if c.Start <= 19 && c.End >= 10 {
			t.Fatalf("corrupt span 10-19 still in completed: %+v", st.Completed)
		}
	}
	// And the good bytes (0-9 and 20-39) must still be claimed complete.
	covering := func(s, e int64) bool {
		for _, c := range st.Completed {
			if c.Start <= s && c.End >= e {
				return true
			}
		}
		return false
	}
	if !covering(0, 9) {
		t.Errorf("good span 0-9 dropped from completed: %+v", st.Completed)
	}
	if !covering(20, 39) {
		t.Errorf("good span 20-39 dropped from completed: %+v", st.Completed)
	}
}

// TestFinalizeClearsOnNoManifestHashFailure pins the no-manifest
// failure policy: without a per-chunk manifest to localize corruption,
// finalize clears the sidecar so the next run re-downloads everything
// (rather than skipping ranges that may be corrupt).
func TestFinalizeClearsOnNoManifestHashFailure(t *testing.T) {
	payload := makePayload(40)
	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")
	if err := os.WriteFile(outFile, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	url := "https://example.com/file"
	d := &Downloader{
		url:           url,
		outFile:       outFile,
		totalSize:     40,
		expectedHash:  "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef0", // wrong: forces verifyFileHash failure
		hashAlgo:      "sha256",
		resumePath:    resumePath(outFile),
		resumeEnabled: true,
		quiet:         true,
	}
	d.seedCompleted([]Task{{0, 39}})
	// Pre-create the sidecar so we can assert it gets cleared.
	if err := d.saveResume(url, d.snapshotCompleted(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(d.resumePath); err != nil {
		t.Fatal(err)
	}
	if err := d.finalize(nil, nil); err == nil {
		t.Fatal("expected finalize to fail on hash mismatch, got nil")
	}
	if _, err := os.Stat(d.resumePath); !os.IsNotExist(err) {
		t.Fatalf("resume sidecar must be cleared on no-manifest integrity failure; stat err=%v", err)
	}
}

func TestClearResumeRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "sidecar.gofetch.resume")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if err := clearResume(link); err == nil {
		t.Fatal("expected clearResume to refuse a symlink")
	}
	// Target must be untouched.
	if _, err := os.Stat(target); err != nil {
		t.Errorf("symlink target was removed: %v", err)
	}
}

func TestAtomicWriteFileFailures(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing parent dir", func(t *testing.T) {
		if err := atomicWriteFile(filepath.Join(dir, "nope", "x"), []byte("x")); err == nil {
			t.Error("expected error for missing parent dir")
		}
	})

	t.Run("non-writable dir", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("running as root; permissions are not enforced")
		}
		ro := filepath.Join(dir, "ro")
		if err := os.Mkdir(ro, 0o555); err != nil {
			t.Fatal(err)
		}
		if err := atomicWriteFile(filepath.Join(ro, "x"), []byte("x")); err == nil {
			t.Error("expected error writing into read-only dir")
		}
	})

	t.Run("happy path leaves no tmp", func(t *testing.T) {
		p := filepath.Join(dir, "ok")
		if err := atomicWriteFile(p, []byte("hello")); err != nil {
			t.Fatalf("atomicWriteFile: %v", err)
		}
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "hello" {
			t.Errorf("content = %q, want hello", got)
		}
		if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("stale .tmp left behind")
		}
	})
}

// TestResolveResumeCrossMirrorSpliceGuard pins the security guard that
// prevents silently splicing bytes from two different mirrors: switching
// to a mirror WITHOUT a per-chunk manifest must discard in-memory
// completed ranges (same-size mirrors may serve different bytes). With a
// manifest vouching for the bytes, reuse is allowed.
func TestResolveResumeCrossMirrorSpliceGuard(t *testing.T) {
	dir := t.TempDir()

	t.Run("discards without manifest", func(t *testing.T) {
		d := NewDownloader("http://primary.example/file.bin", filepath.Join(dir, "a.bin"), Options{})
		d.seedCompleted([]Task{{Start: 0, End: 999}})
		if n := len(d.snapshotCompleted()); n != 1 {
			t.Fatalf("precondition: %d completed ranges, want 1", n)
		}
		// Mirror failover, same size, no manifest on disk.
		completed := d.resolveResume("http://mirror.example/file.bin", 1000)
		if len(completed) != 0 {
			t.Fatalf("resolveResume reused %d ranges without a manifest: %v", len(completed), completed)
		}
		if n := len(d.snapshotCompleted()); n != 0 {
			t.Fatalf("accumulator not cleared: %d ranges", n)
		}
	})

	t.Run("reuses with manifest", func(t *testing.T) {
		d := NewDownloader("http://primary.example/file.bin", filepath.Join(dir, "b.bin"), Options{})
		d.manifest = &Manifest{Version: ManifestVersion, Algo: "sha256"}
		d.seedCompleted([]Task{{Start: 0, End: 999}})
		completed := d.resolveResume("http://mirror.example/file.bin", 1000)
		if len(completed) != 1 || completed[0].Start != 0 || completed[0].End != 999 {
			t.Fatalf("expected manifest-vouched reuse, got %v", completed)
		}
	})
}
