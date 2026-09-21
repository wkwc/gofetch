package fetch

import (
	"os"
	"path/filepath"
	"testing"
)

// newResumeTestDownloader builds a Downloader wired to a temp output file
// with resume enabled, for exercising resolveResume in isolation. The
// output file is created at 1000 bytes (the total every test here uses):
// real resume flows always have the partial file on disk, and
// resolveResume's stale-sidecar guard requires it.
func newResumeTestDownloader(t *testing.T) *Downloader {
	t.Helper()
	out := filepath.Join(t.TempDir(), "f.bin")
	d := NewDownloader("https://primary.example/f.bin", out, Options{})
	if err := os.WriteFile(out, make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

// TestResolveResumeDisabled verifies --no-resume never seeds ranges.
func TestResolveResumeDisabled(t *testing.T) {
	d := newResumeTestDownloader(t)
	d.resumeEnabled = false
	d.resumePath = ""
	d.recordCompleted(Task{Start: 0, End: 99})
	if got := d.resolveResume("https://primary.example/f.bin", 1000); got != nil {
		t.Errorf("resolveResume with --no-resume = %v, want nil", got)
	}
}

// TestResolveResumeSidecarRecovery covers the happy path: a valid on-disk
// sidecar restores completed ranges, promotes in-progress bytes, and
// inherits the persisted hash algo+value.
func TestResolveResumeSidecarRecovery(t *testing.T) {
	d := newResumeTestDownloader(t)

	ws := newWorkerState()
	ws.reset(Task{Start: 100, End: 199})
	ws.bytesDone.Store(50)

	d.totalSize = 1000
	d.hashAlgo = ""
	d.expectedHash = ""
	if err := d.saveResume(d.url, []Task{{Start: 0, End: 99}}, []*workerState{ws}); err != nil {
		t.Fatalf("saveResume: %v", err)
	}
	// Simulate a fresh process: the caller no longer knows the algo.
	if _, err := os.Stat(d.resumePath); err != nil {
		t.Fatalf("sidecar missing: %v", err)
	}

	completed := d.resolveResume(d.url, 1000)
	// The promoted in-progress span {100,149} is adjacent to {0,99}, so
	// dedupTasks merges them into one range.
	want := []Task{{Start: 0, End: 149}}
	if len(completed) != len(want) {
		t.Fatalf("completed = %v, want %v", completed, want)
	}
	for i, tk := range want {
		if completed[i] != tk {
			t.Errorf("completed[%d] = %v, want %v", i, completed[i], tk)
		}
	}
}

// TestResolveResumeInheritsHash verifies a sidecar written with an
// explicit -h value restores the algorithm across a process restart, so
// a sha512 download never verifies with the wrong (or no) algorithm.
func TestResolveResumeInheritsHash(t *testing.T) {
	d := newResumeTestDownloader(t)

	d.totalSize = 1000
	d.hashAlgo = "sha512"
	d.expectedHash = "feedbeef"
	if err := d.saveResume(d.url, []Task{{Start: 0, End: 999}}, nil); err != nil {
		t.Fatalf("saveResume: %v", err)
	}

	d.hashAlgo = ""
	d.expectedHash = ""
	d.resolveResume(d.url, 1000)

	if d.hashAlgo != "sha512" || d.expectedHash != "feedbeef" {
		t.Errorf("hash inheritance failed: algo=%q hash=%q", d.hashAlgo, d.expectedHash)
	}
}

// TestResolveResumeCrossMirrorSplicing verifies the anti-splicing gate:
// a mirror switch without a manifest must discard in-memory ranges from
// the prior mirror (two same-size mirrors may serve different bytes),
// while a manifest vouching for the bytes permits reuse.
func TestResolveResumeCrossMirrorSplicing(t *testing.T) {
	d := newResumeTestDownloader(t)
	d.totalSize = 1000
	d.recordCompleted(Task{Start: 0, End: 99})
	d.seedCompleted(d.snapshotCompleted())

	// Without a manifest: discard.
	d.manifest = nil
	if got := d.resolveResume("https://mirror.example/f.bin", 1000); got != nil {
		t.Errorf("unmanifested mirror switch should discard ranges, got %v", got)
	}
	if n := len(d.snapshotCompleted()); n != 0 {
		t.Errorf("in-memory ranges not discarded: %d remain", n)
	}

	// With a manifest: reuse.
	d.recordCompleted(Task{Start: 0, End: 99})
	d.seedCompleted(d.snapshotCompleted())
	d.manifest = &Manifest{}
	got := d.resolveResume("https://mirror.example/f.bin", 1000)
	if len(got) != 1 || got[0] != (Task{Start: 0, End: 99}) {
		t.Errorf("manifest-vouched reuse failed: got %v", got)
	}
}

// TestResolveResumeCorruptSidecar verifies a corrupt on-disk sidecar is
// cleared (not retried forever) while in-memory progress from a
// same-size failover survives.
func TestResolveResumeCorruptSidecar(t *testing.T) {
	d := newResumeTestDownloader(t)
	d.totalSize = 1000
	d.recordCompleted(Task{Start: 0, End: 99})
	d.seedCompleted(d.snapshotCompleted())
	if err := os.WriteFile(d.resumePath, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write corrupt sidecar: %v", err)
	}

	completed := d.resolveResume(d.url, 1000)
	if len(completed) != 1 || completed[0] != (Task{Start: 0, End: 99}) {
		t.Errorf("corrupt sidecar should fall back to in-memory progress, got %v", completed)
	}
	if _, err := os.Stat(d.resumePath); !os.IsNotExist(err) {
		t.Errorf("corrupt sidecar not cleared: stat err=%v", err)
	}
}
