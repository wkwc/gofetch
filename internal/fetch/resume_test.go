package fetch

import (
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
		URL:          url,
		OutFile:      outFile,
		totalSize:    total,
		expectedHash: "deadbeef",
		resumePath:   resumePath(outFile),
	}
	want := []Task{{0, 999999}, {2000000, 2999999}, {5000000, 6299999}}
	if err := d.saveResume(want); err != nil {
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
		URL:        "https://example.com/file",
		OutFile:    "x",
		totalSize:  1000,
		resumePath: path,
	}
	if err := d.saveResume([]Task{{0, 9}}); err != nil {
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
		URL:        "https://example.com/file",
		OutFile:    "x",
		totalSize:  100,
		resumePath: path,
	}
	_ = d.saveResume([]Task{{0, 9}})

	clearResume(path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file should be deleted after clearResume")
	}

	// Clear on empty path is no-op
	clearResume("")

	// Corrupted JSON
	corrupt := filepath.Join(dir, "c.gofetch.resume")
	os.WriteFile(corrupt, []byte("not json{{{"), 0o644)
	if _, err := loadResume(corrupt, "https://example.com/file", 100); err == nil {
		t.Error("corrupt JSON: expected error, got nil")
	}
}

func TestSortByStart(t *testing.T) {
	got := []Task{{30, 39}, {0, 9}, {20, 29}, {10, 19}}
	out := sortByStart(got)
	for i := 0; i < len(out)-1; i++ {
		if out[i].Start > out[i+1].Start {
			t.Fatalf("not sorted: %v", out)
		}
	}
	// nil in stays nil out
	if out := sortByStart(nil); out != nil {
		t.Errorf("nil should stay nil, got %v", out)
	}
	// single element
	if out := sortByStart([]Task{{5, 10}}); len(out) != 1 {
		t.Errorf("single element: got %v", out)
	}
}

func TestSaveResumeNoPath(t *testing.T) {
	// resumePath=="" → saveResume is a no-op
	d := &Downloader{resumePath: ""}
	if err := d.saveResume([]Task{}); err != nil {
		t.Errorf("empty resumePath should be no-op: %v", err)
	}
}
