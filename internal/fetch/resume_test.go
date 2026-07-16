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
		url:          url,
		outFile:      outFile,
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
		url:        "https://example.com/file",
		outFile:    "x",
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
		url:        "https://example.com/file",
		outFile:    "x",
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
	sortByStart(got)
	for i := 0; i < len(got)-1; i++ {
		if got[i].Start > got[i+1].Start {
			t.Fatalf("not sorted: %v", got)
		}
	}
	// nil in stays nil out (no-op, no crash)
	sortByStart(nil)
	// single element (no-op, no crash)
	sortByStart([]Task{{5, 10}})
}

func TestSaveResumeNoPath(t *testing.T) {
	// resumePath=="" → saveResume is a no-op
	d := &Downloader{resumePath: ""}
	if err := d.saveResume([]Task{}); err != nil {
		t.Errorf("empty resumePath should be no-op: %v", err)
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
