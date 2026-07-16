package fetch

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAllocateSparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	f, err := allocateSparse(path, 1024*1024, false)
	if err != nil {
		t.Fatalf("allocateSparse: %v", err)
	}
	defer f.Close()

	if info, err := os.Stat(path); err != nil {
		t.Fatalf("Stat: %v", err)
	} else if info.Size() != 1024*1024 {
		t.Errorf("file size = %d, want %d", info.Size(), 1024*1024)
	}

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("WriteAt 0: %v", err)
	}
	if _, err := f.WriteAt([]byte("world"), 512*1024); err != nil {
		t.Fatalf("WriteAt mid: %v", err)
	}
}

func TestAllocateSparseZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")

	f, err := allocateSparse(path, 0, false)
	if err != nil {
		t.Fatalf("allocateSparse(0): %v", err)
	}
	defer f.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("file size = %d, want 0", info.Size())
	}
}

func TestResumePath(t *testing.T) {
	if got := resumePath("download.bin"); got != "download.bin.gofetch.resume" {
		t.Errorf("got %q", got)
	}
}

func TestMainEntryURLParse(t *testing.T) {
	// Mirror the URL validation done by main.go run().
	cases := []struct {
		raw  string
		want bool
	}{
		{"https://example.com/file", true},
		{"http://example.com:8080/file.bin", true},
		{"not-a-url", false},
		{"", false},
	}
	for _, tt := range cases {
		u, e := url.Parse(tt.raw)
		ok := e == nil && u.Scheme != "" && u.Host != ""
		if ok != tt.want {
			t.Errorf("url.Parse(%q) → ok=%v, want %v", tt.raw, ok, tt.want)
		}
	}
}

func TestMirrorListParsing(t *testing.T) {
	// The mirror parser logic in main.go constructs a list:
	raw := "https://example.com/file"
	mirrors := "https://a/file,https://b/file,https://c/file"
	got := []string{raw}
	for _, m := range strings.Split(mirrors, ",") {
		m = strings.TrimSpace(m)
		if m != "" && m != raw {
			got = append(got, m)
		}
	}
	want := []string{raw, "https://a/file", "https://b/file", "https://c/file"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestParseUint(t *testing.T) {
	tests := []struct {
		s    string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"42", 42, true},
		{"999999999", 999999999, true},
		{"9223372036854775807", 9223372036854775807, true}, // MaxInt64
		{"9223372036854775808", 0, false},                   // MaxInt64+1 → overflow
		{"99999999999999999999", 0, false},                  // way too big
		{"", 0, false},
		{"abc", 0, false},
		{"12abc", 0, false},
	}
	for _, tt := range tests {
		got, err := parseUint(tt.s)
		if tt.ok && err != nil {
			t.Errorf("parseUint(%q) error: %v", tt.s, err)
		} else if !tt.ok && err == nil {
			t.Errorf("parseUint(%q) = %d, want error", tt.s, got)
		} else if tt.ok && got != tt.want {
			t.Errorf("parseUint(%q) = %d, want %d", tt.s, got, tt.want)
		}
	}
}

func TestParseContentRangeOverflow(t *testing.T) {
	// Server sends a total that overflows int64.
	_, _, _, ok := parseContentRange("bytes 0-0/99999999999999999999")
	if ok {
		t.Error("expected parse failure for overflowing Content-Range total")
	}
}
