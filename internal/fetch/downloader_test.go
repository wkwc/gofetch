package fetch

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestAllocateFileWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	fw, err := allocateFileWriter(path, 1024*1024, false)
	if err != nil {
		t.Fatalf("allocateFileWriter: %v", err)
	}
	defer func() { _ = fw.Close() }()

	if info, err := os.Stat(path); err != nil {
		t.Fatalf("Stat: %v", err)
	} else if info.Size() != 1024*1024 {
		t.Errorf("file size = %d, want %d", info.Size(), 1024*1024)
	}

	if _, err := fw.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("WriteAt 0: %v", err)
	}
	if _, err := fw.WriteAt([]byte("world"), 512*1024); err != nil {
		t.Fatalf("WriteAt mid: %v", err)
	}
}

func TestAllocateRawZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")

	fw, err := allocateRawFile(path, 0, false)
	if err != nil {
		t.Fatalf("allocateRawFile(0): %v", err)
	}
	defer func() { _ = fw.Close() }()

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

func TestURLValidation(t *testing.T) {
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
		{"9223372036854775807", 9223372036854775807, true},
		{"9223372036854775808", 0, false},
		{"99999999999999999999", 0, false},
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
	_, _, _, ok := parseContentRange("bytes 0-0/99999999999999999999")
	if ok {
		t.Error("expected parse failure for overflowing Content-Range total")
	}
}
