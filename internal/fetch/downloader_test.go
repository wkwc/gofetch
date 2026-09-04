package fetch

import (
	"net/url"
	"os"
	"path/filepath"
	"net/http/httptest"
	"net/http"
	"encoding/pem"
	"testing"
)

func TestAllocateFileWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	fw, err := allocateFileWriter(path, 1024*1024, false, false)
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
	if _, ok := fw.(mmapWriterBytes); !ok {
		t.Error("expected mmap writer for sized non-resume allocate")
	}
}

// Resume-enabled first attempt (no partial file yet) must still prefer mmap.
func TestAllocateFileWriterResumeFreshUsesMmap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fresh.bin")
	fw, err := allocateFileWriter(path, 1024*1024, true, false)
	if err != nil {
		t.Fatalf("allocateFileWriter resume fresh: %v", err)
	}
	defer func() { _ = fw.Close() }()
	wb, ok := fw.(mmapWriterBytes)
	if !ok || wb.Bytes() == nil {
		t.Fatal("expected mmap-backed writer for resume=true + missing file")
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
		if (err != nil) != !tt.ok {
			t.Errorf("parseUint(%q) error mismatch: got err=%v, want err=%v", tt.s, err, !tt.ok)
			continue
		}
		if tt.ok && got != tt.want {
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

// TestApplyProbeSizeMismatchWipesProgress pins the applyProbe invariant:
// a mirror that proves a DIFFERENT size than a prior mirror must discard
// accumulated progress (bytes cannot be reused), truncate the output, and
// clear the resume sidecar. Same-size probes keep progress.
func TestApplyProbeSizeMismatchWipesProgress(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	d := NewDownloader("http://example.com/file.bin", out, Options{})

	// Simulate partial progress from a failed mirror, with a durable
	// resume sidecar on disk.
	d.totalSize = 1000
	if err := os.WriteFile(out, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	d.seedCompleted([]Task{{Start: 0, End: 999}})
	if err := d.saveResume(d.url, d.snapshotCompleted(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(d.resumePath); err != nil {
		t.Fatalf("resume file missing: %v", err)
	}

	// Same size → progress kept, nothing wiped.
	d.applyProbe(probeInfo{total: 1000, supportsRanges: true})
	if n := len(d.snapshotCompleted()); n != 1 {
		t.Fatalf("same-size probe wiped progress: %d ranges", n)
	}

	// Different size → progress discarded, file truncated, sidecar cleared.
	d.applyProbe(probeInfo{total: 2000, supportsRanges: true})
	if n := len(d.snapshotCompleted()); n != 0 {
		t.Fatalf("size change kept %d completed ranges", n)
	}
	if info, err := os.Stat(out); err != nil || info.Size() != 0 {
		t.Errorf("output not truncated to 0: size=%v err=%v", info, err)
	}
	if _, err := os.Stat(d.resumePath); !os.IsNotExist(err) {
		t.Errorf("resume sidecar not cleared: %v", err)
	}
}

// TestRetryMaxOverride verifies the --max-retries escape hatch reaches the
// retry budget used by both the requeue path and the HTTP-status loop.
func TestRetryMaxOverride(t *testing.T) {
	d := NewDownloader("http://example.com/file.bin", "out.bin", Options{RetryMax: 3})
	if d.autoConfig.RetryMax != 3 {
		t.Errorf("RetryMax = %d, want 3", d.autoConfig.RetryMax)
	}
	// Default (0) keeps the auto-tuned 10.
	d2 := NewDownloader("http://example.com/file.bin", "out.bin", Options{})
	if d2.autoConfig.RetryMax != 10 {
		t.Errorf("default RetryMax = %d, want 10", d2.autoConfig.RetryMax)
	}
}

func TestNewTransport(t *testing.T) {
	tr := NewTransport(Options{Workers: 4, BufSize: 64 << 10})
	defer tr.CloseIdleConnections()
	if tr.MaxIdleConnsPerHost != 4 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 4", tr.MaxIdleConnsPerHost)
	}
	if tr.DialContext == nil {
		t.Error("expected an SSRF-hardened DialContext")
	}
	// Workers=0 -> auto-derived (>= 4).
	tr2 := NewTransport(Options{})
	defer tr2.CloseIdleConnections()
	if tr2.MaxIdleConnsPerHost < 4 {
		t.Errorf("auto MaxIdleConnsPerHost = %d, want >= 4", tr2.MaxIdleConnsPerHost)
	}
}

func TestValidateCACert(t *testing.T) {
	dir := t.TempDir()
	// A real (self-signed) certificate validates.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(srv.Close)
	valid := filepath.Join(dir, "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(valid, pemBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCACert(valid); err != nil {
		t.Errorf("ValidateCACert(valid) = %v, want nil", err)
	}

	// Missing and non-certificate files are rejected.
	if err := ValidateCACert(filepath.Join(dir, "nope.pem")); err == nil {
		t.Error("expected error for missing file")
	}
	garbage := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a cert\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCACert(garbage); err == nil {
		t.Error("expected error for a file with no certificates")
	}
}
