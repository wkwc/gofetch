package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wkwc/gofetch/internal/fetch"
)

// captureStdout runs fn while capturing everything printed to stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	fn()
	_ = w.Close()
	out, _ := io.ReadAll(r)
	_ = r.Close()
	return string(out)
}

func TestAutoDetectLocalSidecar(t *testing.T) {
	dir := t.TempDir()

	t.Run("no sidecar", func(t *testing.T) {
		out := filepath.Join(dir, "none.bin")
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if algo != "" || hex != "" {
			t.Errorf("algo=%q hex=%q, want empty", algo, hex)
		}
	})

	t.Run("sha256 sidecar", func(t *testing.T) {
		out := filepath.Join(dir, "data.bin")
		hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
		if err := os.WriteFile(out+".sha256", []byte(hash+"  data.bin\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha256" || hex != hash {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, hash)
		}
	})

	t.Run("sha512sum variant", func(t *testing.T) {
		out := filepath.Join(dir, "big.bin")
		hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		if err := os.WriteFile(out+".sha512sum", []byte(hash+"  big.bin\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha512" || hex != hash {
			t.Errorf("algo=%q hex=%q, want sha512/%q", algo, hex, hash)
		}
	})

	t.Run("default flag auto-detects local sidecar", func(t *testing.T) {
		// "-h" defaults to "" and must pick up the sidecar next to the
		// output with zero configuration.
		out := filepath.Join(dir, "data.bin") // sha256 sidecar created above
		algo, hex, err := resolveHash(t.Context(), "", "https://example.com/data.bin", out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		want := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
		if algo != "sha256" || hex != want {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, want)
		}
	})

	t.Run("explicit flag overrides auto", func(t *testing.T) {
		// resolveHash with an explicit -h value must not touch the sidecar.
		ctx := t.Context()
		explicit := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		algo, hex, err := resolveHash(ctx, "sha256:"+explicit, "https://example.com/x.bin", filepath.Join(dir, "data.bin"))
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha256" || hex != explicit {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, explicit)
		}
	})
}

func TestNormalizeMirrors(t *testing.T) {
	// run() tests enable fetch.AllowLoopbackDial via --allow-loopback;
	// the SSRF-rejection cases below need it disabled. Save/restore so
	// shuffled test order cannot leak state between tests.
	prev := fetch.AllowLoopbackDial
	fetch.AllowLoopbackDial = false
	t.Cleanup(func() { fetch.AllowLoopbackDial = prev })

	t.Run("empty", func(t *testing.T) {
		got, err := normalizeMirrors(t.Context(), "")
		if err != nil || got != nil {
			t.Fatalf("empty: got=%v err=%v, want nil/nil", got, err)
		}
	})

	t.Run("bare hostnames get https", func(t *testing.T) {
		got, err := normalizeMirrors(t.Context(), "example.com, www.example.com")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		want := []string{"https://example.com", "https://www.example.com"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("mirror[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("explicit scheme preserved", func(t *testing.T) {
		got, err := normalizeMirrors(t.Context(), "http://example.com")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 1 || got[0] != "http://example.com" {
			t.Errorf("got %v, want [http://example.com]", got)
		}
	})

	t.Run("private mirror rejected", func(t *testing.T) {
		_, err := normalizeMirrors(t.Context(), "127.0.0.1")
		if err == nil {
			t.Fatal("expected SSRF rejection of loopback mirror")
		}
	})
}

// newTestServer serves a fixed payload with HEAD + Range support, so the
// full probe → range-download path is exercised in CLI tests.
func newTestServer(t *testing.T, payload []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if rh := r.Header.Get("Range"); rh != "" {
			var start, end int64
			if _, err := fmt.Sscanf(rh, "bytes=%d-%d", &start, &end); err == nil {
				if end >= int64(len(payload)) || end < 0 {
					end = int64(len(payload)) - 1
				}
				if start >= 0 && start <= end {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
					w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(payload[start : end+1])
					return
				}
			}
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRunDownloadSingle(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 64*1024) // 640 KiB → range mode
	srv := newTestServer(t, payload)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	if code := run([]string{"--allow-loopback", "-q", "-o", out, srv.URL}); code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestRunDownloadIntoDir(t *testing.T) {
	payload := []byte("hello into dir")
	srv := newTestServer(t, payload)
	dir := t.TempDir()
	// -o pointing at an existing directory downloads into it by basename.
	if code := run([]string{"--allow-loopback", "-q", "-o", dir, srv.URL + "/file.bin"}); code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	got, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("content = %q, want %q", got, payload)
	}
}

func TestRunMultiURL(t *testing.T) {
	payloadA := bytes.Repeat([]byte("A"), 256*1024)
	payloadB := bytes.Repeat([]byte("B"), 512*1024)
	srv := newTestServer(t, payloadA)
	srvB := newTestServer(t, payloadB)
	dir := filepath.Join(t.TempDir(), "dl") // does not exist yet → created
	code := run([]string{
		"--allow-loopback", "-q", "-o", dir,
		srv.URL + "/a.bin", srvB.URL + "/b.bin",
	})
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	for name, want := range map[string][]byte{"a.bin": payloadA, "b.bin": payloadB} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s mismatch: got %d bytes, want %d", name, len(got), len(want))
		}
	}
}

func TestRunMultiURLRejectsFileOut(t *testing.T) {
	srv := newTestServer(t, []byte("x"))
	fileOut := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(fileOut, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"--allow-loopback", "-q", "-o", fileOut, srv.URL + "/a.bin", srv.URL + "/b.bin"}); code != 1 {
		t.Fatalf("run = %d, want 1", code)
	}
}

func TestRunHeaders(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Test"), r.Header.Get("User-Agent"))
		mu.Unlock()
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
	}))
	t.Cleanup(srv.Close)
	out := filepath.Join(t.TempDir(), "h.bin")
	code := run([]string{
		"--allow-loopback", "-q", "-A", "my-agent/9", "-H", "X-Test: hello",
		"-o", out, srv.URL,
	})
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("server saw no requests")
	}
	for i, h := range seen {
		if i%2 == 0 && h != "hello" {
			t.Errorf("request %d X-Test = %q, want hello", i/2, h)
		}
		if i%2 == 1 && h != "my-agent/9" {
			t.Errorf("request %d User-Agent = %q, want my-agent/9", i/2, h)
		}
	}
}

func TestRunHeaderCannotOverrideIdentity(t *testing.T) {
	var enc string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusOK)
			return
		}
		enc = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("abc"))
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "e.bin")
	code := run([]string{"--allow-loopback", "-q", "-H", "Accept-Encoding: gzip", "-o", out, srv.URL})
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	if enc != "identity" {
		t.Errorf("Accept-Encoding = %q, want identity (user header must not override it)", enc)
	}
}

func TestRunVersion(t *testing.T) {
	if code := run([]string{"-version"}); code != 0 {
		t.Errorf("run(-version) = %d, want 0", code)
	}
}

func TestRunHelp(t *testing.T) {
	if code := run([]string{"-help"}); code != 0 {
		t.Errorf("run(-help) = %d, want 0", code)
	}
}

func TestRunBadFlag(t *testing.T) {
	if code := run([]string{"-nope"}); code != 2 {
		t.Errorf("run(-nope) = %d, want 2", code)
	}
}

func TestRunNoURL(t *testing.T) {
	if code := run(nil); code != 2 {
		t.Errorf("run() = %d, want 2", code)
	}
}

func TestParseRateLimit(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1024", 1024, false},
		{"500k", 500 << 10, false},
		{"2M", 2 << 20, false},
		{"10m", 10 << 20, false},
		{"1G", 1 << 30, false},
		{"abc", 0, true},
		{"-5", 0, true},
		{"2x", 0, true},
		{"k", 0, true},
	}
	for _, tt := range cases {
		got, err := parseRateLimit(tt.in)
		if tt.err {
			if err == nil {
				t.Errorf("parseRateLimit(%q) = %d, want error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseRateLimit(%q) error: %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseRateLimit(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestValidateHeaders(t *testing.T) {
	cases := []struct {
		h    string
		good bool
	}{
		{"Authorization: Bearer abc", true},
		{"X-Foo: bar", true},
		{"Foo", false},
		{": bar", false},
		{"", false},
	}
	for _, tt := range cases {
		err := validateHeaders([]string{tt.h})
		if tt.good && err != nil {
			t.Errorf("validateHeaders(%q) = %v, want nil", tt.h, err)
		}
		if !tt.good && err == nil {
			t.Errorf("validateHeaders(%q) = nil, want error", tt.h)
		}
	}
}

func TestRunRateLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 1024*1024) // 10 MiB
	srv := newTestServer(t, payload)
	out := filepath.Join(t.TempDir(), "rl.bin")
	start := time.Now()
	code := run([]string{"--allow-loopback", "-q", "--limit-rate", "5M", "-o", out, srv.URL})
	elapsed := time.Since(start)
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	t.Logf("10 MiB at 5 MiB/s took %v", elapsed)
	if elapsed < 1500*time.Millisecond {
		t.Errorf("rate-limited download finished in %v, too fast", elapsed)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestRunInfo(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 512*1024) // 5 MiB
	srv := newTestServer(t, payload)

	out := captureStdout(t, func() {
		if code := run([]string{"--allow-loopback", "--info", srv.URL}); code != 0 {
			t.Errorf("run(--info) = %d, want 0", code)
		}
	})
	for _, want := range []string{"url:", "size:", "ranges:", "workers:", "buf:"} {
		if !strings.Contains(out, want) {
			t.Errorf("--info output missing %q:\n%s", want, out)
		}
	}
}

func TestRunWorkersBufSize(t *testing.T) {
	payload := bytes.Repeat([]byte("abcdef"), 512*1024)
	srv := newTestServer(t, payload)
	out := filepath.Join(t.TempDir(), "wb.bin")
	code := run([]string{"--allow-loopback", "-q", "-x", "2", "--buf-size", "64k", "-o", out, srv.URL})
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestRunWorkersInvalid(t *testing.T) {
	if code := run([]string{"--allow-loopback", "-x", "999", "-o", "x.bin", "http://127.0.0.1:1/x.bin"}); code != 1 {
		t.Errorf("run(-x 999) = %d, want 1", code)
	}
}

func TestRunNoClobber(t *testing.T) {
	payload := []byte("clobber me")
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "c.bin")
	if code := run([]string{"--allow-loopback", "-q", "-o", out, srv.URL}); code != 0 {
		t.Fatalf("first run = %d", code)
	}
	before := hits.Load()

	// Second run with --no-clobber must skip without hitting the server.
	code := run([]string{"--allow-loopback", "-q", "--no-clobber", "-o", out, srv.URL})
	if code != 0 {
		t.Fatalf("no-clobber run = %d, want 0", code)
	}
	if hits.Load() != before {
		t.Errorf("no-clobber hit the server: %d -> %d requests", before, hits.Load())
	}
}

func TestRunInfoJSON(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 512*1024) // 5 MiB
	srv := newTestServer(t, payload)

	out := captureStdout(t, func() {
		if code := run([]string{"--allow-loopback", "--info", "--json", srv.URL}); code != 0 {
			t.Errorf("run(--info --json) = %d, want 0", code)
		}
	})
	var got struct {
		URL            string `json:"url"`
		Size           int64  `json:"size"`
		SupportsRanges bool   `json:"supports_ranges"`
		Workers        int    `json:"workers"`
		BufSize        int    `json:"buf_size"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("--json output is not a single JSON object: %v\n%s", err, out)
	}
	if got.URL != srv.URL {
		t.Errorf("url = %q, want %q", got.URL, srv.URL)
	}
	if got.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", got.Size, len(payload))
	}
	if !got.SupportsRanges {
		t.Error("supports_ranges = false, want true")
	}
	if got.Workers < 1 || got.BufSize < 1 {
		t.Errorf("workers=%d buf_size=%d, want > 0", got.Workers, got.BufSize)
	}
}

func TestRunJSONRequiresInfo(t *testing.T) {
	if code := run([]string{"--json", "-o", "x.bin", "http://127.0.0.1:1/x.bin"}); code != 1 {
		t.Errorf("run(--json without --info) = %d, want 1", code)
	}
}

func TestRunMaxRetries(t *testing.T) {
	payload := []byte("retries")
	srv := newTestServer(t, payload)
	out := filepath.Join(t.TempDir(), "r.bin")
	code := run([]string{"--allow-loopback", "-q", "--max-retries", "3", "-o", out, srv.URL})
	if code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("content = %q, want %q", got, payload)
	}
}

func TestRunMaxRetriesInvalid(t *testing.T) {
	if code := run([]string{"--max-retries", "1000", "-o", "x.bin", "http://127.0.0.1:1/x.bin"}); code != 1 {
		t.Errorf("run(--max-retries 1000) = %d, want 1", code)
	}
}

func TestRunInfoJSONError(t *testing.T) {
	// A URL that fails to probe must still emit a valid JSON object with
	// an error field (one line per URL, JSONL contract).
	out := captureStdout(t, func() {
		// 127.0.0.1 with --allow-loopback: probe will attempt a dial and fail.
		if code := run([]string{"--allow-loopback", "--info", "--json", "http://127.0.0.1:1/x.bin"}); code != 1 {
			t.Errorf("run(--info --json failing URL) = %d, want 1", code)
		}
	})
	var got struct {
		URL   string `json:"url"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("error result is not valid JSON: %v\n%s", err, out)
	}
	if got.URL != "http://127.0.0.1:1/x.bin" {
		t.Errorf("url = %q", got.URL)
	}
	if got.Error == "" {
		t.Error("expected non-empty error field")
	}
}

func TestRunHashAutoRemoteMD5(t *testing.T) {
	// A dataset server that publishes an .md5 sidecar (Zenodo/4TU style):
	// `-h auto` must fetch it remotely and verify, with zero local sidecar.
	payload := bytes.Repeat([]byte("physics-data-"), 64*1024) // ~704 KiB
	md5hex := md5.Sum(payload)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file.bin":
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		case "/file.bin.md5":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(hex.EncodeToString(md5hex[:]) + "  file.bin\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "file.bin")
	code := run([]string{"--allow-loopback", "-q", "-h", "auto", "-o", out, srv.URL + "/file.bin"})
	if code != 0 {
		t.Fatalf("run(-h auto) = %d, want 0 (remote md5 sidecar verified)", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// A wrong remote md5 must fail the download.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/file.bin" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("00000000000000000000000000000000  file.bin\n"))
	}))
	t.Cleanup(badSrv.Close)
	badOut := filepath.Join(t.TempDir(), "file.bin")
	if code := run([]string{"--allow-loopback", "-q", "-h", "auto", "-o", badOut, badSrv.URL + "/file.bin"}); code != 1 {
		t.Errorf("run(-h auto, wrong remote md5) = %d, want 1", code)
	}
}

func TestRunNoClobberResumesPartial(t *testing.T) {
	// A partial download (resume sidecar present) must NOT be skipped by
	// --no-clobber — it proceeds so the download can resume.
	payload := []byte("resume me")
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "p.bin")
	if err := os.WriteFile(out, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out+".gofetch.resume", []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if code := run([]string{"--allow-loopback", "-q", "--no-clobber", "-o", out, srv.URL}); code != 0 {
		t.Fatalf("run = %d, want 0", code)
	}
	if hits.Load() == 0 {
		t.Error("--no-clobber skipped a partial download instead of resuming")
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("content = %q, want %q", got, payload)
	}
}

func TestRunCACert(t *testing.T) {
	// A self-signed HTTPS dataset mirror becomes reachable via --ca-cert.
	payload := bytes.Repeat([]byte("private-mirror-"), 64*1024)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	srv.StartTLS()
	t.Cleanup(srv.Close)

	certPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(certPath, pemBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "s.bin")
	code := run([]string{"--allow-loopback", "-q", "--ca-cert", certPath, "-o", out, srv.URL})
	if code != 0 {
		t.Fatalf("run(--ca-cert) = %d, want 0", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// Without the CA, the same URL must fail (cert not trusted).
	badOut := filepath.Join(t.TempDir(), "bad.bin")
	if code := run([]string{"--allow-loopback", "-q", "-o", badOut, srv.URL}); code != 1 {
		t.Errorf("run without --ca-cert = %d, want 1", code)
	}
}

func TestRunHashAutoContainer(t *testing.T) {
	// A mirror that ships a SHA256SUMS container (Ubuntu/Debian style):
	// `-h auto` must fetch it, find the entry for the file, and verify.
	payload := bytes.Repeat([]byte("iso-data-"), 64*1024)
	sh := sha256.Sum256(payload)
	hex := hex.EncodeToString(sh[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/distros/latest/file.iso":
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		case "/distros/latest/SHA256SUMS":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(
				"0000000000000000000000000000000000000000000000000000000000000000  other-file.iso\n" +
					hex + "  ./latest/file.iso\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "file.iso")
	code := run([]string{"--allow-loopback", "-q", "-h", "auto", "-o", out, srv.URL + "/distros/latest/file.iso"})
	if code != 0 {
		t.Fatalf("run(-h auto with SHA256SUMS container) = %d, want 0", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestAutoDetectLocalSidecarContainer(t *testing.T) {
	dir := t.TempDir()
	hash := "be8458032f8105e60ee2a3067f950b6e3c007ee51b38dac50e8b48e765561c91"
	// Write a SHA256SUMS container listing two files, one matching.
	if err := os.WriteFile(filepath.Join(dir, "SHA256SUMS"),
		[]byte(hash+"  archlinux-x86_64.iso\n"+"0000000000000000000000000000000000000000000000000000000000000000  other.iso\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "archlinux-x86_64.iso")
	algo, hex, err := autoDetectLocalSidecar(out)
	if err != nil {
		t.Fatalf("autoDetectLocalSidecar: %v", err)
	}
	if algo != "sha256" || hex != hash {
		t.Errorf("got %s:%s, want sha256:%s", algo, hex, hash)
	}
	// A file not listed in the container → no match, no error.
	other := filepath.Join(dir, "unlisted.iso")
	if algo, hex, err := autoDetectLocalSidecar(other); err != nil || algo != "" || hex != "" {
		t.Errorf("unlisted file: algo=%q hex=%q err=%v, want empty", algo, hex, err)
	}
}
