package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wkwc/gofetch/internal/fetch"
)

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
