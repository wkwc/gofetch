package fetch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCheckRedirectSafeGuards verifies the redirect guard the production
// clients install: hop cap, scheme cap, and private-host rejection.
func TestCheckRedirectSafeGuards(t *testing.T) {
	req := func(u string) *http.Request {
		r, err := http.NewRequestWithContext(testCtx(t, time.Second), http.MethodGet, u, http.NoBody)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		return r
	}

	if err := CheckRedirectSafe(req("ftp://example.com/f"), nil); err == nil {
		t.Error("expected non-http scheme rejection, got nil")
	}
	if err := CheckRedirectSafe(req("https://example.com/f"), nil); err != nil {
		t.Errorf("public https redirect should pass: %v", err)
	}

	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })
	if err := CheckRedirectSafe(req("http://127.0.0.1/f"), nil); err == nil {
		t.Error("expected private-host rejection, got nil")
	} else if !strings.Contains(err.Error(), "private/internal") {
		t.Errorf("expected private/internal message, got: %v", err)
	}

	via := []*http.Request{req("https://a/"), req("https://b/"), req("https://c/")}
	if err := CheckRedirectSafe(req("https://d/"), via); err == nil {
		t.Error("expected hop-cap rejection, got nil")
	}
}

// TestCheckRedirectSafeEndToEnd drives the guard through a real client:
// a public server redirecting to loopback must be refused when loopback
// is not allowed.
func TestCheckRedirectSafeEndToEnd(t *testing.T) {
	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/secret", http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{CheckRedirect: CheckRedirectSafe, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(testCtx(t, 5*time.Second), http.MethodGet, srv.URL, http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("redirect to loopback should be blocked, got nil error")
	}
	if !strings.Contains(err.Error(), "private/internal") {
		t.Errorf("expected private/internal block, got: %v", err)
	}
}

// TestNewSafeClientFetchesHash covers the CLI's sidecar client: it must
// fetch and parse a checksum over the SSRF-hardened transport.
func TestNewSafeClientFetchesHash(t *testing.T) {
	payload := makePayload(8 * 1024)
	hash := sha256Hex(payload)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(hash + "  out.bin\n"))
	}))
	t.Cleanup(srv.Close)

	algo, hex, err := FetchSidecarHash(context.Background(), NewSafeClient(5*time.Second), srv.URL)
	if err != nil {
		t.Fatalf("FetchSidecarHash via NewSafeClient: %v", err)
	}
	if algo != "sha256" || hex != hash {
		t.Errorf("got %s:%s, want sha256:%s", algo, hex, hash)
	}
}

func TestReadSidecarFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.sha256")
	hash := sha256Hex(makePayload(1024))
	if err := os.WriteFile(path, []byte(hash+"  out.bin\n"), 0o644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	algo, hex, err := ReadSidecarFile(path)
	if err != nil {
		t.Fatalf("ReadSidecarFile: %v", err)
	}
	if algo != "sha256" || hex != hash {
		t.Errorf("got %s:%s, want sha256:%s", algo, hex, hash)
	}

	if _, _, err := ReadSidecarFile(filepath.Join(dir, "missing.sha256")); err == nil {
		t.Error("expected error for missing sidecar, got nil")
	}
}

// TestFetchChecksumForFile covers the container-checksum path used by
// `-h auto`: a matched entry is a result; a missing/unreachable container
// or an entry for a different file is not an error.
func TestFetchChecksumForFile(t *testing.T) {
	iso := sha256Hex(makePayload(2048))
	content := iso + "  ubuntu-24.04.3-desktop-amd64.iso\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(content))
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{Transport: &http.Transport{}}
	ctx := context.Background()

	algo, hex, err := FetchChecksumForFile(ctx, client, srv.URL+"/sha256sums.txt", "ubuntu-24.04.3-desktop-amd64.iso")
	if err != nil {
		t.Fatalf("FetchChecksumForFile: %v", err)
	}
	if hex != iso || algo != "sha256" {
		t.Errorf("got %s:%s, want sha256:%s", algo, hex, iso)
	}

	algo, hex, err = FetchChecksumForFile(ctx, client, srv.URL+"/sha256sums.txt", "other.iso")
	if err != nil || hex != "" {
		t.Errorf("non-matching entry should be no finding, got algo=%q hex=%q err=%v", algo, hex, err)
	}

	algo, hex, err = FetchChecksumForFile(ctx, client, srv.URL+"/missing.txt", "any.iso")
	if err != nil || hex != "" {
		t.Errorf("absent container should be no finding, got algo=%q hex=%q err=%v", algo, hex, err)
	}
}

// TestLoadEnvProxyHosts verifies env parsing: scheme prefixing, case
// folding, and host matching. Restores the package-global proxy state.
func TestLoadEnvProxyHosts(t *testing.T) {
	proxyMu.Lock()
	prevInit, prevHosts := proxyInit, proxyHosts
	proxyMu.Unlock()
	t.Cleanup(func() {
		proxyMu.Lock()
		proxyInit, proxyHosts = prevInit, prevHosts
		proxyMu.Unlock()
	})

	t.Setenv("HTTP_PROXY", "pr0xy-test.invalid:3128")
	t.Setenv("ALL_PROXY", "https://ALSO-pr0xy-test.invalid:8443")
	proxyMu.Lock()
	loadEnvProxyHostsLocked()
	proxyMu.Unlock()

	for _, hostport := range []string{
		"pr0xy-test.invalid:3128",
		"pr0xy-test.invalid",
		"also-pr0xy-test.invalid:8443",
	} {
		if !envProxyHost(hostport) {
			t.Errorf("envProxyHost(%q) = false, want true", hostport)
		}
	}
	if envProxyHost("unrelated.invalid:80") {
		t.Error("envProxyHost matched an unregistered host")
	}
}
