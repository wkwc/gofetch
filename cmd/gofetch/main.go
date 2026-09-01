// Command gofetch is an opinionated concurrent HTTP downloader.
//
// Usage:
//
//	gofetch [options] <url>
//
// It just works. Workers, buffers, timeouts, retries, compression, proxy,
// and resume are all auto-configured. The only flags are for things that
// genuinely require user input: output path, hash verification, and
// verbosity.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/wkwc/gofetch/internal/fetch"
)

// version is injected at link time: -ldflags="-X main.version=v1.2.3"
var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	var (
		outPath     = flag.String("o", "", "output file path (default: basename of URL)")
		quiet       = flag.Bool("q", false, "suppress progress output")
		verbose     = flag.Bool("v", false, "verbose logging")
		hashFlag    = flag.String("h", "", "verify integrity: auto-detects local sidecar, or sha256:hex / sha512:hex / path / auto")
		noResume    = flag.Bool("no-resume", false, "disable resume (default: on)")
		mirrorsFlag = flag.String("m", "", "comma-separated mirror URLs tried on failure (bare hostnames get https://)")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: gofetch [options] <url>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "opinionated concurrent downloader — everything auto-tuned internally")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "examples:")
		fmt.Fprintln(os.Stderr, "  gofetch https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -o out.bin https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -h auto https://example.com/file.bin        # local sidecar, else fetch .sha256/.sha512")
		fmt.Fprintln(os.Stderr, "  gofetch -h sha256:abc123... https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -q https://example.com/file.bin             # quiet (prints filename only)")
		fmt.Fprintln(os.Stderr, "  gofetch -v https://example.com/file.bin              # verbose (debug to stderr)")
		fmt.Fprintln(os.Stderr, "  gofetch -m mirror1,mirror2,mirror3 https://primary.com/file.bin")
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("gofetch", version)
		return 0
	}

	if flag.NArg() != 1 {
		flag.Usage()
		return 2
	}
	rawURL := flag.Arg(0)

	if err := validateURL(rawURL); err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	u, _ := url.Parse(rawURL)

	out := *outPath
	if out == "" {
		base := filepath.Base(u.Path)
		if base == "" || base == "/" {
			base = "downloaded.bin"
		}
		out = base
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mirrors, err := normalizeMirrors(*mirrorsFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	algo, hashHex, err := resolveHash(ctx, *hashFlag, rawURL, out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	d := fetch.NewDownloader(rawURL, out, fetch.Options{
		HashAlgo:     algo,
		ExpectedHash: hashHex,
		NoResume:     *noResume,
		Verbose:      *verbose,
		Quiet:        *quiet,
		Mirrors:      mirrors,
	})

	if err := d.Download(ctx); err != nil {
		// User-initiated cancel (Ctrl-C / SIGTERM) is not a failure of
		// the downloader — the partial progress was already flushed to
		// the resume sidecar, so say so plainly instead of wrapping it
		// as a mirror error.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintln(os.Stderr, "gofetch: interrupted; partial progress saved, re-run to resume")
			return 130
		}
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	// Always print the output path on success (quiet: filename only;
	// verbose/normal: summary already went to stderr in finalize).
	fmt.Println(out)
	return 0
}

// normalizeMirrors parses the -m flag into validated mirror URLs. Bare
// hostnames get https:// prepended so `-m mirror1.com,mirror2.com` works
// as documented. Each mirror is validated with the same SSRF guards as the
// primary URL.
func normalizeMirrors(flag string) ([]string, error) {
	if flag == "" {
		return nil, nil
	}
	parts := strings.Split(flag, ",")
	mirrors := make([]string, 0, len(parts))
	for i, m := range parts {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if !strings.Contains(m, "://") {
			m = "https://" + m
		}
		if err := validateURL(m); err != nil {
			return nil, fmt.Errorf("mirror %d: %w", i+1, err)
		}
		mirrors = append(mirrors, m)
	}
	return mirrors, nil
}

// resolveHash figures out the hash algorithm and expected hex from the -h flag.
// Supports:
//   - ""            → auto-detect a local <out>.sha256/.sha512 sidecar; no verification if none
//   - "auto"        → local sidecar first, else fetch <url>.sha256 / <url>.sha512
//   - "sha256:hex"  → explicit algo + hex
//   - "sha512:hex"  → explicit algo + hex
//   - "hex..."      → bare hex, treated as sha256
//   - "/path/file"  → read sidecar file from local path
func resolveHash(ctx context.Context, flag string, rawURL, outPath string) (algo, hashHex string, err error) {
	if flag == "" || flag == "auto" {
		// "" auto-detects a local <out>.sha256/.sha512 sidecar only.
		// "auto" falls back to fetching <url>.sha256 / <url>.sha512.
		algo, hex, e := autoDetectLocalSidecar(outPath)
		if flag == "" || e != nil || hex != "" {
			return algo, hex, e
		}
		return autoDetectRemoteSidecar(ctx, rawURL)
	}
	// Check if it's a local file path
	if _, statErr := os.Stat(flag); statErr == nil {
		return readSidecarFile(flag)
	}
	// Check if it's a URL
	if strings.HasPrefix(flag, "http://") || strings.HasPrefix(flag, "https://") {
		return fetchSidecarURL(ctx, fetch.NewSafeClient(15*time.Second), flag)
	}
	// Otherwise parse as algo:hex or bare hex
	return fetch.ParseHashFlag(flag)
}

// autoDetectLocalSidecar looks for <out>.sha256, <out>.sha512 (and the
// *sum variants) next to the output file. This makes hash verification
// work with zero configuration when a sidecar already sits beside the
// download — the common case for mirrors that ship checksums.
func autoDetectLocalSidecar(outPath string) (algo, hashHex string, err error) {
	for _, suffix := range []string{".sha256", ".sha512", ".sha256sum", ".sha512sum"} {
		path := outPath + suffix
		if _, statErr := os.Stat(path); statErr != nil {
			continue
		}
		return readSidecarFile(path)
	}
	return "", "", nil
}

// autoDetectRemoteSidecar fetches <url>.sha256 then <url>.sha512 sidecars.
// A single SSRF-hardened client is reused across suffix attempts so the
// transport and connection pool are created once, not per attempt.
func autoDetectRemoteSidecar(ctx context.Context, rawURL string) (algo, hashHex string, err error) {
	client := fetch.NewSafeClient(15 * time.Second)
	for _, suffix := range []string{".sha256", ".sha512", ".sha256sum", ".sha512sum"} {
		sidecarURL := rawURL + suffix
		algo, hex, e := fetchSidecarURL(ctx, client, sidecarURL)
		if e == nil && hex != "" {
			return algo, hex, nil
		}
	}
	return "", "", nil // no sidecar found — silently skip
}

// fetchSidecarURL fetches a sidecar hash file from a URL and parses it.
// Requires HTTPS and rejects private/internal IP ranges to prevent SSRF.
func fetchSidecarURL(ctx context.Context, client *http.Client, sidecarURL string) (algo, hashHex string, err error) {
	parsed, err := url.Parse(sidecarURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid sidecar URL: %w", err)
	}
	// SSRF protection: require HTTPS and reject private/internal IPs
	if parsed.Scheme != "https" {
		return "", "", fmt.Errorf("sidecar URL must use HTTPS")
	}
	if fetch.HostIsPrivate(parsed.Hostname()) {
		return "", "", fmt.Errorf("sidecar URL host %q resolves to a private/internal address (SSRF guard)", parsed.Hostname())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sidecarURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}
	// Same SSRF-hardened dial + redirect policy as the main downloader.
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("sidecar HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", "", fmt.Errorf("read sidecar: %w", err)
	}
	return parseSidecarContent(string(data), sidecarURL)
}

// validateURL ensures the URL is fetchable in a public-downloader
// context: scheme is http or https, AND the resolved IP is not a
// private/loopback/link-local/unspecified address. DNS failures REJECT
// the URL — an attacker who can fail DNS resolution (or trick
// getaddrinfo into short-timeout behaviour) must not be able to bypass
// the internal-IP check by exploiting a transient lookup error.
func validateURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid URL: %s", rawURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s (use http or https)", u.Scheme)
	}
	// SSRF: reject URLs that resolve to loopback / private / link-local /
	// multicast / unspecified IPs. Combined with DialContextAuto (which
	// pins each dial to a non-private resolved IP), DNS rebinding cannot
	// pivot the connection into an internal host mid-stream.
	if fetch.HostIsPrivate(u.Hostname()) {
		return fmt.Errorf("URL host %q resolves to a private/internal address (SSRF guard)",
			u.Hostname())
	}
	return nil
}

// readSidecarFile reads a local sidecar hash file and parses it.
// Accepts any readable absolute or relative path; symlinks are followed
// by os.ReadFile. We do not jail the path to cwd: README documents
// `gofetch -h /tmp/file.sha256` and similar, and sidecar content is
// parsed (never executed), so the only risk is reading an attacker-
// chosen file — equivalent to running `cat` on it.
func readSidecarFile(path string) (algo, hashHex string, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve sidecar path: %w", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return "", "", fmt.Errorf("read sidecar: %w", err)
	}
	return parseSidecarContent(string(data), abs)
}

// parseSidecarContent parses a sidecar hash file. Common formats:
//
//	<hash>  <filename>
//	<hash>
//
// The algorithm is inferred from the hash length (64 = sha256, 128 = sha512)
// or from the file extension (.sha256, .sha512). The hex is validated.
func parseSidecarContent(content, sourcePath string) (algo, hashHex string, err error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", "", fmt.Errorf("empty sidecar file: %s", sourcePath)
	}
	// Take the first token (the hash)
	fields := strings.Fields(content)
	if len(fields) == 0 {
		return "", "", fmt.Errorf("no hash found in sidecar: %s", sourcePath)
	}
	hexHash := fields[0]

	// Validate hex characters
	if !isValidHex(hexHash) {
		return "", "", fmt.Errorf("invalid hex in sidecar: %s", sourcePath)
	}

	// Infer algorithm from hash length
	switch len(hexHash) {
	case 64:
		return "sha256", hexHash, nil
	case 128:
		return "sha512", hexHash, nil
	default:
		// Fall back to file extension
		switch {
		case strings.HasSuffix(sourcePath, ".sha256"), strings.HasSuffix(sourcePath, ".sha256sum"):
			return "sha256", hexHash, nil
		case strings.HasSuffix(sourcePath, ".sha512"), strings.HasSuffix(sourcePath, ".sha512sum"):
			return "sha512", hexHash, nil
		}
		return "", "", fmt.Errorf("cannot determine hash algorithm from sidecar (hex length %d): %s", len(hexHash), sourcePath)
	}
}

// isValidHex checks if a string contains only valid hex characters.
func isValidHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
