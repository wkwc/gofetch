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
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/local/gofetch/internal/fetch"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		outPath     = flag.String("o", "", "output file path (default: basename of URL)")
		quiet       = flag.Bool("q", false, "suppress progress output")
		verbose     = flag.Bool("v", false, "verbose logging")
		hashFlag    = flag.String("h", "", "verify integrity (sha256:hex, sha512:hex, auto, or path to .sha256/.sha512 sidecar)")
		noResume    = flag.Bool("no-resume", false, "disable resume (default: on)")
		mirrorsFlag = flag.String("m", "", "comma-separated list of mirror URLs to try on failure")
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
		fmt.Fprintln(os.Stderr, "  gofetch -h auto https://example.com/file.bin        # auto-detect .sha256/.sha512 sidecar")
		fmt.Fprintln(os.Stderr, "  gofetch -h sha256:abc123... https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -q https://example.com/file.bin             # quiet (prints filename only)")
		fmt.Fprintln(os.Stderr, "  gofetch -v https://example.com/file.bin              # verbose (debug to stderr)")
		fmt.Fprintln(os.Stderr, "  gofetch -m mirror1,mirror2,mirror3 https://primary.com/file.bin")
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		return 2
	}
	rawURL := flag.Arg(0)

	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintln(os.Stderr, "gofetch: invalid URL:", rawURL)
		return 1
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		fmt.Fprintln(os.Stderr, "gofetch: unsupported scheme:", u.Scheme, "(use http or https)")
		return 1
	}

	out := *outPath
	if out == "" {
		base := filepath.Base(u.Path)
		if base == "" || base == "/" {
			base = "downloaded.bin"
		}
		out = base
	}

	var mirrors []string
	if *mirrorsFlag != "" {
		mirrors = strings.Split(*mirrorsFlag, ",")
		for i := range mirrors {
			mirrors[i] = strings.TrimSpace(mirrors[i])
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	if !*quiet {
		fmt.Println(out)
	}
	return 0
}

// resolveHash figures out the hash algorithm and expected hex from the -h flag.
// Supports:
//   - ""            → no verification
//   - "auto"        → try to fetch <url>.sha256 or <url>.sha512 sidecar
//   - "sha256:hex"  → explicit algo + hex
//   - "sha512:hex"  → explicit algo + hex
//   - "hex..."      → bare hex, treated as sha256
//   - "/path/file"  → read sidecar file from local path
func resolveHash(ctx context.Context, flag string, rawURL, outPath string) (algo, hashHex string, err error) {
	if flag == "" {
		return "", "", nil
	}
	if flag == "auto" {
		return autoDetectSidecar(ctx, rawURL)
	}
	// Check if it's a local file path
	if _, statErr := os.Stat(flag); statErr == nil {
		return readSidecarFile(flag)
	}
	// Check if it's a URL
	if strings.HasPrefix(flag, "http://") || strings.HasPrefix(flag, "https://") {
		return fetchSidecarURL(ctx, flag)
	}
	// Otherwise parse as algo:hex or bare hex
	return fetch.ParseHashFlag(flag)
}

// autoDetectSidecar tries to fetch <url>.sha256 then <url>.sha512 sidecar files.
func autoDetectSidecar(ctx context.Context, rawURL string) (algo, hashHex string, err error) {
	for _, suffix := range []string{".sha256", ".sha512", ".sha256sum", ".sha512sum"} {
		sidecarURL := rawURL + suffix
		algo, hex, e := fetchSidecarURL(ctx, sidecarURL)
		if e == nil && hex != "" {
			return algo, hex, nil
		}
	}
	return "", "", nil // no sidecar found — silently skip
}

// fetchSidecarURL fetches a sidecar hash file from a URL and parses it.
// Requires HTTPS and rejects private/internal IP ranges to prevent SSRF.
func fetchSidecarURL(ctx context.Context, sidecarURL string) (algo, hashHex string, err error) {
	parsed, err := url.Parse(sidecarURL)
	if err != nil {
		return "", "", nil
	}
	// SSRF protection: require HTTPS and reject private/internal IPs
	if parsed.Scheme != "https" {
		return "", "", nil
	}
	if isPrivateOrInternalIP(parsed.Hostname()) {
		return "", "", nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sidecarURL, nil)
	if err != nil {
		return "", "", nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", "", nil
	}
	return parseSidecarContent(string(data), sidecarURL)
}

// isPrivateOrInternalIP checks if a hostname resolves to a private, loopback,
// or link-local IP address. Returns true for IPs that should not be contacted
// in a public download context (SSRF prevention).
func isPrivateOrInternalIP(hostname string) bool {
	ips, err := net.LookupIP(hostname)
	if err != nil {
		// If we can't resolve, be safe and reject
		return true
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			return true
		}
	}
	return false
}

// readSidecarFile reads a local sidecar hash file and parses it.
// Validates that the path is safe (no directory traversal).
func readSidecarFile(path string) (algo, hashHex string, err error) {
	// Resolve to absolute path and ensure it doesn't escape the working directory
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve sidecar path: %w", err)
	}
	// Ensure the path is within the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("get working dir: %w", err)
	}
	rel, err := filepath.Rel(cwd, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", "", fmt.Errorf("sidecar path %s escapes working directory", path)
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
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
