package fetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ParseSidecarContent parses a sidecar hash file. Common formats:
//
//	<hash>  <filename>
//	<hash>
//
// The algorithm is inferred from the hash length (32 = md5, 40 = sha1,
// 64 = sha256, 128 = sha512) or from the file extension. The hex is
// validated.
func ParseSidecarContent(content, sourcePath string) (algo, hashHex string, err error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", "", fmt.Errorf("empty sidecar file: %s", sourcePath)
	}
	fields := strings.Fields(content)
	if len(fields) == 0 {
		return "", "", fmt.Errorf("no hash found in sidecar: %s", sourcePath)
	}
	hexHash := fields[0]

	if !IsValidHex(hexHash) {
		return "", "", fmt.Errorf("invalid hex in sidecar: %s", sourcePath)
	}

	for _, algo := range []string{"md5", "sha1", "sha256", "sha512"} {
		if len(hexHash) == hashSize(algo)*2 {
			return algo, hexHash, nil
		}
	}
	// Fall back to the file extension.
	switch {
	case strings.HasSuffix(sourcePath, ".sha256"), strings.HasSuffix(sourcePath, ".sha256sum"):
		return "sha256", hexHash, nil
	case strings.HasSuffix(sourcePath, ".sha512"), strings.HasSuffix(sourcePath, ".sha512sum"):
		return "sha512", hexHash, nil
	case strings.HasSuffix(sourcePath, ".sha1"), strings.HasSuffix(sourcePath, ".sha1sum"):
		return "sha1", hexHash, nil
	case strings.HasSuffix(sourcePath, ".md5"), strings.HasSuffix(sourcePath, ".md5sum"):
		return "md5", hexHash, nil
	}
	return "", "", fmt.Errorf("cannot determine hash algorithm from sidecar (hex length %d): %s", len(hexHash), sourcePath)
}

// IsValidHex reports whether s contains only ASCII hex digits.
func IsValidHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// ReadSidecarFile reads a local sidecar hash file and parses it.
// Accepts any readable absolute or relative path; symlinks are followed
// by os.ReadFile. We do not jail the path to cwd: README documents
// `gofetch -h /tmp/file.sha256` and similar, and sidecar content is
// parsed (never executed), so the only risk is reading an attacker-
// chosen file — equivalent to running `cat` on it.
func ReadSidecarFile(path string) (algo, hashHex string, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve sidecar path: %w", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return "", "", fmt.Errorf("read sidecar: %w", err)
	}
	return ParseSidecarContent(string(data), abs)
}

// FetchSidecarHash fetches a sidecar hash file from a URL and parses it.
// The scheme is the caller's choice: `-h auto` derives it from the
// primary URL, so an http primary gets an http sidecar (the download is
// already unauthenticated; the sidecar adds no new exposure) and an https
// primary stays https. The SSRF host check applies regardless of scheme —
// private/loopback/link-local hosts are always rejected.
func FetchSidecarHash(ctx context.Context, client *http.Client, sidecarURL string) (algo, hashHex string, err error) {
	parsed, err := url.Parse(sidecarURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid sidecar URL: %w", err)
	}
	// SSRF protection: reject private/internal IPs (scheme checked by the
	// caller via URL construction or an explicit -h URL).
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", fmt.Errorf("sidecar URL must use http or https")
	}
	if HostIsPrivateContext(ctx, parsed.Hostname()) {
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
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("sidecar HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", "", fmt.Errorf("read sidecar: %w", err)
	}
	return ParseSidecarContent(string(data), sidecarURL)
}
