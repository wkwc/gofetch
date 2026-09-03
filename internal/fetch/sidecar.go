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

	if algo, ok := algoForLen(hexHash); ok {
		return algo, hexHash, nil
	}
	// Fall back to the file extension.
	switch {
	case strings.HasSuffix(sourcePath, ".sha256"), strings.HasSuffix(sourcePath, ".sha256sum"):
		return algoSHA256, hexHash, nil
	case strings.HasSuffix(sourcePath, ".sha512"), strings.HasSuffix(sourcePath, ".sha512sum"):
		return algoSHA512, hexHash, nil
	case strings.HasSuffix(sourcePath, ".sha1"), strings.HasSuffix(sourcePath, ".sha1sum"):
		return algoSHA1, hexHash, nil
	case strings.HasSuffix(sourcePath, ".md5"), strings.HasSuffix(sourcePath, ".md5sum"):
		return algoMD5, hexHash, nil
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

// fetchSidecarContent fetches a hash file from a URL with the same SSRF
// guards as the main downloader and returns its raw text (bounded to
// 1 MiB — checksum containers are small).
func fetchSidecarContent(ctx context.Context, client *http.Client, sidecarURL string) (string, error) {
	parsed, err := url.Parse(sidecarURL)
	if err != nil {
		return "", fmt.Errorf("invalid sidecar URL: %w", err)
	}
	// SSRF protection: reject private/internal IPs (scheme checked by the
	// caller via URL construction or an explicit -h URL).
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("sidecar URL must use http or https")
	}
	if HostIsPrivateContext(ctx, parsed.Hostname()) {
		return "", fmt.Errorf("sidecar URL host %q resolves to a private/internal address (SSRF guard)", parsed.Hostname())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sidecarURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	// Same SSRF-hardened dial + redirect policy as the main downloader.
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("sidecar HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read sidecar: %w", err)
	}
	return string(data), nil
}

// FetchSidecarHash fetches a sidecar hash file from a URL and parses it.
// The scheme is the caller's choice: `-h auto` derives it from the
// primary URL, so an http primary gets an http sidecar (the download is
// already unauthenticated; the sidecar adds no new exposure) and an https
// primary stays https. The SSRF host check applies regardless of scheme —
// private/loopback/link-local hosts are always rejected.
func FetchSidecarHash(ctx context.Context, client *http.Client, sidecarURL string) (algo, hashHex string, err error) {
	content, err := fetchSidecarContent(ctx, client, sidecarURL)
	if err != nil {
		return "", "", err
	}
	return ParseSidecarContent(content, sidecarURL)
}

// algoForLen infers the hash algorithm from hex length (32=md5, 40=sha1,
// 64=sha256, 128=sha512). Shared by the sidecar and container parsers.
func algoForLen(h string) (string, bool) {
	for _, algo := range []string{algoMD5, algoSHA1, algoSHA256, algoSHA512} {
		if len(h) == hashSize(algo)*2 {
			return algo, true
		}
	}
	return "", false
}

// ParseChecksumContainer parses a multi-entry checksum file such as
// Arch's `sha256sums.txt` or Ubuntu's `SHA256SUMS` (lines of
// `<hash>  <filename>`) and returns the hash whose final path component
// matches want. Returns ("", "") when no entry matches.
func ParseChecksumContainer(content, want string) (algo, hashHex string) {
	want = filepath.Base(want)
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if filepath.Base(fields[len(fields)-1]) != want {
			continue
		}
		h := fields[0]
		if !IsValidHex(h) {
			continue
		}
		if algo, ok := algoForLen(h); ok {
			return algo, h
		}
	}
	return "", ""
}

// FetchChecksumForFile fetches a container checksum file and returns the
// entry for want (matched on the final path component). Returns no error
// and empty values when the container is missing or has no matching entry.
func FetchChecksumForFile(ctx context.Context, client *http.Client, containerURL, want string) (algo, hashHex string, err error) {
	// Best-effort auto-detection: a missing or unreachable container (404,
	// connection error, empty) is NOT an error — it means "no checksum
	// here", and the caller moves on. Only a matched entry is a result.
	content, err := fetchSidecarContent(ctx, client, containerURL)
	if err != nil {
		return "", "", nil // container absent or unreachable → not a finding
	}
	algo, hex := ParseChecksumContainer(content, want)
	return algo, hex, nil
}
