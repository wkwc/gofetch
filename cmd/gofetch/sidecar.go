package main

import (
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wkwc/gofetch/internal/fetch"
)

// resolveHash figures out the hash algorithm and expected hex from the -h flag.
// Supports:
//   - ""            → auto-detect a local <out>.md5/.sha1/.sha256/.sha512 sidecar; no verification if none
//   - "auto"        → local sidecar first, else fetch <url>.md5 / <url>.sha1 / <url>.sha256 / <url>.sha512
//   - "sha256:hex"  → explicit algo + hex
//   - "sha512:hex"  → explicit algo + hex
//   - "hex..."      → bare hex, inferred by length (32=md5, 40=sha1, 64=sha256, 128=sha512)
//   - "/path/file"  → read sidecar file from local path
//   - "http(s)://"  → fetch sidecar hash from a URL
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
	// Local file path.
	if _, statErr := os.Stat(flag); statErr == nil {
		return fetch.ReadSidecarFile(flag)
	}
	// Sidecar URL.
	if strings.HasPrefix(flag, "http://") || strings.HasPrefix(flag, "https://") {
		return fetch.FetchSidecarHash(ctx, fetch.NewSafeClient(15*time.Second), flag)
	}
	// Otherwise parse as algo:hex or bare hex.
	return fetch.ParseHashFlag(flag)
}

// autoDetectLocalSidecar looks for <out>.md5/.sha1/.sha256/.sha512 (and
// *sum variants) next to the output file, then for a container checksum
// file (SHA256SUMS / sha256sums.txt) in the output's directory, matching
// the entry for the output's basename. This makes hash verification work
// with zero configuration when a checksum already sits beside the
// download — the common case for mirrors that ship checksums.
func autoDetectLocalSidecar(outPath string) (algo, hashHex string, err error) {
	for _, suffix := range []string{".sha256", ".sha512", ".sha1", ".md5", ".sha256sum", ".sha512sum", ".sha1sum", ".md5sum"} {
		path := outPath + suffix
		if _, statErr := os.Stat(path); statErr != nil {
			continue
		}
		return fetch.ReadSidecarFile(path)
	}
	// Container checksum file in the output's directory.
	dir := filepath.Dir(outPath)
	for _, container := range []string{"SHA256SUMS", "sha256sums.txt", "SHA512SUMS", "sha512sums.txt"} {
		data, readErr := os.ReadFile(filepath.Join(dir, container))
		if readErr != nil {
			continue
		}
		if algo, hex := fetch.ParseChecksumContainer(string(data), filepath.Base(outPath)); hex != "" {
			return algo, hex, nil
		}
	}
	return "", "", nil
}

// autoDetectRemoteSidecar probes, in parallel, <url>.md5/.sha1/.sha256/.sha512
// sidecars and container checksum files (Arch `sha256sums.txt`,
// Ubuntu/Debian `SHA256SUMS`, `sha512sums.txt`, `SHA512SUMS`) in the same
// directory, matching the entry for the file being downloaded. First hit wins
// and the rest are cancelled.
//
// Parallel (not sequential) is worth it: on a real mirror each probe is a
// ~400 ms round-trip, so 12 sequential probes cost seconds; concurrent they
// cost ~1 RTT. Measured safe against real WAF/rate-limited mirrors (12
// concurrent 404s, no blocks). On HTTP/2 the probes multiplex on one
// connection at zero extra cost. The sidecar scheme matches the primary URL
// (http primary → http sidecar). A single SSRF-hardened client is shared, so
// the transport and connection pool are created once.
func autoDetectRemoteSidecar(ctx context.Context, rawURL string) (algo, hashHex string, err error) {
	client := fetch.NewSafeClient(15 * time.Second)
	base := path.Base(urlPath(rawURL))
	if base == "" || base == "." || base == "/" {
		return "", "", nil
	}
	dir := strings.TrimSuffix(rawURL, base)

	type candidate struct {
		url  string
		want string // non-empty => container entry, matched by basename
	}
	var cands []candidate
	for _, suffix := range []string{".md5", ".sha1", ".sha256", ".sha512", ".md5sum", ".sha1sum", ".sha256sum", ".sha512sum"} {
		cands = append(cands, candidate{url: rawURL + suffix})
	}
	for _, c := range []string{"sha256sums.txt", "SHA256SUMS", "sha512sums.txt", "SHA512SUMS"} {
		cands = append(cands, candidate{url: dir + c, want: base})
	}

	type result struct{ algo, hex string }
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan result, 1)

	var wg sync.WaitGroup
	for _, c := range cands {
		wg.Add(1)
		go func(c candidate) {
			defer wg.Done()
			if c.want != "" {
				a, h, _ := fetch.FetchChecksumForFile(probeCtx, client, c.url, c.want)
				if h != "" {
					select {
					case results <- result{a, h}:
					default:
					}
					cancel()
				}
				return
			}
			a, h, e := fetch.FetchSidecarHash(probeCtx, client, c.url)
			if e == nil && h != "" {
				select {
				case results <- result{a, h}:
				default:
				}
				cancel()
			}
		}(c)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case r := <-results:
		return r.algo, r.hex, nil
	case <-done:
		return "", "", nil // none found
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
}

// urlPath returns the parsed URL path, defaulting to the raw string.
func urlPath(rawURL string) string {
	if u, err := url.Parse(rawURL); err == nil {
		return u.Path
	}
	return rawURL
}
