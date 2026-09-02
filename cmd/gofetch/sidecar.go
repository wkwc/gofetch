package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/wkwc/gofetch/internal/fetch"
)

// resolveHash figures out the hash algorithm and expected hex from the -h flag.
// Supports:
//   - ""            → auto-detect a local <out>.sha256/.sha512 sidecar; no verification if none
//   - "auto"        → local sidecar first, else fetch <url>.sha256 / <url>.sha512
//   - "sha256:hex"  → explicit algo + hex
//   - "sha512:hex"  → explicit algo + hex
//   - "hex..."      → bare hex, treated as sha256
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
		return fetch.ReadSidecarFile(path)
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
		algo, hex, e := fetch.FetchSidecarHash(ctx, client, sidecarURL)
		if e == nil && hex != "" {
			return algo, hex, nil
		}
	}
	return "", "", nil // no sidecar found — silently skip
}
