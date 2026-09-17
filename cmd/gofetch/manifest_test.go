package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/wkwc/gofetch/internal/fetch"
)

// TestWriteManifestRoundTrip covers the -manifest-out writer: it must
// produce a loadable manifest whose chunks verify against the file.
func TestWriteManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data.bin")
	payload := bytes.Repeat([]byte("manifest-me-"), 300*1024) // >3 MiB → multiple 1 MiB chunks
	if err := os.WriteFile(file, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	mout := filepath.Join(dir, "out.gofetch.manifest")
	if err := writeManifest(mout, file); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	m, err := fetch.LoadManifest(mout)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if len(m.Chunks) < 2 {
		t.Fatalf("want multiple chunks for a >3 MiB file, got %d", len(m.Chunks))
	}
	if err := m.VerifyFull(file); err != nil {
		t.Fatalf("VerifyFull: %v", err)
	}
}

// TestWriteManifestMissingFile propagates the read error instead of
// writing an empty manifest.
func TestWriteManifestMissingFile(t *testing.T) {
	if err := writeManifest(filepath.Join(t.TempDir(), "o.manifest"), filepath.Join(t.TempDir(), "nope.bin")); err == nil {
		t.Fatal("want error for missing input, got nil")
	}
}
