package main

import (
	"fmt"

	"github.com/wkwc/gofetch/internal/fetch"
)

// writeManifest generates a per-chunk integrity manifest for the
// downloaded file and writes it to path. A later `gofetch` run that
// finds this manifest beside the output verifies each chunk during
// download and can surgically recover from corruption.
func writeManifest(path, file string) error {
	m, err := fetch.ManifestForFile(file, "sha256", 0)
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := fetch.WriteManifest(path, m); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	return nil
}
