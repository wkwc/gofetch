package fetch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// fileHexSHA256 returns the hex-encoded SHA-256 of the file at path.
func fileHexSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hexEqual compares two hex strings case-insensitively.
func hexEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// verifyFileHash computes the file's SHA-256 and compares against expected.
// Returns nil on match or when expected is empty.
func verifyFileHash(path, expected string) error {
	if expected == "" {
		return nil
	}
	got, err := fileHexSHA256(path)
	if err != nil {
		return err
	}
	if !hexEqual(got, expected) {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expected, got)
	}
	return nil
}
