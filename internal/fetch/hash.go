package fetch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// hashBufSize is the buffer size used for SHA-256 computation.
const hashBufSize = 256 * 1024 // 256 KiB

// fileHexSHA256 returns the hex-encoded SHA-256 of the file at path.
func fileHexSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, hashBufSize)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hexEqual compares two hex strings case-insensitively.
func hexEqual(a, b string) bool {
	return len(a) == len(b) && strings.EqualFold(a, b)
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
