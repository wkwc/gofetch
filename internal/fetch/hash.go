package fetch

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// hashBufSize is the buffer size used for SHA-256 computation.
const hashBufSize = 256 * 1024 // 256 KiB

var hashBufPool = sync.Pool{
	New: func() any { b := make([]byte, hashBufSize); return &b },
}

// fileHexSHA256 returns the hex-encoded SHA-256 of the file at path.
func fileHexSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	bp := hashBufPool.Get().(*[]byte)
	defer hashBufPool.Put(bp)
	if _, err := io.CopyBuffer(h, f, *bp); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hexEqual compares two hex strings case-insensitively.
func hexEqual(a, b string) bool {
	return len(a) == len(b) && strings.EqualFold(a, b)
}

// ValidateHexSHA256 returns nil if s is a valid 64-character hex string.
func ValidateHexSHA256(s string) error {
	if len(s) != sha256.Size*2 {
		return errors.New("expected 64 hex characters")
	}
	for i := range s {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return errors.New("expected 64 hex characters")
		}
	}
	return nil
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
