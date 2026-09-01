package fetch

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
)

// hashBufSize is the buffer size used for hash computation.
// Matches the worker read-buffer pool so fileHexHash shares it.
const hashBufSize = bufSizeLarge

// newHash returns a hash.Hash for the given algorithm name.
func newHash(algo string) hash.Hash {
	switch algo {
	case "sha512":
		return sha512.New()
	default:
		return sha256.New()
	}
}

// hashSize returns the digest size for the given algorithm.
func hashSize(algo string) int {
	switch algo {
	case "sha512":
		return sha512.Size
	default:
		return sha256.Size
	}
}

// ParseHashFlag parses "-h sha256:abcdef..." or "-h sha512:abcdef...".
// Also accepts bare hex (treated as sha256 for backward compatibility).
// Returns (algo, hex, error).
func ParseHashFlag(s string) (algo, hexHash string, err error) {
	if s == "" {
		return "", "", nil
	}
	// Check for "algo:hex" format
	if idx := strings.IndexByte(s, ':'); idx > 0 {
		a := strings.ToLower(s[:idx])
		h := s[idx+1:]
		if a != "sha256" && a != "sha512" {
			return "", "", fmt.Errorf("unsupported hash algorithm %q (use sha256 or sha512)", a)
		}
		if err := validateHexHash(h, a); err != nil {
			return "", "", err
		}
		return a, h, nil
	}
	// Bare hex — assume sha256
	if err := validateHexHash(s, "sha256"); err != nil {
		return "", "", err
	}
	return "sha256", s, nil
}

// validateHexHash checks that s is a valid hex string of the correct length.
func validateHexHash(s, algo string) error {
	expected := hashSize(algo) * 2
	if len(s) != expected {
		return fmt.Errorf("expected %d hex characters for %s, got %d", expected, algo, len(s))
	}
	// `encoding/hex.DecodeString` accepts lower- or upper-case and short-
	// inputs (returns an error for odd-length). Combined with the length
	// check above this is functionally equivalent to a hand-written
	// loop, with no allocation and a single vectorised call.
	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("invalid hex for %s: %w", algo, err)
	}
	return nil
}

// fileHexHash computes the hex-encoded hash of the file at path.
func fileHexHash(path, algo string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := newHash(algo)
	buf := acquireBuf(hashBufSize)
	defer releaseBuf(buf)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hexEqual compares two hex strings case-insensitively.
func hexEqual(a, b string) bool {
	return len(a) == len(b) && strings.EqualFold(a, b)
}

// verifyFileHash computes the file's hash and compares against expected.
// Returns nil on match or when expected is empty.
func verifyFileHash(path, algo, expected string) error {
	if expected == "" {
		return nil
	}
	got, err := fileHexHash(path, algo)
	if err != nil {
		return err
	}
	if !hexEqual(got, expected) {
		return fmt.Errorf("hash mismatch (%s): expected %s, got %s", algo, expected, got)
	}
	return nil
}
