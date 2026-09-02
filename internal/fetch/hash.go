package fetch

import (
	"crypto/md5"
	"crypto/sha1"
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
// md5 and sha1 are supported for integrity verification of third-party
// datasets (Zenodo/4TU/Planck publish them); they are not collision
// resistant, so use sha256/sha512 when tamper resistance matters.
func newHash(algo string) hash.Hash {
	switch algo {
	case "sha512":
		return sha512.New()
	case "sha1":
		return sha1.New()
	case "md5":
		return md5.New()
	default:
		return sha256.New()
	}
}

// hashSize returns the digest size for the given algorithm.
func hashSize(algo string) int {
	switch algo {
	case "sha512":
		return sha512.Size
	case "sha1":
		return sha1.Size
	case "md5":
		return md5.Size
	default:
		return sha256.Size
	}
}

// ParseHashFlag parses "-h sha256:abcdef...", "-h md5:...", etc.
// Also accepts bare hex, inferring the algorithm from its length
// (32 = md5, 40 = sha1, 64 = sha256, 128 = sha512), consistent with
// sidecar inference. Returns (algo, hex, error).
func ParseHashFlag(s string) (algo, hexHash string, err error) {
	if s == "" {
		return "", "", nil
	}
	// Check for "algo:hex" format
	if idx := strings.IndexByte(s, ':'); idx > 0 {
		a := strings.ToLower(s[:idx])
		h := s[idx+1:]
		switch a {
		case "sha256", "sha512", "sha1", "md5":
		default:
			return "", "", fmt.Errorf("unsupported hash algorithm %q (use sha256, sha512, sha1, or md5)", a)
		}
		if err := validateHexHash(h, a); err != nil {
			return "", "", err
		}
		return a, h, nil
	}
	// Bare hex — infer the algorithm from the length.
	for _, algo := range []string{"md5", "sha1", "sha256", "sha512"} {
		if len(s) == hashSize(algo)*2 {
			if err := validateHexHash(s, algo); err != nil {
				return "", "", err
			}
			return algo, s, nil
		}
	}
	return "", "", fmt.Errorf("expected 32 (md5), 40 (sha1), 64 (sha256), or 128 (sha512) hex characters, got %d", len(s))
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
