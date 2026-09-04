package fetch

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.00 GB"},
		{-1, "-1 B"},
	}
	for _, tt := range tests {
		if got := HumanBytes(tt.input); got != tt.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestHexEqual(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"abcdef", "ABCDEF", true},
		{"aB0c", "ab0c", true},
		{"abcdef", "abcd00", false},
		{"abc", "abcd", false},
		{"", "", true},
		{"", "a", false},
	}
	for _, tt := range tests {
		if got := hexEqual(tt.a, tt.b); got != tt.want {
			t.Errorf("hexEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestVerifyFileHash(t *testing.T) {
	dir := t.TempDir()
	hash := sha256.Sum256([]byte("hello world\n"))
	want := hex.EncodeToString(hash[:])

	path := filepath.Join(dir, "hw.txt")
	if err := os.WriteFile(path, []byte("hello world\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := verifyFileHash(path, "sha256", want); err != nil {
		t.Errorf("verifyFileHash match: %v", err)
	}
	// empty expected → no-op
	if err := verifyFileHash(path, "sha256", ""); err != nil {
		t.Errorf("verifyFileHash empty: %v", err)
	}
	// wrong hash
	if err := verifyFileHash(path, "sha256", strings.Repeat("0", 64)); err == nil {
		t.Error("verifyFileHash wrong hash: expected error, got nil")
	}
}

func TestParseHashFlag(t *testing.T) {
	sha256Hex := strings.Repeat("ab", 32) // 64 hex chars
	sha512Hex := strings.Repeat("cd", 64) // 128 hex chars
	sha1Hex := strings.Repeat("ef", 20)   // 40 hex chars
	md5Hex := strings.Repeat("12", 16)    // 32 hex chars

	tests := []struct {
		name    string
		input   string
		algo    string
		hexHash string
		wantErr bool
	}{
		{name: "empty", input: "", algo: "", hexHash: ""},
		{name: "bare sha256 hex", input: sha256Hex, algo: "sha256", hexHash: sha256Hex},
		{name: "bare sha512 hex", input: sha512Hex, algo: "sha512", hexHash: sha512Hex},
		{name: "bare sha1 hex", input: sha1Hex, algo: "sha1", hexHash: sha1Hex},
		{name: "bare md5 hex", input: md5Hex, algo: "md5", hexHash: md5Hex},
		{name: "algo:hex md5", input: "md5:" + md5Hex, algo: "md5", hexHash: md5Hex},
		{name: "algo:hex sha1", input: "sha1:" + sha1Hex, algo: "sha1", hexHash: sha1Hex},
		{name: "algo:hex sha256", input: "sha256:" + sha256Hex, algo: "sha256", hexHash: sha256Hex},
		{name: "algo:hex sha512", input: "sha512:" + sha512Hex, algo: "sha512", hexHash: sha512Hex},
		{name: "uppercase algo", input: "SHA256:" + sha256Hex, algo: "sha256", hexHash: sha256Hex},
		{name: "unsupported algo", input: "md5:" + sha256Hex, wantErr: true},
		{name: "wrong length sha256", input: "abcd", wantErr: true},
		{name: "invalid hex chars", input: "zzzz" + strings.Repeat("ab", 30), wantErr: true},
		{name: "invalid hex length (34)", input: strings.Repeat("ab", 17), wantErr: true},
		{name: "wrong length for sha256 (too long)", input: strings.Repeat("ab", 48), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo, hexHash, err := ParseHashFlag(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got algo=%q hex=%q", algo, hexHash)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if algo != tt.algo {
				t.Errorf("algo = %q, want %q", algo, tt.algo)
			}
			if hexHash != tt.hexHash {
				t.Errorf("hexHash = %q, want %q", hexHash, tt.hexHash)
			}
		})
	}
}

func TestHashSize(t *testing.T) {
	if got := hashSize("sha256"); got != sha256.Size {
		t.Errorf("hashSize(sha256) = %d, want %d", got, sha256.Size)
	}
	if got := hashSize("sha512"); got != sha512.Size {
		t.Errorf("hashSize(sha512) = %d, want %d", got, sha512.Size)
	}
	if got := hashSize("unknown"); got != sha256.Size {
		t.Errorf("hashSize(unknown) = %d, want %d (default sha256)", got, sha256.Size)
	}
	if got := hashSize(""); got != sha256.Size {
		t.Errorf("hashSize('') = %d, want %d (default sha256)", got, sha256.Size)
	}
}

func TestValidateHexHash(t *testing.T) {
	valid256 := strings.Repeat("a", 64)
	valid512 := strings.Repeat("b", 128)

	if err := validateHexHash(valid256, "sha256"); err != nil {
		t.Errorf("valid sha256: %v", err)
	}
	if err := validateHexHash(valid512, "sha512"); err != nil {
		t.Errorf("valid sha512: %v", err)
	}
	if err := validateHexHash("zzzz", "sha256"); err == nil {
		t.Error("invalid hex: expected error")
	}
	if err := validateHexHash(valid256, "sha512"); err == nil {
		t.Error("wrong length for sha512: expected error")
	}
}

// TestAlgoInferenceConsistency verifies every hash parser and helper
// agrees on the algorithm for the same hex: algoForLen, ParseHashFlag
// (bare and algo:hex), ParseSidecarContent, and hashSize must all
// infer identical results.
func TestAlgoInferenceConsistency(t *testing.T) {
	for _, algo := range []string{algoMD5, algoSHA1, algoSHA256, algoSHA512} {
		hex := strings.Repeat("a", hashSize(algo)*2)

		got, ok := algoForLen(hex)
		if !ok || got != algo {
			t.Errorf("algoForLen(%d) = %s/%v, want %s", len(hex), got, ok, algo)
		}

		a1, h1, err := ParseHashFlag(hex)
		if err != nil || a1 != algo || h1 != hex {
			t.Errorf("ParseHashFlag(bare %s) = %s/%s/%v", algo, a1, h1, err)
		}

		a2, _, err := ParseHashFlag(algo + ":" + hex)
		if err != nil || a2 != algo {
			t.Errorf("ParseHashFlag(%s:hex) = %s/%v", algo, a2, err)
		}

		a3, h3, err := ParseSidecarContent(hex+"  f.bin\n", "f")
		if err != nil || a3 != algo || h3 != hex {
			t.Errorf("ParseSidecarContent(%s) = %s/%s/%v", algo, a3, h3, err)
		}

		if len(hex) != hashSize(algo)*2 {
			t.Errorf("hashSize(%s) length mismatch", algo)
		}
	}
}
