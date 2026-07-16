package fetch

import (
	"crypto/sha256"
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
		if got := humanBytes(tt.input); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
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
