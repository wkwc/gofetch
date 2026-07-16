package fetch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManifestVerifyChunk(t *testing.T) {
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks: []ChunkHash{
			{Start: 0, End: 9, Hash: sha256Hex([]byte("hello"))},
			{Start: 10, End: 19, Hash: sha256Hex([]byte("world"))},
		},
	}

	if err := m.VerifyChunk(0, 9, []byte("hello")); err != nil {
		t.Errorf("VerifyChunk(0,9,hello) = %v, want nil", err)
	}
	if err := m.VerifyChunk(10, 19, []byte("world")); err != nil {
		t.Errorf("VerifyChunk(10,19,world) = %v, want nil", err)
	}
	if err := m.VerifyChunk(0, 9, []byte("wrong")); err == nil {
		t.Error("VerifyChunk with wrong data should fail")
	}
	if err := m.VerifyChunk(100, 199, []byte("anything")); err != nil {
		t.Errorf("VerifyChunk for non-existent chunk = %v, want nil (advisory)", err)
	}
	if err := (*Manifest)(nil).VerifyChunk(0, 9, []byte("hello")); err != nil {
		t.Errorf("nil manifest = %v, want nil", err)
	}
}

func TestManifestVerifyFull(t *testing.T) {
	payload := []byte("hello world this is a test")
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks: []ChunkHash{
			{Start: 0, End: 10, Hash: sha256Hex(payload[:11])},
			{Start: 11, End: 25, Hash: sha256Hex(payload[11:])},
		},
	}

	tmpFile := filepath.Join(t.TempDir(), "manifest_test.bin")
	if err := os.WriteFile(tmpFile, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := m.VerifyFull(tmpFile); err != nil {
		t.Errorf("VerifyFull correct file = %v, want nil", err)
	}

	if err := os.WriteFile(tmpFile, []byte("corrupted"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.VerifyFull(tmpFile); err == nil {
		t.Error("VerifyFull corrupted file = nil, want error")
	}
}