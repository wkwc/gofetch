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

// TestManifestVerifyRangeMultiChunk exercises a task spanning multiple
// manifest chunks: VerifyRange must verify every fully-contained chunk
// in [start, end] and skip partial overlaps on the right edge.
func TestManifestVerifyRangeMultiChunk(t *testing.T) {
	payload := []byte("the quick brown fox jumps over the lazy dog")
	// 4 chunks of length 10 each (indices 0-9, 10-19, 20-29, 30-39),
	// tail 40.. ignored by the manifest.
	c := func(s, e int) ChunkHash {
		return ChunkHash{Start: int64(s), End: int64(e), Hash: sha256Hex(payload[s : e+1])}
	}
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks:  []ChunkHash{c(0, 9), c(10, 19), c(20, 29), c(30, 39)},
	}
	// Range 5-34 fully contains chunks 10-19, 20-29 and partially
	// overlaps 0-9 (left) and 30-39 (right). Only 10-29 verify here.
	data := payload[5:35]
	if err := m.VerifyRange(5, 34, data); err != nil {
		t.Errorf("VerifyRange(5,34) = %v, want nil (contained chunks match)", err)
	}
	// Corrupt the bytes that chunk 10-19 maps to (offset 5 in data).
	bad := append([]byte(nil), data...)
	copy(bad[5:15], "xxxxxxxxxx")
	if err := m.VerifyRange(5, 34, bad); err == nil {
		t.Error("VerifyRange with corrupted contained chunk should fail")
	}
}

// TestManifestVerifyRangeUnsortedInput locks in the buildIndex sort:
// an unsorted Chunks slice must still verify correctly because
// VerifyRange binary-searches the (post-sort) slice.
func TestManifestVerifyRangeUnsortedInput(t *testing.T) {
	payload := []byte("0123456789ABCDEFGHIJ")
	c := func(s, e int) ChunkHash {
		return ChunkHash{Start: int64(s), End: int64(e), Hash: sha256Hex(payload[s : e+1])}
	}
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		// Deliberately reverse order.
		Chunks: []ChunkHash{c(10, 19), c(0, 9)},
	}
	if err := m.VerifyRange(0, 19, payload); err != nil {
		t.Errorf("VerifyRange on unsorted manifest = %v, want nil", err)
	}
	// Verify via the index fast-path (exact boundary) as well.
	if err := m.VerifyChunk(0, 9, payload[0:10]); err != nil {
		t.Errorf("VerifyChunk(0,9) on unsorted manifest = %v", err)
	}
	if err := m.VerifyChunk(10, 19, payload[10:20]); err != nil {
		t.Errorf("VerifyChunk(10,19) on unsorted manifest = %v", err)
	}
}

// TestManifestBadChunks exercises the BadChunks localization used by the
// finalize surgical-trim path: only the chunks whose on-disk bytes hash
// mismatch are returned, while intact chunks are skipped.
func TestManifestBadChunks(t *testing.T) {
	payload := []byte("thequickbrownfo") // 15 bytes; chunks 0-4,5-9,10-14
	c := func(s, e int) ChunkHash {
		return ChunkHash{Start: int64(s), End: int64(e), Hash: sha256Hex(payload[s : e+1])}
	}
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks:  []ChunkHash{c(0, 4), c(5, 9), c(10, 14)},
	}
	tmp := filepath.Join(t.TempDir(), "badchunks.bin")
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	// All intact → no bad.
	if bad := m.BadChunks(tmp); len(bad) != 0 {
		t.Errorf("BadChunks on intact file = %d, want 0", len(bad))
	}
	// Corrupt the middle chunk (bytes 5-9).
	corrupt := append([]byte(nil), payload...)
	copy(corrupt[5:10], "ZZZZZ")
	if err := os.WriteFile(tmp, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	bad := m.BadChunks(tmp)
	if len(bad) != 1 {
		t.Fatalf("BadChunks = %d entries, want 1: %+v", len(bad), bad)
	}
	if bad[0].Start != 5 || bad[0].End != 9 {
		t.Errorf("bad chunk = %d-%d, want 5-9", bad[0].Start, bad[0].End)
	}
	// Corrupt the first chunk too → two bad.
	if err := os.WriteFile(tmp, append([]byte("XXXXX"), corrupt[5:]...), 0o644); err != nil {
		t.Fatal(err)
	}
	bad = m.BadChunks(tmp)
	if len(bad) != 2 {
		t.Fatalf("BadChunks = %d entries, want 2: %+v", len(bad), bad)
	}
}

// TestManifestBadChunksTruncatedFile exercises the conservative policy
// on a truncated output: chunks past the readable tail are reported
// bad (so the resume sidecar drops them and forces a re-fetch), not
// silently skipped.
func TestManifestBadChunksTruncatedFile(t *testing.T) {
	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes
	c := func(s, e int) ChunkHash {
		return ChunkHash{Start: int64(s), End: int64(e), Hash: sha256Hex(payload[s : e+1])}
	}
	m := &Manifest{
		Version: 1,
		Algo:    "sha256",
		Chunks:  []ChunkHash{c(0, 4), c(5, 9), c(10, 14), c(15, 19)},
	}
	// Keep only the first 12 bytes — chunk 2 (10-14) reads short, chunk 3 (15-19) is past EOF.
	tru := filepath.Join(t.TempDir(), "tru.bin")
	_ = os.WriteFile(tru, payload[:12], 0o644)
	bad := m.BadChunks(tru)
	if len(bad) != 2 {
		t.Fatalf("BadChunks on truncated file = %d, want 2 (chunks 10-14 and 15-19): %+v", len(bad), bad)
	}
}
