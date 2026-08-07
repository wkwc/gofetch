package fetch

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// ManifestVersion is the current manifest format version.
const ManifestVersion = 1

// Manifest holds per-chunk hashes for piece-level integrity.
type Manifest struct {
	Version int                        `json:"version"`
	Algo    string                     `json:"algo"`
	Chunks  []ChunkHash                `json:"chunks"`
	index   map[int64]map[int64]string // start -> end -> hash (built on load)
	once    sync.Once                  // guards lazy buildIndex
}

// ChunkHash maps a byte range to its expected hash.
type ChunkHash struct {
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	Hash  string `json:"hash"`
}

// LoadManifest reads a .gofetch.manifest JSON file.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if m.Version < 1 || m.Version > ManifestVersion {
		return nil, fmt.Errorf("manifest: unsupported version %d", m.Version)
	}
	if m.Algo == "" {
		m.Algo = "sha256"
	}
	// Validate chunk geometry: discard any chunk where End < Start.
	// A corrupt manifest with inverted ranges would cause integer
	// underflow in VerifyFull, leading to near-infinite read loops.
	valid := m.Chunks[:0]
	for _, c := range m.Chunks {
		if c.End >= c.Start {
			valid = append(valid, c)
		}
	}
	m.Chunks = valid
	m.buildIndex()
	return &m, nil
}

// VerifyChunk checks that data matches the expected hash for [start, end].
// Returns nil if no matching chunk entry exists (manifest is advisory).
// Prefer VerifyRange when the download buffer is smaller than the chunk.
func (m *Manifest) VerifyChunk(start, end int64, data []byte) error {
	if m == nil {
		return nil
	}
	m.buildIndex()
	if endMap, ok := m.index[start]; ok {
		if expected, ok := endMap[end]; ok {
			return verifyHash(data, m.Algo, expected)
		}
	}
	return nil
}

// VerifyRange verifies every manifest chunk that is fully contained in
// [start, end] by hashing the corresponding slice of data. data must be
// the complete bytes for that range (data[0] == file byte at start).
// Partial overlaps are skipped (verified later by VerifyFull).
//
// The naive implementation scanned all M chunks linearly for each task,
// making post-task verification O(N·M) overall. Chunks are kept sorted
// by Start (buildIndex), so binary-search to the first chunk with
// Start >= start and iterate only while Start <= end — turning the hot
// path into O(log M + k) where k is the contained count.
func (m *Manifest) VerifyRange(start, end int64, data []byte) error {
	if m == nil {
		return nil
	}
	want := end - start + 1
	if int64(len(data)) < want {
		return fmt.Errorf("manifest: VerifyRange short buffer: have %d want %d", len(data), want)
	}
	m.buildIndex()
	chunks := m.Chunks
	// Find first chunk with Start >= start. sort.Search semantics.
	lo, hi := 0, len(chunks)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if chunks[mid].Start < start {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	for i := lo; i < len(chunks); i++ {
		c := chunks[i]
		if c.Start > end {
			break
		}
		// c.Start >= start (binary-search precondition) and c.Start <= end.
		// Fully contained iff c.End <= end; skip partial overlaps (left
		// edge aligned but right edge past end) — they are covered by VerifyFull.
		if c.End > end {
			continue
		}
		off := c.Start - start
		size := c.End - c.Start + 1
		if err := verifyHash(data[off:off+size], m.Algo, c.Hash); err != nil {
			return fmt.Errorf("manifest: chunk %d-%d: %w", c.Start, c.End, err)
		}
	}
	return nil
}

// VerifyFull reads path and verifies every chunk in the manifest.
func (m *Manifest) VerifyFull(path string) error {
	if m == nil {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	const bufSize = 256 * 1024
	buf := make([]byte, bufSize)
	for _, c := range m.Chunks {
		size := c.End - c.Start + 1
		if _, err := f.Seek(c.Start, io.SeekStart); err != nil {
			return fmt.Errorf("manifest: seek %d: %w", c.Start, err)
		}
		h := newHash(m.Algo)
		remaining := size
		for remaining > 0 {
			n := int64(bufSize)
			if n > remaining {
				n = remaining
			}
			if _, err := io.ReadFull(f, buf[:n]); err != nil {
				return fmt.Errorf("manifest: read %d-%d: %w", c.Start, c.End, err)
			}
			h.Write(buf[:n])
			remaining -= n
		}
		got := hex.EncodeToString(h.Sum(nil))
		if !hexEqual(got, c.Hash) {
			return fmt.Errorf("manifest: chunk %d-%d hash mismatch: expected %s, got %s",
				c.Start, c.End, c.Hash, got)
		}
	}
	return nil
}

// BadChunks reads path and returns every manifest chunk whose on-disk
// bytes do not match the expected hash. Used by finalize (on a
// VerifyFull failure with a manifest) to surgically trim the resume
// sidecar: only the corrupt byte ranges are dropped from `completed`
// so the next run re-fetches just those, rather than re-downloading
// the entire file. Returns nil when the manifest is nil or whenever
// the file cannot even be opened (caller falls back to a clear).
func (m *Manifest) BadChunks(path string) []ChunkHash {
	if m == nil {
		return nil
	}
	m.buildIndex()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	const bufSize = 256 * 1024
	buf := make([]byte, bufSize)
	var bad []ChunkHash
	for _, c := range m.Chunks {
		size := c.End - c.Start + 1
		if _, err := f.Seek(c.Start, io.SeekStart); err != nil {
			// A seek/read error against one chunk: be conservative and
			// report it as bad so the resume sidecar drops it.
			bad = append(bad, c)
			continue
		}
		h := newHash(m.Algo)
		remaining := size
		readErr := false
		for remaining > 0 {
			n := int64(bufSize)
			if n > remaining {
				n = remaining
			}
			if _, err := io.ReadFull(f, buf[:n]); err != nil {
				bad = append(bad, c)
				readErr = true
				break
			}
			h.Write(buf[:n])
			remaining -= n
		}
		if readErr {
			continue
		}
		got := hex.EncodeToString(h.Sum(nil))
		if !hexEqual(got, c.Hash) {
			bad = append(bad, c)
		}
	}
	return bad
}

func verifyHash(data []byte, algo, expected string) error {
	h := newHash(algo)
	h.Write(data)
	got := hex.EncodeToString(h.Sum(nil))
	if !hexEqual(got, expected) {
		return fmt.Errorf("hash mismatch (%s): expected %s, got %s", algo, expected, got)
	}
	return nil
}

// buildIndex builds the O(1) lookup map for chunks and (once) sorts
// the Chunks slice by Start so VerifyRange can binary-search. Called
// eagerly from LoadManifest and lazily from VerifyChunk via sync.Once.
func (m *Manifest) buildIndex() {
	m.once.Do(func() {
		// Sort by Start (stable) so VerifyRange's binary search is valid.
		// VerifyFull is order-independent (it seeks per chunk), so the
		// reorder is safe for it too. Manifests that happen to be pre-
		// sorted pay only a single O(n) check.
		sortBefore := false
		for i := 1; i < len(m.Chunks); i++ {
			if m.Chunks[i].Start < m.Chunks[i-1].Start {
				sortBefore = true
				break
			}
		}
		if sortBefore {
			// In-place stable sort by Start; preserves relative order of
			// equal-Start chunks (which the index dedups by End anyway).
			sort.SliceStable(m.Chunks, func(i, j int) bool {
				return m.Chunks[i].Start < m.Chunks[j].Start
			})
		}
		m.index = make(map[int64]map[int64]string, len(m.Chunks))
		for _, c := range m.Chunks {
			if m.index[c.Start] == nil {
				m.index[c.Start] = make(map[int64]string)
			}
			m.index[c.Start][c.End] = c.Hash
		}
	})
}
