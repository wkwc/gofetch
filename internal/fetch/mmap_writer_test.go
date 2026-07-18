package fetch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewMmapWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	mw, err := newMmapWriter(path, 4096)
	if err != nil {
		t.Fatalf("newMmapWriter: %v", err)
	}
	defer mw.Close()

	if mw.size != 4096 {
		t.Errorf("size = %d, want 4096", mw.size)
	}
	if len(mw.data) != 4096 {
		t.Errorf("data len = %d, want 4096", len(mw.data))
	}
}

func TestNewMmapWriterInvalidSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	if _, err := newMmapWriter(path, 0); err == nil {
		t.Error("expected error for size=0")
	}
	if _, err := newMmapWriter(path, 1<<62+1); err == nil {
		t.Error("expected error for size > 1<<62")
	}
}

func TestMmapWriterWriteAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	mw, err := newMmapWriter(path, 4096)
	if err != nil {
		t.Fatalf("newMmapWriter: %v", err)
	}
	defer mw.Close()

	data := []byte("hello, mmap!")
	n, err := mw.WriteAt(data, 100)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != len(data) {
		t.Errorf("n = %d, want %d", n, len(data))
	}

	// Verify bytes were written to the mmap'd region
	for i, b := range data {
		if mw.data[100+i] != b {
			t.Errorf("data[%d] = %d, want %d", 100+i, mw.data[100+i], b)
		}
	}
}

func TestMmapWriterWriteAtOOB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	mw, err := newMmapWriter(path, 64)
	if err != nil {
		t.Fatalf("newMmapWriter: %v", err)
	}
	defer mw.Close()

	_, err = mw.WriteAt(make([]byte, 10), 60) // 60+10 > 64
	if err == nil {
		t.Error("expected out-of-bounds error")
	}
}

func TestMmapWriterBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	mw, err := newMmapWriter(path, 1024)
	if err != nil {
		t.Fatalf("newMmapWriter: %v", err)
	}
	defer mw.Close()

	if mw.Bytes() == nil {
		t.Error("expected non-nil Bytes()")
	}
	if len(mw.Bytes()) != 1024 {
		t.Errorf("Bytes() len = %d, want 1024", len(mw.Bytes()))
	}
}

func TestRawFileWriterBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	rfw := &rawFileWriter{F: f}
	if rfw.Bytes() != nil {
		t.Error("rawFileWriter.Bytes() should return nil")
	}
}

func TestMmapWriterCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	mw, err := newMmapWriter(path, 4096)
	if err != nil {
		t.Fatalf("newMmapWriter: %v", err)
	}

	// First close
	if err := mw.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	// Second close should be safe (data is nil)
	if err := mw.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestAllocateFileWriterSmallFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.bin")

	fw, err := allocateFileWriter(path, 0, false)
	if err != nil {
		t.Fatalf("allocateFileWriter: %v", err)
	}
	defer fw.Close()

	// Size 0 → raw file writer
	if _, ok := fw.(*rawFileWriter); !ok {
		t.Error("expected rawFileWriter for size=0")
	}
}

func TestAllocateFileWriterWithMmap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")

	fw, err := allocateFileWriter(path, 4096, false)
	if err != nil {
		t.Fatalf("allocateFileWriter: %v", err)
	}
	defer fw.Close()

	// Size > 0 → should be mmap writer
	if _, ok := fw.(*mmapWriter); !ok {
		t.Error("expected mmapWriter for size > 0")
	}
}
