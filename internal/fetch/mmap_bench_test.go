package fetch

import (
	"os"
	"syscall"
	"testing"
)

// BenchmarkMmapSingle measures the cost of a single mmap/unmap cycle on
// a small file. This is the per-download overhead that mmapWriter
// incurs uniquely.
func BenchmarkMmapSingle(b *testing.B) {
	tmp := b.TempDir()
	path := tmp + "/mmap.bench"

	const size int64 = 16 * 1024 * 1024
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var fdUintptr uintptr
		fd, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			b.Fatal(err)
		}
		fdUintptr = fd.Fd()
		data, err := syscall.Mmap(int(fdUintptr), 0, int(size),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_SHARED)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
		if err := syscall.Munmap(data); err != nil {
			b.Fatal(err)
		}
		fd.Close()
	}
}
