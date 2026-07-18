//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package fetch

import (
	"syscall"
	"unsafe"
)

// mmapSys maps the fd into process memory.
func mmapSys(fd uintptr, size int) ([]byte, error) {
	return syscall.Mmap(int(fd), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
}

// munmapSys unmaps a previously mmap'd region.
func munmapSys(data []byte) error {
	return syscall.Munmap(data)
}

// hintSequential tells the kernel to optimize for sequential access.
// Uses madvise(MADV_SEQUENTIAL); falls back to no-op on error.
func hintSequential(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall6(
		syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		2, // MADV_SEQUENTIAL — value is 2 on Linux, macOS, and all BSDs.
		0, 0, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}
