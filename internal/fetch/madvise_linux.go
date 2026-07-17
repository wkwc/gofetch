//go:build linux

package fetch

import (
	"syscall"
	"unsafe"
)

const (
	madvWillneed   = 3 // MADV_WILLNEED
	madvSequential = 2
	madvHugepage   = 14
)

// prefaultMmap tells the kernel to prep all pages of b into the
// page cache before we start writing to them. On Linux this is
// MADV_WILLNEED, which forces page allocation without copy.
func prefaultMmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(madvWillneed),
	)
	return e
}

// hintMmapSequential tells the kernel we'll write/read mmap'd memory
// linearly. Reduces readahead state hardware doesn't need.
func hintMmapSequential(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(madvSequential),
	)
	return e
}
