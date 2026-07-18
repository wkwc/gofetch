//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package fetch

import "fmt"

// mmapSys is unsupported on this platform.
func mmapSys(_ uintptr, _ int) ([]byte, error) {
	return nil, fmt.Errorf("mmap not supported on this platform")
}

// munmapSys is unsupported on this platform.
func munmapSys(_ []byte) error {
	return fmt.Errorf("munmap not supported on this platform")
}

// hintSequential is a no-op on non-Linux platforms.
func hintSequential(_ []byte) error { return nil }

// tcpSetKeepalive is a no-op on non-Linux platforms.
func tcpSetKeepalive(_ uintptr) {}
