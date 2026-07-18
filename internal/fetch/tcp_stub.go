//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package fetch

// tcpKeepalive is a no-op on non-Unix platforms.
func tcpKeepalive(_ uintptr) {}
