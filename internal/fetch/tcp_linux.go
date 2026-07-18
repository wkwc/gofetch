//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package fetch

import "syscall"

// tcpKeepalive sets aggressive keepalive on a raw TCP fd (Linux-only
// constants). On other Unixes this is a best-effort no-op per constant.
func tcpKeepalive(fd uintptr) {
	// TCP_KEEPIDLE/INTVL/CNT are Linux-specific; silently ignored on
	// other platforms where these constants don't exist.
	syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 10) //nolint:errcheck
	syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3) //nolint:errcheck
	syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)   //nolint:errcheck
}
