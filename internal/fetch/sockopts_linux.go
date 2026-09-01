//go:build linux

package fetch

import "syscall"

// tuneFD applies aggressive TCP tuning to a connected socket fd (Linux only).
// All SetsockoptInt errors are ignored: unsupported Linux kernels silently
// return EINVAL and we treat that as best-effort (the connection itself has
// already been established by the time Control runs).
func tuneFD(fd uintptr) {
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpFastOpen, 1)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, tcpNotSentLowatV)
	// TCP_QUICKACK (1) re-enables quick ACKs after a burst of data-only
	// segments; for a pure-download socket it keeps ACKs from being
	// delayed behind the Nagle-suppressed writes above.
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 10)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)
}
