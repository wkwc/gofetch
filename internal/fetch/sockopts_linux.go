//go:build linux

package fetch

import "syscall"

// tuneFD applies aggressive TCP tuning to a connected socket fd (Linux only).
// All SetsockoptInt errors are ignored:Unsupported Linux kernels silently
// return EINVAL and we treat that as best-effort (the connection itself has
// already been established by the time Control runs).
func tuneFD(fd uintptr) error {
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpFastOpen, 1)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, tcpNotSentLowatV)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 10)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)
	return nil
}
