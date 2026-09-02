//go:build linux

package fetch

import "syscall"

// TCP_NOTSENT_LOWAT (0x19) is not exported by the linux syscall
// package as of Go 1.26. The previous 0x17 was TCP_FASTOPEN on
// every arch, so we were setting FASTOPEN twice and never touching
// NOTSENT_LOWAT. Defined here so future kernels can adopt these
// without losing our perf-tuning; on unsupported kernels the
// syscall is silently a no-op via the SetsockoptInt return.
const (
	tcpFastOpen      = 23
	tcpNotSentLowat  = 25
	tcpNotSentLowatV = 131072
)

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
