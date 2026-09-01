//go:build !linux

package fetch

// tuneFD is a no-op on non-Linux platforms: TCP_FASTOPEN, NOTSENT_LOWAT, and
// Linux-specific TCP_KEEP* constants are unavailable there. TCP_NODELAY is
// already applied via the Dialer.KeepAlive field above, and we do not rely
// on TCP keepalive probes for idle-stream detection (see idleBody).
func tuneFD(_ uintptr) {}
