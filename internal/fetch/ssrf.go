package fetch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

// AllowLoopbackDial, when true, permits dials to loopback addresses.
// Production stays false; unit tests set it so httptest can bind 127.0.0.1.
// Never enable this for the CLI entrypoint.
var AllowLoopbackDial bool

// HostIsPrivate reports whether hostname resolves to a private, loopback,
// link-local, multicast, or unspecified IP. DNS failure → true (fail closed).
// Loopback is treated as private unless AllowLoopbackDial is set (tests).
func HostIsPrivate(hostname string) bool {
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return true
	}
	for _, ip := range ips {
		if ipIsBlocked(ip) {
			return true
		}
	}
	return false
}

func ipIsBlocked(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}
	if ip.IsLoopback() {
		return !AllowLoopbackDial
	}
	return ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified()
}

// CheckRedirectSafe rejects redirects to private/internal hosts and
// bounds hop count. Use as http.Client.CheckRedirect.
func CheckRedirectSafe(req *http.Request, via []*http.Request) error {
	if len(via) >= 3 {
		return errors.New("too many redirects")
	}
	u := req.URL
	if u == nil {
		return errors.New("redirect missing URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("redirect to unsupported scheme %q", u.Scheme)
	}
	if HostIsPrivate(u.Hostname()) {
		return fmt.Errorf("redirect to private/internal host %q blocked", u.Hostname())
	}
	return nil
}

// DialContextSafe resolves the host, rejects private IPs, and dials the
// first allowed IP literal so a later DNS rebind cannot pivot the TCP
// connection into an internal host.
func DialContextSafe(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	var last error
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 10 * time.Second}
	d.Control = func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpFastOpen, 1)
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, tcpNotSentLowatV)
			tcpKeepalive(fd)
		})
	}
	for _, ipa := range ips {
		if ipIsBlocked(ipa.IP) {
			last = fmt.Errorf("resolved private/internal IP %s for %s", ipa.IP, host)
			continue
		}
		target := net.JoinHostPort(ipa.IP.String(), port)
		conn, err := d.DialContext(ctx, network, target)
		if err == nil {
			return conn, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("no routable addresses for %s", host)
	}
	return nil, last
}

// DialContextAllowPrivate is like DialContextSafe but allows private IPs.
// Used when connecting to a proxy host (trusted intermediary).
func DialContextAllowPrivate(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	var last error
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 10 * time.Second}
	d.Control = func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpFastOpen, 1)
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, tcpNotSentLowatV)
			tcpKeepalive(fd)
		})
	}
	for _, ipa := range ips {
		target := net.JoinHostPort(ipa.IP.String(), port)
		conn, err := d.DialContext(ctx, network, target)
		if err == nil {
			return conn, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("no routable addresses for %s", host)
	}
	return nil, last
}

// NewSafeClient returns an HTTP client with SSRF-hardened dial and redirects.
// timeout covers the entire request (use for short sidecar fetches).
func NewSafeClient(timeout time.Duration) *http.Client {
	ac := AutoConfigure(0)
	t := newAutoTransport(ac)
	t.DialContext = DialContextSafe
	return &http.Client{
		Timeout:       timeout,
		Transport:     t,
		CheckRedirect: CheckRedirectSafe,
	}
}
