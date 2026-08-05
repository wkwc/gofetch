package fetch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
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

// tunedDialer returns a Dialer with TCP keepalive / FASTOPEN / NODELAY / NOTSENT_LOWAT.
//
// Per-fd tuning lives in tuneFD (build-tagged per platform) so this file
// stays portable; the Linux variant applies TCP_NODELAY+FASTOPEN+NOTSENT_LOWAT
// + aggressive keepalive, while non-Linux platforms no-op.
func tunedDialer() *net.Dialer {
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 10 * time.Second}
	d.Control = func(network, address string, c syscall.RawConn) error {
		var ctrlErr error
		if err := c.Control(func(fd uintptr) {
			ctrlErr = tuneFD(fd)
		}); err != nil {
			return err
		}
		return ctrlErr
	}
	return d
}

// dialResolved resolves addr, optionally filters blocked IPs, and dials the
// first usable address. blockPrivate=true rejects private/loopback/etc.
func dialResolved(ctx context.Context, network, addr string, blockPrivate bool) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	d := tunedDialer()
	var last error
	for _, ipa := range ips {
		if blockPrivate && ipIsBlocked(ipa.IP) {
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

// DialContextSafe resolves the host, rejects private IPs, and dials the
// first allowed IP literal so a later DNS rebind cannot pivot the TCP
// connection into an internal host.
func DialContextSafe(ctx context.Context, network, addr string) (net.Conn, error) {
	return dialResolved(ctx, network, addr, true)
}

// DialContextAllowPrivate is like DialContextSafe but allows private IPs.
// Used when connecting to a proxy host (trusted intermediary).
func DialContextAllowPrivate(ctx context.Context, network, addr string) (net.Conn, error) {
	return dialResolved(ctx, network, addr, false)
}

// DialContextAuto dials targets with SSRF filtering, but allows private
// addresses when the first hop is an HTTP(S)/ALL proxy from the environment
// (operators commonly run local proxies on 127.0.0.1).
func DialContextAuto(ctx context.Context, network, addr string) (net.Conn, error) {
	if envProxyHost(addr) {
		return DialContextAllowPrivate(ctx, network, addr)
	}
	return DialContextSafe(ctx, network, addr)
}

var (
	proxyHostsOnce sync.Once
	proxyHosts     map[string]struct{}
)

// envProxyHost reports whether hostport matches a proxy configured via
// HTTP_PROXY / HTTPS_PROXY / ALL_PROXY (case-insensitive env names).
func envProxyHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	host = strings.ToLower(host)
	proxyHostsOnce.Do(func() {
		proxyHosts = make(map[string]struct{})
		for _, key := range []string{
			"HTTP_PROXY", "http_proxy",
			"HTTPS_PROXY", "https_proxy",
			"ALL_PROXY", "all_proxy",
		} {
			v := os.Getenv(key)
			if v == "" {
				continue
			}
			// url.Parse needs a scheme; bare "host:port" is common.
			if !strings.Contains(v, "://") {
				v = "http://" + v
			}
			u, err := url.Parse(v)
			if err != nil {
				continue
			}
			if h := strings.ToLower(u.Hostname()); h != "" {
				proxyHosts[h] = struct{}{}
			}
		}
	})
	_, ok := proxyHosts[host]
	return ok
}

// NewSafeClient returns an HTTP client with SSRF-hardened dial and redirects.
// timeout covers the entire request (use for short sidecar fetches).
func NewSafeClient(timeout time.Duration) *http.Client {
	ac := AutoConfigure(0)
	t := newAutoTransport(ac)
	// Sidecar fetches never need a private-IP proxy hop exception beyond
	// DialContextAuto (which already allows env proxy hosts).
	t.DialContext = DialContextAuto
	return &http.Client{
		Timeout:       timeout,
		Transport:     t,
		CheckRedirect: CheckRedirectSafe,
	}
}
