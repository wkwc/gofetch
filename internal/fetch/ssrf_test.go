package fetch

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestDialContextSafeBlocksPrivate(t *testing.T) {
	// Save and restore the test default (httptest needs loopback).
	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })

	ctx := testCtx(t, 2*time.Second)

	_, err := DialContextSafe(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
	if err == nil {
		t.Fatal("expected DialContextSafe to reject loopback, got nil error")
	}

	_, err = DialContextSafe(ctx, "tcp", net.JoinHostPort("10.0.0.1", "1"))
	if err == nil {
		t.Fatal("expected DialContextSafe to reject RFC1918, got nil error")
	}
	if !strings.Contains(err.Error(), "private/internal") {
		t.Fatalf("expected private/internal reject, got: %v", err)
	}
}

func TestDialContextAllowPrivateAcceptsLoopback(t *testing.T) {
	// Even with AllowLoopbackDial=false, the allow-private dialer must
	// not filter loopback — proxies on 127.0.0.1 rely on this.
	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })

	// Connection will fail (nothing listening on :1) but the error must
	// not be the private-IP reject.
	ctx := testCtx(t, 500*time.Millisecond)
	_, err := DialContextAllowPrivate(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "private/internal") {
		t.Fatalf("AllowPrivate should not reject loopback: %v", err)
	}
}

func TestIpIsBlocked(t *testing.T) {
	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })

	cases := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},
		{"10.1.2.3", true},
		{"192.168.0.1", true},
		{"169.254.1.1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
	}
	for _, tt := range cases {
		ip := net.ParseIP(tt.ip)
		if ip == nil {
			t.Fatalf("ParseIP(%q)", tt.ip)
		}
		if got := ipIsBlocked(ip); got != tt.want {
			t.Errorf("ipIsBlocked(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}

// resetProxyHosts empties the global proxy allowlist so tests that call
// allowProxyHost do not leak entries into later runs (e.g. -count=N).
func resetProxyHosts() {
	proxyHostsOnce.Do(loadEnvProxyHosts)
	proxyHosts = map[string]struct{}{}
}

// TestAllowProxyHost verifies an explicit --proxy host (which may be a
// private local proxy) becomes a trusted intermediary for dialing, so
// DialContextAuto does not SSRF-block it.
func TestAllowProxyHost(t *testing.T) {
	resetProxyHosts()
	host := "proxy.example.invalid"
	if envProxyHost(host + ":8080") {
		t.Fatalf("precondition: %q should not be a known proxy", host)
	}
	allowProxyHost(host)
	if !envProxyHost(host + ":8080") {
		t.Errorf("allowProxyHost(%q) did not make it a trusted proxy", host)
	}
	if envProxyHost("other.example.invalid:8080") {
		t.Errorf("unrelated host became a trusted proxy")
	}
}

// TestAllowProxyHostDialAuto verifies the full path: after registering an
// explicit proxy host, DialContextAuto tries to connect to it rather than
// rejecting it as private (the connection fails with "refused", not the
// SSRF reject).
func TestAllowProxyHostDialAuto(t *testing.T) {
	prev := AllowLoopbackDial.Load()
	AllowLoopbackDial.Store(false)
	t.Cleanup(func() { AllowLoopbackDial.Store(prev) })
	resetProxyHosts()

	allowProxyHost("127.0.0.1")
	ctx := testCtx(t, 500*time.Millisecond)
	_, err := DialContextAuto(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "private/internal") {
		t.Fatalf("explicit proxy host must not be SSRF-blocked: %v", err)
	}
}
