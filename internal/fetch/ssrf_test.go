package fetch

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestDialContextSafeBlocksPrivate(t *testing.T) {
	// Save and restore the test default (httptest needs loopback).
	prev := AllowLoopbackDial
	AllowLoopbackDial = false
	t.Cleanup(func() { AllowLoopbackDial = prev })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

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
	prev := AllowLoopbackDial
	AllowLoopbackDial = false
	t.Cleanup(func() { AllowLoopbackDial = prev })

	// Connection will fail (nothing listening on :1) but the error must
	// not be the private-IP reject.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := DialContextAllowPrivate(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "private/internal") {
		t.Fatalf("AllowPrivate should not reject loopback: %v", err)
	}
}

func TestIpIsBlocked(t *testing.T) {
	prev := AllowLoopbackDial
	AllowLoopbackDial = false
	t.Cleanup(func() { AllowLoopbackDial = prev })

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
