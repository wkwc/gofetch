package fetch

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseSidecarContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		source  string
		algo    string
		hex     string
		wantErr bool
	}{
		{
			name:    "sha256 with filename",
			content: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789  out.bin\n",
			source:  "file.sha256",
			algo:    "sha256",
			hex:     "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			name:    "sha512 with filename",
			content: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  out.bin\n",
			source:  "file.sha512",
			algo:    "sha512",
			hex:     "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		{
			name:    "bare sha256 hex",
			content: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\n",
			source:  "file",
			algo:    "sha256",
			hex:     "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			name:    "empty content",
			content: "",
			source:  "file.sha256",
			wantErr: true,
		},
		{
			name:    "invalid hex chars",
			content: "zzzz0123456789abcdef0123456789abcdef0123456789abcdef0123456789\n",
			source:  "file.sha256",
			wantErr: true,
		},
		{
			name:    "ambiguous length falls back to extension",
			content: "abcdef0123456789\n",
			source:  "file.sha256",
			// 16 hex chars matches no known algo, so the extension decides.
			algo: "sha256",
			hex:  "abcdef0123456789",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo, hex, err := ParseSidecarContent(tt.content, tt.source)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got algo=%q hex=%q", algo, hex)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if algo != tt.algo {
				t.Errorf("algo = %q, want %q", algo, tt.algo)
			}
			if hex != tt.hex {
				t.Errorf("hex = %q, want %q", hex, tt.hex)
			}
		})
	}
}

func TestIsValidHex(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"abcdef", true},
		{"ABCDEF", true},
		{"0123456789abcdef", true},
		{"xyz", false},
		{"", true},
		{"abcg", false},
	}
	for _, tt := range tests {
		if got := IsValidHex(tt.input); got != tt.want {
			t.Errorf("IsValidHex(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestFetchSidecarHashTLS(t *testing.T) {
	payload := makePayload(32 * 1024)
	hash := sha256Hex(payload)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(hash + "  file.bin\n"))
	}))
	t.Cleanup(srv.Close)

	// The SSRF guard needs the loopback host to be permitted (test default).
	if !AllowLoopbackDial {
		t.Fatal("AllowLoopbackDial must be true in tests")
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test-only
	}}

	algo, hex, err := FetchSidecarHash(context.Background(), client, srv.URL)
	if err != nil {
		t.Fatalf("FetchSidecarHash: %v", err)
	}
	if algo != "sha256" || hex != hash {
		t.Errorf("got %s:%s, want sha256:%s", algo, hex, hash)
	}
}

func TestParseSidecarContentMD5SHA1(t *testing.T) {
	cases := []struct {
		hexLen int
		algo   string
	}{
		{32, "md5"},
		{40, "sha1"},
		{64, "sha256"},
		{128, "sha512"},
	}
	for _, tt := range cases {
		hex := strings.Repeat("a", tt.hexLen)
		algo, got, err := ParseSidecarContent(hex+"  f.bin\n", "f")
		if err != nil {
			t.Fatalf("len %d: %v", tt.hexLen, err)
		}
		if algo != tt.algo || got != hex {
			t.Errorf("len %d: algo=%q hex-len=%d, want %q", tt.hexLen, algo, len(got), tt.algo)
		}
	}
	// Extension fallback still works when the length is ambiguous... it is
	// not; but a .md5 file with a short odd string falls back to extension.
	algo, _, err := ParseSidecarContent("abcd\n", "f.md5")
	if err != nil {
		t.Fatalf("md5 extension fallback: %v", err)
	}
	if algo != "md5" {
		t.Errorf("extension fallback algo = %q, want md5", algo)
	}
}
