package fetch

import "testing"

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
			name:    "wrong length hex for sha256",
			content: "abcdef0123456789abcdef0123456789\n",
			source:  "file.sha256",
			// 32 hex chars doesn't match sha256 (64) or sha512 (128),
			// but ParseSidecarContent falls back to extension.
			algo: "sha256",
			hex:  "abcdef0123456789abcdef0123456789",
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
