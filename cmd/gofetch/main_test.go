package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAutoDetectLocalSidecar(t *testing.T) {
	dir := t.TempDir()

	t.Run("no sidecar", func(t *testing.T) {
		out := filepath.Join(dir, "none.bin")
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if algo != "" || hex != "" {
			t.Errorf("algo=%q hex=%q, want empty", algo, hex)
		}
	})

	t.Run("sha256 sidecar", func(t *testing.T) {
		out := filepath.Join(dir, "data.bin")
		hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
		if err := os.WriteFile(out+".sha256", []byte(hash+"  data.bin\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha256" || hex != hash {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, hash)
		}
	})

	t.Run("sha512sum variant", func(t *testing.T) {
		out := filepath.Join(dir, "big.bin")
		hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		if err := os.WriteFile(out+".sha512sum", []byte(hash+"  big.bin\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		algo, hex, err := autoDetectLocalSidecar(out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha512" || hex != hash {
			t.Errorf("algo=%q hex=%q, want sha512/%q", algo, hex, hash)
		}
	})

	t.Run("default flag auto-detects local sidecar", func(t *testing.T) {
		// "-h" defaults to "" and must pick up the sidecar next to the
		// output with zero configuration.
		out := filepath.Join(dir, "data.bin") // sha256 sidecar created above
		algo, hex, err := resolveHash(t.Context(), "", "https://example.com/data.bin", out)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		want := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
		if algo != "sha256" || hex != want {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, want)
		}
	})

	t.Run("explicit flag overrides auto", func(t *testing.T) {
		// resolveHash with an explicit -h value must not touch the sidecar.
		ctx := t.Context()
		explicit := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		algo, hex, err := resolveHash(ctx, "sha256:"+explicit, "https://example.com/x.bin", filepath.Join(dir, "data.bin"))
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if algo != "sha256" || hex != explicit {
			t.Errorf("algo=%q hex=%q, want sha256/%q", algo, hex, explicit)
		}
	})
}

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
			// but parseSidecarContent falls back to extension.
			algo: "sha256",
			hex:  "abcdef0123456789abcdef0123456789",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo, hex, err := parseSidecarContent(tt.content, tt.source)
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
		if got := isValidHex(tt.input); got != tt.want {
			t.Errorf("isValidHex(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
