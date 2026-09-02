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

func TestNormalizeMirrors(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		got, err := normalizeMirrors("")
		if err != nil || got != nil {
			t.Fatalf("empty: got=%v err=%v, want nil/nil", got, err)
		}
	})

	t.Run("bare hostnames get https", func(t *testing.T) {
		got, err := normalizeMirrors("example.com, www.example.com")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		want := []string{"https://example.com", "https://www.example.com"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("mirror[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("explicit scheme preserved", func(t *testing.T) {
		got, err := normalizeMirrors("http://example.com")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 1 || got[0] != "http://example.com" {
			t.Errorf("got %v, want [http://example.com]", got)
		}
	})

	t.Run("private mirror rejected", func(t *testing.T) {
		_, err := normalizeMirrors("127.0.0.1")
		if err == nil {
			t.Fatal("expected SSRF rejection of loopback mirror")
		}
	})
}
