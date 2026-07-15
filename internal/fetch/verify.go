package fetch

// HashType identifies a hash algorithm.
type HashType string

// HashSHA256 is SHA-256, returning a 32-byte digest.
const HashSHA256 HashType = "sha256"

// VerifyConfig configures post-download integrity verification.
type VerifyConfig struct {
	HashType   HashType
	Expected   string // hex-encoded expected digest
	Sidecar    bool   // if true and Expected is empty, fetch SidecarURL
	SidecarURL string
}
