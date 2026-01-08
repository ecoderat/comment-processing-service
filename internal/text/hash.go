package text

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashSHA256Hex returns the SHA-256 hex digest of the input.
func HashSHA256Hex(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}
