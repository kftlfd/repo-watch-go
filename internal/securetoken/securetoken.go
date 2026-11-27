// Package securetoken creates, hashes, and validates opaque bearer tokens.
//
// Tokens are random base64url-encoded byte strings. Only the SHA-256 hash of a
// token is stored; the raw value is returned to the caller that sends it to the
// user and is never persisted. Token sizes and lifetimes belong to the packages
// that define the tokens.
package securetoken

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// Generate returns a new random token of n bytes, encoded with base64url
// without padding.
func Generate(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("token size must be positive")
	}

	token := make([]byte, n)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(token), nil
}

// Hash returns the hexadecimal SHA-256 hash of token. It is the value stored in
// the database for every opaque token.
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Validate reports whether token has the shape produced by Generate for n
// bytes: canonical base64url without padding that decodes to exactly n bytes.
func Validate(token string, n int) bool {
	if n <= 0 || token == "" {
		return false
	}

	decoded, err := base64.RawURLEncoding.Strict().DecodeString(token)
	return err == nil && len(decoded) == n
}
