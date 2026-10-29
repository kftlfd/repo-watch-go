package subs

import (
	"crypto/rand"
	"encoding/base64"
)

const (
	TokenTypeConfirm = "confirm"
	TokenTypeUnsub   = "unsub"

	tokenBytes = 16
)

func newToken() (string, error) {
	token := make([]byte, tokenBytes)

	if _, err := rand.Read(token); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(token), nil
}

// ValidateToken reports whether token has the shape produced by newToken. It
// lets the API reject malformed tokens before querying the database.
func ValidateToken(token string) bool {
	b, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(b) == tokenBytes
}
