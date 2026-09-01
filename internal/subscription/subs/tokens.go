package subs

import (
	"crypto/rand"
	"encoding/base64"
)

const (
	TokenTypeConfirm = "confirm"
	TokenTypeUnsub   = "unsub"
)

func newToken() (string, error) {
	token := make([]byte, 16)

	if _, err := rand.Read(token); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(token), nil
}
