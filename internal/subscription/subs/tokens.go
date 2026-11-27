package subs

import (
	"time"

	"repo-watch/internal/securetoken"
)

const (
	TokenTypeConfirm = "confirm"
	TokenTypeUnsub   = "unsub"

	SubscriptionTokenConfirmTTL = time.Hour

	tokenBytes = 16
)

func newToken() (string, error) {
	return securetoken.Generate(tokenBytes)
}

func hashToken(token string) string {
	return securetoken.Hash(token)
}

// ValidateToken reports whether token has the shape produced by newToken. It
// lets the API reject malformed tokens before hashing and querying the
// database.
func ValidateToken(token string) bool {
	return securetoken.Validate(token, tokenBytes)
}
