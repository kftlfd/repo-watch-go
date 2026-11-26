package auth

import (
	"time"

	"repo-watch/internal/securetoken"
)

const (
	// UserTokenTypeVerify is the user_tokens type for email verification.
	UserTokenTypeVerify = "verify"

	// UserTokenVerifyTTL is how long an email-verification token is valid.
	UserTokenVerifyTTL = 24 * time.Hour

	userTokenVerifyBytes = 32
	refreshTokenBytes    = 32
	jwtIDBytes           = 16
)

// NewVerificationToken returns a new 32-byte email-verification token and the
// SHA-256 hash that must be persisted. The raw value is only sent to the user.
func NewVerificationToken() (raw, hash string, err error) {
	raw, err = securetoken.Generate(userTokenVerifyBytes)
	if err != nil {
		return "", "", err
	}

	return raw, securetoken.Hash(raw), nil
}
