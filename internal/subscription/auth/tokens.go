package auth

import (
	"time"

	"repo-watch/internal/securetoken"
)

const (
	// UserTokenTypeVerify is the user_tokens type for email verification.
	UserTokenTypeVerify = "verify"

	// VerificationTokenTTL is how long an email-verification token is valid.
	VerificationTokenTTL = 24 * time.Hour

	verificationTokenBytes = 32
	refreshTokenBytes      = 32
	jtiBytes               = 16
)

// NewVerificationToken returns a new 32-byte email-verification token and the
// SHA-256 hash that must be persisted. The raw value is only sent to the user.
func NewVerificationToken() (raw, hash string, err error) {
	raw, err = securetoken.Generate(verificationTokenBytes)
	if err != nil {
		return "", "", err
	}

	return raw, securetoken.Hash(raw), nil
}
