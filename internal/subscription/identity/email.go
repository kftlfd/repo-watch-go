// Package identity defines validated values shared by the subscription
// service.
package identity

import (
	"errors"
	"net/mail"
	"strings"
)

// ErrInvalidEmail is returned when an email address is not a bare, valid
// address.
var ErrInvalidEmail = errors.New("invalid email")

// Email is a normalized email address. Construct one with ParseEmail.
type Email string

// ParseEmail validates and normalizes a bare email address. The stored form is
// lowercased and display-name forms are rejected.
func ParseEmail(raw string) (Email, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ErrInvalidEmail
	}

	addr, err := mail.ParseAddress(raw)
	if err != nil || addr.Address != raw {
		return "", ErrInvalidEmail
	}

	email := strings.ToLower(addr.Address)
	if email == "" {
		return "", ErrInvalidEmail
	}

	return Email(email), nil
}

// String returns the normalized address.
func (e Email) String() string {
	return string(e)
}
