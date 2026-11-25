// Package auth implements password, token, and session primitives for the
// subscription service.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	// MinPasswordLength is the minimum accepted password length in bytes.
	MinPasswordLength = 12

	// MaxPasswordLength is the maximum accepted password length in bytes.
	MaxPasswordLength = 1024

	defaultSaltLength = 16
	defaultKeyLength  = 32

	maxMemory      = 1024 * 1024 // 1 GiB expressed in KiB
	maxIterations  = 10
	maxParallelism = 64
	maxSaltLength  = 1024
	maxKeyLength   = 1024
)

var (
	ErrPasswordTooShort    = errors.New("password too short")
	ErrPasswordTooLong     = errors.New("password too long")
	ErrInvalidPasswordHash = errors.New("invalid password hash")
)

// PasswordParams are the Argon2id parameters embedded in a password hash. They
// are stored in the PHC string so they can be changed without breaking existing
// hashes.
type PasswordParams struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultPasswordParams are the parameters used for new password hashes:
// Argon2id, 64 MiB, three passes, two lanes, a 16-byte salt, and a 32-byte key.
var DefaultPasswordParams = PasswordParams{
	Memory:      64 * 1024,
	Iterations:  3,
	Parallelism: 2,
	SaltLength:  defaultSaltLength,
	KeyLength:   defaultKeyLength,
}

// HashPassword hashes password with the default Argon2id parameters and returns
// a PHC string.
func HashPassword(password string) (string, error) {
	return HashPasswordWithParams(password, DefaultPasswordParams)
}

// HashPasswordWithParams hashes password with params and returns a PHC string
// in the form $argon2id$v=19$m=...,t=...,p=...$salt$hash.
func HashPasswordWithParams(password string, params PasswordParams) (string, error) {
	if err := validatePasswordLength(password); err != nil {
		return "", err
	}
	if err := validateParams(params); err != nil {
		return "", err
	}

	salt := make([]byte, params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}

	key := argon2.IDKey(
		[]byte(password),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		params.KeyLength,
	)

	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		params.Memory,
		params.Iterations,
		params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// VerifyPassword reports whether password matches the PHC-encoded hash. A
// malformed hash returns ErrInvalidPasswordHash.
func VerifyPassword(encoded, password string) (bool, error) {
	if len(password) > MaxPasswordLength {
		return false, ErrPasswordTooLong
	}

	params, salt, expected, err := parsePasswordHash(encoded)
	if err != nil {
		return false, err
	}

	actual := argon2.IDKey(
		[]byte(password),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		uint32(len(expected)),
	)

	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func validatePasswordLength(password string) error {
	if len(password) < MinPasswordLength {
		return ErrPasswordTooShort
	}
	if len(password) > MaxPasswordLength {
		return ErrPasswordTooLong
	}
	return nil
}

func validateParams(params PasswordParams) error {
	switch {
	case params.Memory == 0 || params.Memory > maxMemory:
		return fmt.Errorf("%w: invalid memory parameter", ErrInvalidPasswordHash)
	case params.Iterations == 0 || params.Iterations > maxIterations:
		return fmt.Errorf("%w: invalid iterations parameter", ErrInvalidPasswordHash)
	case params.Parallelism == 0 || params.Parallelism > maxParallelism:
		return fmt.Errorf("%w: invalid parallelism parameter", ErrInvalidPasswordHash)
	case params.SaltLength < 8 || params.SaltLength > maxSaltLength:
		return fmt.Errorf("%w: invalid salt length", ErrInvalidPasswordHash)
	case params.KeyLength < 16 || params.KeyLength > maxKeyLength:
		return fmt.Errorf("%w: invalid key length", ErrInvalidPasswordHash)
	default:
		return nil
	}
}

func parsePasswordHash(encoded string) (PasswordParams, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}
	if parts[1] != "argon2id" {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}
	if parts[2] != fmt.Sprintf("v=%d", argon2.Version) {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}

	params := PasswordParams{}
	seen := make(map[string]bool, 3)
	for _, field := range strings.Split(parts[3], ",") {
		name, value, ok := strings.Cut(field, "=")
		if !ok || seen[name] {
			return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
		}
		seen[name] = true

		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
		}

		switch name {
		case "m":
			params.Memory = uint32(parsed)
		case "t":
			params.Iterations = uint32(parsed)
		case "p":
			if parsed > 255 {
				return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
			}
			params.Parallelism = uint8(parsed)
		default:
			return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
		}
	}
	if !seen["m"] || !seen["t"] || !seen["p"] {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return PasswordParams{}, nil, nil, ErrInvalidPasswordHash
	}

	params.SaltLength = uint32(len(salt))
	params.KeyLength = uint32(len(key))
	if err := validateParams(params); err != nil {
		return PasswordParams{}, nil, nil, err
	}

	return params, salt, key, nil
}
