package auth

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashAndVerifyPassword(t *testing.T) {
	const password = "correct horse battery staple"

	hash, err := HashPassword(password)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(hash, "$argon2id$v=19$m=65536,t=3,p=2$"))
	require.NotContains(t, hash, password)

	ok, err := VerifyPassword(hash, password)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = VerifyPassword(hash, password+"x")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHashPasswordUsesProvidedParams(t *testing.T) {
	params := PasswordParams{
		Memory:      8 * 1024,
		Iterations:  1,
		Parallelism: 1,
		SaltLength:  8,
		KeyLength:   16,
	}

	hash, err := HashPasswordWithParams("a sufficiently long password", params)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(hash, "$argon2id$v=19$m=8192,t=1,p=1$"))

	ok, err := VerifyPassword(hash, "a sufficiently long password")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPasswordLengthLimits(t *testing.T) {
	_, err := HashPassword(strings.Repeat("a", MinPasswordLength-1))
	require.ErrorIs(t, err, ErrPasswordTooShort)

	_, err = HashPassword(strings.Repeat("a", MaxPasswordLength+1))
	require.ErrorIs(t, err, ErrPasswordTooLong)

	hash, err := HashPassword(strings.Repeat("a", MinPasswordLength))
	require.NoError(t, err)

	ok, err := VerifyPassword(hash, strings.Repeat("a", MaxPasswordLength+1))
	require.ErrorIs(t, err, ErrPasswordTooLong)
	require.False(t, ok)
}

func TestVerifyPasswordRejectsMalformedHashes(t *testing.T) {
	tests := []string{
		"",
		"not-a-hash",
		"$argon2i$v=19$m=65536,t=3,p=2$c2FsdHNhbHQ$aGFzaA",
		"$argon2id$v=18$m=65536,t=3,p=2$c2FsdHNhbHQ$aGFzaA",
		"$argon2id$v=19$m=65536,t=3$c2FsdHNhbHQ$aGFzaA",
		"$argon2id$v=19$m=notanumber,t=3,p=2$c2FsdHNhbHQ$aGFzaA",
		"$argon2id$v=19$m=65536,t=3,p=2$%%%$aGFzaA",
		"$argon2id$v=19$m=65536,t=3,p=2$c2FsdHNhbHQ$%%%",
	}

	for _, encoded := range tests {
		ok, err := VerifyPassword(encoded, "a sufficiently long password")
		require.ErrorIs(t, err, ErrInvalidPasswordHash, encoded)
		require.False(t, ok)
	}
}

func TestHashPasswordRejectsInvalidParams(t *testing.T) {
	tests := []PasswordParams{
		{Memory: 0, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{Memory: maxMemory + 1, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{Memory: 1024, Iterations: 0, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{Memory: 1024, Iterations: 1, Parallelism: 0, SaltLength: 16, KeyLength: 32},
		{Memory: 1024, Iterations: 1, Parallelism: 1, SaltLength: 1, KeyLength: 32},
		{Memory: 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 1},
	}

	for _, params := range tests {
		_, err := HashPasswordWithParams("a sufficiently long password", params)
		require.ErrorIs(t, err, ErrInvalidPasswordHash)
	}
}
