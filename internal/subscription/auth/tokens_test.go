package auth

import (
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/securetoken"
)

func TestNewVerificationToken(t *testing.T) {
	raw, hash, err := NewVerificationToken()
	require.NoError(t, err)
	require.True(t, securetoken.Validate(raw, verificationTokenBytes))
	require.Equal(t, securetoken.Hash(raw), hash)
	require.NotEqual(t, raw, hash)
}
