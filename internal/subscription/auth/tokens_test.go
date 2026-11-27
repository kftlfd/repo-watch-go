package auth

import (
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/securetoken"
)

func TestNewVerificationToken(t *testing.T) {
	raw, hash, err := NewUserVerifyToken()
	require.NoError(t, err)
	require.True(t, securetoken.Validate(raw, userTokenVerifyBytes))
	require.True(t, ValidUserVerifyToken(raw))
	require.Equal(t, securetoken.Hash(raw), hash)
	require.NotEqual(t, raw, hash)
	require.False(t, ValidUserVerifyToken("not-a-token"))
	require.False(t, ValidUserVerifyToken(""))
}
