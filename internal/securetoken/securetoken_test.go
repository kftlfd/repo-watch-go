package securetoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateAndValidate(t *testing.T) {
	for _, size := range []int{16, 32} {
		token, err := Generate(size)
		require.NoError(t, err)
		require.True(t, Validate(token, size))
		require.False(t, Validate(token, size+1))
	}

	_, err := Generate(0)
	require.Error(t, err)
}

func TestValidateRejectsMalformedTokens(t *testing.T) {
	const size = 16

	token, err := Generate(size)
	require.NoError(t, err)

	require.False(t, Validate("", size))
	require.False(t, Validate("not-a-token", size))
	require.False(t, Validate(token+"a", size))
	require.False(t, Validate(token[:len(token)-1], size))
}

func TestHashIsStableAndHidesToken(t *testing.T) {
	token, err := Generate(32)
	require.NoError(t, err)

	hash := Hash(token)
	require.Len(t, hash, 64)
	require.Equal(t, hash, Hash(token))
	require.NotContains(t, hash, token)
	require.NotEqual(t, Hash(token), Hash(token+"x"))
}
