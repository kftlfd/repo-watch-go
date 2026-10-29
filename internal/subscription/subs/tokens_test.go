package subs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateToken(t *testing.T) {
	token, err := newToken()
	require.NoError(t, err)
	require.True(t, ValidateToken(token))

	require.False(t, ValidateToken(""))
	require.False(t, ValidateToken("not-a-token"))
	require.False(t, ValidateToken(token+"a"))
}
