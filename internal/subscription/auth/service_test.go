package auth

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewServiceValidatesJWTSecret(t *testing.T) {
	_, err := NewService(Deps{Config: Config{JWTSecret: []byte("too-short")}})
	require.ErrorIs(t, err, ErrWeakJWTSecret)
}

func TestNewServiceAppliesDefaults(t *testing.T) {
	svc, err := NewService(Deps{
		Config: Config{JWTSecret: []byte(strings.Repeat("s", MinJWTSecretLength))},
	})
	require.NoError(t, err)
	require.Equal(t, DefaultAccessTTL, svc.accessTTL)
	require.Equal(t, DefaultRefreshTTL, svc.refreshTTL)
	require.Equal(t, DefaultJWTLeeway, svc.jwt.leeway)
	require.Equal(t, MinPasswordLength, svc.minPasswordLength)
	require.Equal(t, MaxPasswordLength, svc.maxPasswordLength)
}

func TestServiceValidatesPasswordLength(t *testing.T) {
	svc, err := NewService(Deps{
		Config: Config{JWTSecret: []byte(strings.Repeat("s", MinJWTSecretLength))},
	})
	require.NoError(t, err)

	require.ErrorIs(t, svc.validatePasswordLength(""), ErrInvalidPasswordLength)
	require.NoError(t, svc.validatePasswordLength("a"))
	require.NoError(t, svc.validatePasswordLength(strings.Repeat("a", MaxPasswordLength)))
	require.ErrorIs(t, svc.validatePasswordLength(strings.Repeat("a", MaxPasswordLength+1)), ErrInvalidPasswordLength)
}

func TestNewServiceUsesConfiguredPasswordLengths(t *testing.T) {
	svc, err := NewService(Deps{
		Config: Config{
			JWTSecret:         []byte(strings.Repeat("s", MinJWTSecretLength)),
			MinPasswordLength: 3,
			MaxPasswordLength: 8,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, svc.minPasswordLength)
	require.Equal(t, 8, svc.maxPasswordLength)

	require.ErrorIs(t, svc.validatePasswordLength("ab"), ErrInvalidPasswordLength)
	require.NoError(t, svc.validatePasswordLength("abc"))
	require.NoError(t, svc.validatePasswordLength("abcdefgh"))
	require.ErrorIs(t, svc.validatePasswordLength("abcdefghi"), ErrInvalidPasswordLength)
}

func TestNewServiceRejectsInvalidPasswordLengthRange(t *testing.T) {
	_, err := NewService(Deps{
		Config: Config{
			JWTSecret:         []byte(strings.Repeat("s", MinJWTSecretLength)),
			MinPasswordLength: 9,
			MaxPasswordLength: 8,
		},
	})
	require.ErrorContains(t, err, "min password length")
}
