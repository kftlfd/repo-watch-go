package auth

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testJWTManager(t *testing.T) *JWTManager {
	t.Helper()

	m, err := NewJWTManager([]byte(strings.Repeat("s", MinJWTSecretLength)), "issuer", "audience", 30*time.Second)
	require.NoError(t, err)
	return m
}

func TestIssueAndParseJWT(t *testing.T) {
	m := testJWTManager(t)
	now := time.Unix(1_700_000_000, 0)

	token, err := m.IssueAt("alice@example.com", 42, now, 10*time.Minute)
	require.NoError(t, err)

	claims, err := m.ParseAt(token, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", claims.Subject)
	require.Equal(t, int64(42), claims.SessionID)
	require.NotEmpty(t, claims.ID)
	require.Equal(t, "issuer", claims.Issuer)
	require.Equal(t, "audience", claims.Audience)
	require.Equal(t, now.Unix(), claims.IssuedAt)
	require.Equal(t, now.Add(10*time.Minute).Unix(), claims.ExpiresAt)
}

func TestParseJWTRejectsExpiredAndNotYetValidTokens(t *testing.T) {
	m := testJWTManager(t)
	now := time.Unix(1_700_000_000, 0)

	token, err := m.IssueAt("alice@example.com", 42, now, 10*time.Minute)
	require.NoError(t, err)

	_, err = m.ParseAt(token, now.Add(11*time.Minute+time.Second))
	require.ErrorIs(t, err, ErrInvalidToken)

	_, err = m.ParseAt(token, now.Add(-2*time.Minute))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestParseJWTRejectsWrongSignatureIssuerAndAudience(t *testing.T) {
	m := testJWTManager(t)
	now := time.Now()
	token, err := m.IssueAt("alice@example.com", 42, now, 10*time.Minute)
	require.NoError(t, err)

	other, err := NewJWTManager([]byte(strings.Repeat("x", MinJWTSecretLength)), "issuer", "audience", time.Minute)
	require.NoError(t, err)
	_, err = other.Parse(token)
	require.ErrorIs(t, err, ErrInvalidToken)

	wrongIssuer, err := NewJWTManager([]byte(strings.Repeat("s", MinJWTSecretLength)), "other", "audience", time.Minute)
	require.NoError(t, err)
	_, err = wrongIssuer.Parse(token)
	require.ErrorIs(t, err, ErrInvalidToken)

	wrongAudience, err := NewJWTManager([]byte(strings.Repeat("s", MinJWTSecretLength)), "issuer", "other", time.Minute)
	require.NoError(t, err)
	_, err = wrongAudience.Parse(token)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestParseJWTRejectsUnsupportedAlgorithm(t *testing.T) {
	m := testJWTManager(t)
	now := time.Now()
	token, err := m.IssueAt("alice@example.com", 42, now, 10*time.Minute)
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	parts[0] = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))

	_, err = m.Parse(strings.Join(parts, "."))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestParseJWTRejectsMalformedTokens(t *testing.T) {
	m := testJWTManager(t)

	for _, token := range []string{
		"",
		"one",
		"one.two",
		"one.two.three",
		"!!!.e30.!!!",
	} {
		_, err := m.Parse(token)
		require.ErrorIs(t, err, ErrInvalidToken, token)
	}
}

func TestNewJWTManagerRejectsWeakSecret(t *testing.T) {
	_, err := NewJWTManager([]byte("short"), "issuer", "audience", 0)
	require.ErrorIs(t, err, ErrWeakJWTSecret)
}

func TestIssueJWTRejectsInvalidTTL(t *testing.T) {
	m := testJWTManager(t)

	_, err := m.Issue("alice@example.com", 42, 0)
	require.ErrorIs(t, err, ErrInvalidTokenTTL)
}

func TestParseJWTAllowsLeeway(t *testing.T) {
	m := testJWTManager(t)
	now := time.Unix(1_700_000_000, 0)

	token, err := m.IssueAt("alice@example.com", 42, now, time.Minute)
	require.NoError(t, err)

	_, err = m.ParseAt(token, now.Add(time.Minute+29*time.Second))
	require.NoError(t, err)

	_, err = m.ParseAt(token, now.Add(time.Minute+31*time.Second))
	require.ErrorIs(t, err, ErrInvalidToken)
}
