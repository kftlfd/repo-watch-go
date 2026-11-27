//go:build integration

package auth_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/postgres"
	"repo-watch/internal/securetoken"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
	"repo-watch/internal/subscription/identity"
	"repo-watch/tests/containers"
)

func newAuthService(t *testing.T) (*auth.Service, *dbGen.Queries) {
	t.Helper()

	db := containers.NewPostgresDB(t, "auth_test")
	require.NoError(t, postgres.Migrate(t.Context(), db, migrations.MigrationsFS))

	queries := dbGen.New(db)
	svc, err := auth.NewService(auth.Deps{
		DB:      db,
		Queries: queries,
		Config: auth.Config{
			JWTSecret: []byte(strings.Repeat("s", auth.MinJWTSecretLength)),
		},
	})
	require.NoError(t, err)

	return svc, queries
}

func mustEmail(t *testing.T, raw string) identity.Email {
	t.Helper()

	email, err := identity.ParseEmail(raw)
	require.NoError(t, err)
	return email
}

func createUser(t *testing.T, queries *dbGen.Queries, email string) {
	t.Helper()

	passwordHash, err := auth.HashPassword("correct horse battery staple")
	require.NoError(t, err)

	_, err = queries.CreateUser(t.Context(), dbGen.CreateUserParams{
		Email:        email,
		PasswordHash: passwordHash,
	})
	require.NoError(t, err)
}

// latestVerificationToken returns the raw token from the most recent
// verification email in the outbox. The lease is set in the past so the test
// can read successive rows without waiting.
func latestVerificationToken(t *testing.T, queries *dbGen.Queries) string {
	t.Helper()

	rows, err := queries.GetOutboxBatch(t.Context(), dbGen.GetOutboxBatchParams{
		Limit: 100,
		LeasedUntil: sql.NullTime{
			Valid: true,
			Time:  time.Now().Add(-time.Hour),
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	token := ""
	for _, row := range rows {
		if row.EventType != events.EventEmailQueued {
			continue
		}

		var payload events.EventEmailQueuedPayload
		require.NoError(t, json.Unmarshal(row.Payload, &payload))
		if payload.Type != emails.EmailVerifyAccount {
			continue
		}

		var data emails.EmailVerifyAccountData
		require.NoError(t, json.Unmarshal(payload.Data, &data))
		token = data.VerifyToken
	}

	require.NotEmpty(t, token)
	return token
}

func TestSignupVerifyLoginFlow(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")
	const password = "correct horse battery staple"

	require.NoError(t, svc.Signup(ctx, email, password))

	_, err := svc.Login(ctx, email, password)
	require.ErrorIs(t, err, auth.ErrEmailUnverified)

	token := latestVerificationToken(t, queries)
	require.True(t, securetoken.Validate(token, 32))

	storedToken, err := queries.GetUserToken(ctx, dbGen.GetUserTokenParams{
		UserEmail: email.String(),
		TokenType: auth.UserTokenTypeVerify,
	})
	require.NoError(t, err)
	require.Equal(t, securetoken.Hash(token), storedToken.TokenHash)
	require.NotEqual(t, token, storedToken.TokenHash)

	require.NoError(t, svc.VerifyEmail(ctx, token))

	_, err = queries.GetUserToken(ctx, dbGen.GetUserTokenParams{
		UserEmail: email.String(),
		TokenType: auth.UserTokenTypeVerify,
	})
	require.ErrorIs(t, err, sql.ErrNoRows)

	user, err := queries.GetUserByEmail(ctx, email.String())
	require.NoError(t, err)
	require.True(t, user.EmailVerifiedAt.Valid)

	pair, err := svc.Login(ctx, email, password)
	require.NoError(t, err)
	require.Equal(t, email.String(), pair.UserEmail)
	require.NotEmpty(t, pair.AccessToken)
	require.NotEmpty(t, pair.RefreshToken)

	authenticated, err := svc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)
	require.Equal(t, email.String(), authenticated.User.Email)

	_, err = svc.Login(ctx, email, "wrong password")
	require.ErrorIs(t, err, auth.ErrInvalidCredentials)

	require.ErrorIs(t, svc.VerifyEmail(ctx, token), auth.ErrInvalidToken)
}

func TestSignupIsGenericForExistingAccountsAndResendRotates(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")
	const firstPassword = "correct horse battery staple"
	const secondPassword = "another valid password"

	require.NoError(t, svc.Signup(ctx, email, firstPassword))
	firstToken := latestVerificationToken(t, queries)

	require.NoError(t, svc.Signup(ctx, email, secondPassword))
	secondToken := latestVerificationToken(t, queries)
	require.NotEqual(t, firstToken, secondToken)

	require.NoError(t, svc.ResendVerification(ctx, email))
	thirdToken := latestVerificationToken(t, queries)
	require.NotEqual(t, secondToken, thirdToken)

	require.NoError(t, svc.VerifyEmail(ctx, thirdToken))
	require.ErrorIs(t, svc.VerifyEmail(ctx, secondToken), auth.ErrInvalidToken)

	// Duplicate signup does not change the password of a verified account.
	require.NoError(t, svc.Signup(ctx, email, "verified-account-password-attempt"))
	_, err := svc.Login(ctx, email, secondPassword)
	require.NoError(t, err)

	_, err = svc.Login(ctx, email, firstPassword)
	require.ErrorIs(t, err, auth.ErrInvalidCredentials)
	_, err = svc.Login(ctx, email, "verified-account-password-attempt")
	require.ErrorIs(t, err, auth.ErrInvalidCredentials)

	_, err = svc.Login(ctx, mustEmail(t, "missing@example.com"), firstPassword)
	require.ErrorIs(t, err, auth.ErrInvalidCredentials)
}

func TestCreateSessionStoresOnlyHashesAndAuthenticates(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")

	createUser(t, queries, email.String())

	pair, err := svc.CreateSession(ctx, email)
	require.NoError(t, err)
	require.Equal(t, email.String(), pair.UserEmail)
	require.NotEmpty(t, pair.AccessToken)
	require.NotEmpty(t, pair.RefreshToken)
	require.Equal(t, auth.DefaultAccessTTL, pair.AccessExpiresIn)
	require.Equal(t, auth.DefaultRefreshTTL, pair.RefreshExpiresIn)

	stored, err := queries.GetRefreshTokenByHash(ctx, securetoken.Hash(pair.RefreshToken))
	require.NoError(t, err)
	require.NotEqual(t, pair.RefreshToken, stored.TokenHash)

	authenticated, err := svc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)
	require.Equal(t, email.String(), authenticated.User.Email)
	require.Equal(t, email.String(), authenticated.Session.UserEmail)
}

func TestRefreshSessionRotatesAndReuseRevokesSession(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")

	createUser(t, queries, email.String())

	first, err := svc.CreateSession(ctx, email)
	require.NoError(t, err)

	second, err := svc.RefreshSession(ctx, first.RefreshToken)
	require.NoError(t, err)
	require.NotEqual(t, first.RefreshToken, second.RefreshToken)
	require.Equal(t, first.UserEmail, second.UserEmail)

	used, err := queries.GetRefreshTokenByHash(ctx, securetoken.Hash(first.RefreshToken))
	require.NoError(t, err)
	require.True(t, used.UsedAt.Valid)

	fresh, err := queries.GetRefreshTokenByHash(ctx, securetoken.Hash(second.RefreshToken))
	require.NoError(t, err)
	require.False(t, fresh.UsedAt.Valid)

	_, err = svc.RefreshSession(ctx, first.RefreshToken)
	require.ErrorIs(t, err, auth.ErrTokenReused)

	_, err = svc.Authenticate(ctx, second.AccessToken)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestRevokeSessionInvalidatesAccessToken(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")

	createUser(t, queries, email.String())

	pair, err := svc.CreateSession(ctx, email)
	require.NoError(t, err)

	authenticated, err := svc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)
	require.NoError(t, svc.RevokeSession(ctx, authenticated.Session.ID))

	_, err = svc.Authenticate(ctx, pair.AccessToken)
	require.ErrorIs(t, err, auth.ErrInvalidToken)

	_, err = svc.RefreshSession(ctx, pair.RefreshToken)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestConcurrentRefreshSucceedsOnce(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	email := mustEmail(t, "alice@example.com")

	createUser(t, queries, email.String())

	pair, err := svc.CreateSession(ctx, email)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = svc.RefreshSession(ctx, pair.RefreshToken)
		}(i)
	}
	close(start)
	wg.Wait()

	var succeeded int
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		require.True(t,
			errors.Is(err, auth.ErrTokenReused) || errors.Is(err, auth.ErrInvalidToken),
			"unexpected refresh error: %v", err,
		)
	}
	require.Equal(t, 1, succeeded)
}

func TestRefreshSessionRejectsMalformedToken(t *testing.T) {
	svc, _ := newAuthService(t)

	_, err := svc.RefreshSession(context.Background(), "not-a-token")
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}
