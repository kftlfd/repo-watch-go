//go:build integration

package auth_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/postgres"
	"repo-watch/internal/securetoken"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
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

func TestCreateSessionStoresOnlyHashesAndAuthenticates(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	const email = "alice@example.com"

	createUser(t, queries, email)

	pair, err := svc.CreateSession(ctx, email)
	require.NoError(t, err)
	require.Equal(t, email, pair.UserEmail)
	require.NotEmpty(t, pair.AccessToken)
	require.NotEmpty(t, pair.RefreshToken)
	require.Equal(t, auth.DefaultAccessTTL, pair.AccessExpiresIn)
	require.Equal(t, auth.DefaultRefreshTTL, pair.RefreshExpiresIn)

	stored, err := queries.GetRefreshTokenByHash(ctx, securetoken.Hash(pair.RefreshToken))
	require.NoError(t, err)
	require.NotEqual(t, pair.RefreshToken, stored.TokenHash)

	authenticated, err := svc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)
	require.Equal(t, email, authenticated.User.Email)
	require.Equal(t, email, authenticated.Session.UserEmail)
}

func TestRefreshSessionRotatesAndReuseRevokesSession(t *testing.T) {
	svc, queries := newAuthService(t)
	ctx := context.Background()
	const email = "alice@example.com"

	createUser(t, queries, email)

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
	const email = "alice@example.com"

	createUser(t, queries, email)

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
	const email = "alice@example.com"

	createUser(t, queries, email)

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
