package subs_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"repo-watch/internal/github"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
	"repo-watch/internal/subscription/database/migrations"
	"repo-watch/internal/subscription/subs"
)

type testDB struct {
	Container *pg.PostgresContainer
	DB        *sql.DB
	Queries   *database.Queries
}

func newTestDB(t *testing.T) *testDB {
	t.Helper()

	ctx := context.Background()

	container, err := pg.Run(
		ctx,
		"postgres:16-alpine",
		pg.WithDatabase("subscription_test"),
		pg.WithUsername("postgres"),
		pg.WithPassword("postgres"),
		pg.BasicWaitStrategies(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := postgres.Connect(dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	err = postgres.Migrate(ctx, db, migrations.MigrationsFS)

	require.NoError(t, err)

	return &testDB{
		Container: container,
		DB:        db,
		Queries:   database.New(db),
	}

}

type fakeGitHub struct {
	latestReleaseTag string
	err              error
}

func (g *fakeGitHub) GetLatestReleaseTag(ctx context.Context, owner, name string) (string, error) {
	if g.err != nil {
		return "", g.err
	}
	return g.latestReleaseTag, nil
}

func TestCreateSubscription(t *testing.T) {
	db := newTestDB(t)

	github := &fakeGitHub{
		latestReleaseTag: "v1.2.3",
	}

	svc := subs.NewService(subs.Deps{
		GithubClient: github,
		DB:           db.DB,
		Queries:      db.Queries,
	})

	ctx := context.Background()

	email := "alice@example.com"
	repo := "owner/repository"

	err := svc.CreateSubscription(ctx, email, repo)
	require.NoError(t, err)

	dbRepo, err := db.Queries.GetRepositoryByID(ctx, repo)
	require.NoError(t, err)

	dbSub, err := db.Queries.GetSubscription(ctx, database.GetSubscriptionParams{
		Email:        email,
		RepositoryID: dbRepo.ID,
	})
	require.NoError(t, err)
	require.Equal(t, false, dbSub.Active)

	_, err = db.Queries.GetToken(ctx, database.GetTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      subs.TokenTypeConfirm,
	})
	require.NoError(t, err)

	evCnt, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)
	require.True(t, evCnt == 1)
}

func TestCreateSubscriptionSkipsGithubForKnownRepository(t *testing.T) {
	db := newTestDB(t)

	ctx := context.Background()
	gh := &fakeGitHub{latestReleaseTag: "v1.2.3"}

	svc := subs.NewService(subs.Deps{
		GithubClient: gh,
		DB:           db.DB,
		Queries:      db.Queries,
	})

	require.NoError(t, svc.CreateSubscription(ctx, "alice@example.com", "owner/repository"))

	gh.err = errors.New("github must not be called for a known repository")
	require.NoError(t, svc.CreateSubscription(ctx, "bob@example.com", "owner/repository"))

	dbRepo, err := db.Queries.GetRepositoryByID(ctx, "owner/repository")
	require.NoError(t, err)
	require.Equal(t, "v1.2.3", dbRepo.InitTag)
}

func TestCreateSubscriptionMapsGithubErrors(t *testing.T) {
	db := newTestDB(t)

	ctx := context.Background()

	tests := []struct {
		name  string
		ghErr error
		want  error
	}{
		{
			name:  "rate limit",
			ghErr: github.ErrRateLimit,
			want:  subs.ErrRateLimit,
		},
		{
			name:  "wrapped rate limit",
			ghErr: &github.RateLimitError{StatusCode: 403, RetryAfter: time.Minute},
			want:  subs.ErrRateLimit,
		},
		{
			name:  "transient",
			ghErr: github.ErrTransientError,
			want:  subs.ErrUpstreamError,
		},
		{
			name:  "unexpected",
			ghErr: errors.New("boom"),
			want:  subs.ErrUpstreamError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := subs.NewService(subs.Deps{
				GithubClient: &fakeGitHub{err: tt.ghErr},
				DB:           db.DB,
				Queries:      db.Queries,
			})

			err := svc.CreateSubscription(ctx, "alice@example.com", "owner/repository")
			require.ErrorIs(t, err, tt.want)
		})
	}
}
