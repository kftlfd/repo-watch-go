//go:build integration

package subs_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/github"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
	"repo-watch/internal/subscription/subs"
	"repo-watch/tests/containers"
)

type testDB struct {
	DB      *sql.DB
	Queries *dbGen.Queries
}

func newTestDB(t *testing.T) *testDB {
	t.Helper()

	db := containers.NewPostgresDB(t, "subscription_test")
	require.NoError(t, postgres.Migrate(t.Context(), db, migrations.MigrationsFS))

	return &testDB{
		DB:      db,
		Queries: dbGen.New(db),
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

	dbSub, err := db.Queries.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        email,
		RepositoryID: dbRepo.ID,
	})
	require.NoError(t, err)
	require.Equal(t, false, dbSub.Active)

	_, err = db.Queries.GetToken(ctx, dbGen.GetTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      subs.TokenTypeConfirm,
	})
	require.NoError(t, err)

	evCnt, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)
	require.True(t, evCnt == 1)
}

func TestEmailIsNormalized(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := newTestService(db)

	require.NoError(t, svc.CreateSubscription(ctx, "ALICE@Example.COM", "owner/repository"))

	dbSub, err := db.Queries.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        "alice@example.com",
		RepositoryID: "owner/repository",
	})
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", dbSub.Email)

	confirmToken, err := db.Queries.GetToken(ctx, dbGen.GetTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      subs.TokenTypeConfirm,
	})
	require.NoError(t, err)
	require.NoError(t, svc.ConfirmSubscription(ctx, confirmToken.TokenVal))

	repos, err := svc.GetSubscriptionsList(ctx, "ALICE@Example.com")
	require.NoError(t, err)
	require.Equal(t, []string{"owner/repository"}, repos)

	_, err = svc.GetSubscriptionsList(ctx, "not-an-email")
	require.ErrorIs(t, err, subs.ErrInvalidInput)
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

type testService interface {
	CreateSubscription(ctx context.Context, email, repo string) error
	ConfirmSubscription(ctx context.Context, token string) error
	DeactivateSubscription(ctx context.Context, token string) error
	GetSubscriptionsList(ctx context.Context, email string) ([]string, error)
}

func newTestService(db *testDB) testService {
	return subs.NewService(subs.Deps{
		GithubClient: &fakeGitHub{latestReleaseTag: "v1.2.3"},
		DB:           db.DB,
		Queries:      db.Queries,
	})
}

func createPendingSubscription(t *testing.T, ctx context.Context, db *testDB, svc testService, email, repo string) (dbGen.Subscription, dbGen.Token) {
	t.Helper()

	require.NoError(t, svc.CreateSubscription(ctx, email, repo))

	dbSub, err := db.Queries.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        email,
		RepositoryID: repo,
	})
	require.NoError(t, err)

	token, err := db.Queries.GetToken(ctx, dbGen.GetTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      subs.TokenTypeConfirm,
	})
	require.NoError(t, err)

	return dbSub, token
}

func confirmSubscription(t *testing.T, ctx context.Context, db *testDB, svc testService, sub dbGen.Subscription, token dbGen.Token) dbGen.Token {
	t.Helper()

	require.NoError(t, svc.ConfirmSubscription(ctx, token.TokenVal))

	unsubToken, err := db.Queries.GetToken(ctx, dbGen.GetTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      subs.TokenTypeUnsub,
	})
	require.NoError(t, err)

	return unsubToken
}

func runConcurrently(fns ...func() error) []error {
	errs := make([]error, len(fns))
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func(i int, fn func() error) {
			defer wg.Done()
			<-start
			errs[i] = fn()
		}(i, fn)
	}

	close(start)
	wg.Wait()

	return errs
}

func TestCreateSubscriptionReplacesExistingConfirmToken(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := newTestService(db)

	_, firstToken := createPendingSubscription(t, ctx, db, svc, "alice@example.com", "owner/repository")

	_, secondToken := createPendingSubscription(t, ctx, db, svc, "alice@example.com", "owner/repository")

	require.NotEqual(t, firstToken.TokenVal, secondToken.TokenVal)

	_, err := db.Queries.FindToken(ctx, dbGen.FindTokenParams{
		TokenType: subs.TokenTypeConfirm,
		TokenVal:  firstToken.TokenVal,
	})
	require.ErrorIs(t, err, sql.ErrNoRows)

	require.NoError(t, svc.ConfirmSubscription(ctx, secondToken.TokenVal))
}

func TestConfirmSubscriptionRejectsAlreadyActive(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := newTestService(db)

	sub, confirmToken := createPendingSubscription(t, ctx, db, svc, "alice@example.com", "owner/repository")
	unsubToken := confirmSubscription(t, ctx, db, svc, sub, confirmToken)

	newToken := "new-confirm-token"
	_, err := db.Queries.UpsertToken(ctx, dbGen.UpsertTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      subs.TokenTypeConfirm,
		TokenVal:       newToken,
	})
	require.NoError(t, err)

	repoBefore, err := db.Queries.GetRepositoryByID(ctx, "owner/repository")
	require.NoError(t, err)
	subChangedBefore, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventSubscriptionChanged)
	require.NoError(t, err)
	emailsBefore, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)

	err = svc.ConfirmSubscription(ctx, newToken)
	require.ErrorIs(t, err, subs.ErrSubscriptionAlreadyActive)

	repoAfter, err := db.Queries.GetRepositoryByID(ctx, "owner/repository")
	require.NoError(t, err)
	require.Equal(t, repoBefore.ActiveSubscriptionsCount, repoAfter.ActiveSubscriptionsCount)
	require.Equal(t, repoBefore.ActiveVersion, repoAfter.ActiveVersion)

	subChangedAfter, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventSubscriptionChanged)
	require.NoError(t, err)
	require.Equal(t, subChangedBefore, subChangedAfter)

	emailsAfter, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)
	require.Equal(t, emailsBefore, emailsAfter)

	storedUnsub, err := db.Queries.GetToken(ctx, dbGen.GetTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      subs.TokenTypeUnsub,
	})
	require.NoError(t, err)
	require.Equal(t, unsubToken.TokenVal, storedUnsub.TokenVal)
}

func TestConcurrentConfirmationIncrementsRepositoryOnce(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := newTestService(db)

	_, confirmToken := createPendingSubscription(t, ctx, db, svc, "alice@example.com", "owner/repository")

	errs := runConcurrently(
		func() error { return svc.ConfirmSubscription(ctx, confirmToken.TokenVal) },
		func() error { return svc.ConfirmSubscription(ctx, confirmToken.TokenVal) },
	)

	var succeeded int
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		require.True(t,
			errors.Is(err, subs.ErrSubscriptionAlreadyActive) || errors.Is(err, sql.ErrNoRows),
			"unexpected confirmation error: %v", err,
		)
	}
	require.Equal(t, 1, succeeded)

	repo, err := db.Queries.GetRepositoryByID(ctx, "owner/repository")
	require.NoError(t, err)
	require.Equal(t, int32(1), repo.ActiveSubscriptionsCount)
	require.Equal(t, int32(1), repo.ActiveVersion)

	sub, err := db.Queries.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        "alice@example.com",
		RepositoryID: "owner/repository",
	})
	require.NoError(t, err)
	require.True(t, sub.Active)
	require.Equal(t, int32(1), sub.ActiveVersion)

	subChanged, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventSubscriptionChanged)
	require.NoError(t, err)
	require.Equal(t, int64(1), subChanged)

	repoChanged, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryChanged)
	require.NoError(t, err)
	require.Equal(t, int64(1), repoChanged)

	emails, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)
	require.Equal(t, int64(2), emails)
}

func TestConcurrentUnsubscribeDecrementsRepositoryOnce(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	svc := newTestService(db)

	sub, confirmToken := createPendingSubscription(t, ctx, db, svc, "alice@example.com", "owner/repository")
	unsubToken := confirmSubscription(t, ctx, db, svc, sub, confirmToken)

	errs := runConcurrently(
		func() error { return svc.DeactivateSubscription(ctx, unsubToken.TokenVal) },
		func() error { return svc.DeactivateSubscription(ctx, unsubToken.TokenVal) },
	)

	var succeeded int
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		require.True(t,
			errors.Is(err, subs.ErrSubscriptionAlreadyInactive) || errors.Is(err, sql.ErrNoRows),
			"unexpected unsubscribe error: %v", err,
		)
	}
	require.Equal(t, 1, succeeded)

	repo, err := db.Queries.GetRepositoryByID(ctx, "owner/repository")
	require.NoError(t, err)
	require.Equal(t, int32(0), repo.ActiveSubscriptionsCount)
	require.Equal(t, int32(2), repo.ActiveVersion)

	sub, err = db.Queries.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        "alice@example.com",
		RepositoryID: "owner/repository",
	})
	require.NoError(t, err)
	require.False(t, sub.Active)
	require.Equal(t, int32(2), sub.ActiveVersion)

	subChanged, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventSubscriptionChanged)
	require.NoError(t, err)
	require.Equal(t, int64(2), subChanged)

	repoChanged, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryChanged)
	require.NoError(t, err)
	require.Equal(t, int64(2), repoChanged)

	emails, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventEmailQueued)
	require.NoError(t, err)
	require.Equal(t, int64(3), emails)
}
