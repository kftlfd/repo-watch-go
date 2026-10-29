package subs_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	notificationdatabase "repo-watch/internal/notification/database"
	notificationmigrations "repo-watch/internal/notification/database/migrations"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
	"repo-watch/internal/subscription/subs"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type notificationTestDB struct {
	Container *pg.PostgresContainer
	DB        *sql.DB
	Queries   *notificationdatabase.Queries
}

func newNotificationTestDB(t *testing.T) *notificationTestDB {
	t.Helper()

	ctx := context.Background()

	container, err := pg.Run(
		ctx,
		"postgres:16-alpine",
		pg.WithDatabase("notification_test"),
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

	require.NoError(t, postgres.Migrate(ctx, db, notificationmigrations.MigrationsFS))

	return &notificationTestDB{
		Container: container,
		DB:        db,
		Queries:   notificationdatabase.New(db),
	}
}

func getTokenValue(t *testing.T, q *database.Queries, email, repoID, tokenType string) string {
	t.Helper()

	ctx := context.Background()

	sub, err := q.GetSubscription(ctx, database.GetSubscriptionParams{
		Email:        email,
		RepositoryID: repoID,
	})
	require.NoError(t, err)

	token, err := q.GetToken(ctx, database.GetTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      tokenType,
	})
	require.NoError(t, err)

	return token.TokenVal
}

type notificationSubscription struct {
	Active     bool
	Version    int32
	UnsubToken string
}

func replaySubscriptionChangedEvents(t *testing.T, ctx context.Context, subDB *testDB, notifDB *notificationTestDB) {
	t.Helper()

	rows, err := subDB.DB.QueryContext(
		ctx,
		`SELECT payload FROM outbox WHERE event_type = $1 ORDER BY id ASC`,
		rabbitmq.EventSubscriptionChanged,
	)
	require.NoError(t, err)
	defer rows.Close()

	var payloads [][]byte
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		payloads = append(payloads, payload)
	}
	require.NoError(t, rows.Err())

	for _, payload := range payloads {
		var event rabbitmq.EventSubscriptionChangedPayload
		require.NoError(t, json.Unmarshal(payload, &event))

		require.NoError(t, notifDB.Queries.CreateOrUpdateSubscription(
			ctx,
			notificationdatabase.CreateOrUpdateSubscriptionParams{
				Email:         event.Email,
				RepositoryID:  event.RepositoryID,
				Active:        event.Active,
				ActiveVersion: event.Version,
				UnsubToken:    event.UnsubToken,
			},
		))
	}
}

func getNotificationSubscription(t *testing.T, ctx context.Context, db *sql.DB, email, repoID string) notificationSubscription {
	t.Helper()

	var got notificationSubscription
	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT active, active_version, unsub_token
		 FROM subscriptions
		 WHERE email = $1 AND repository_id = $2`,
		email,
		repoID,
	).Scan(&got.Active, &got.Version, &got.UnsubToken))

	return got
}

// TestResubscribePropagatesSubscriptionVersion replays the subscription.changed
// events produced by the service into the notification projection. It guards
// against accidentally publishing repository versions instead of
// subscription versions: doing so makes notification ignore the resubscribe.
func TestResubscribePropagatesSubscriptionVersion(t *testing.T) {
	subDB := newTestDB(t)
	notifDB := newNotificationTestDB(t)

	github := &fakeGitHub{latestReleaseTag: "v1.2.3"}
	svc := subs.NewService(subs.Deps{
		GithubClient: github,
		DB:           subDB.DB,
		Queries:      subDB.Queries,
	})

	ctx := context.Background()

	const (
		repo   = "owner/repository"
		emailA = "alice@example.com"
		emailB = "bob@example.com"
	)

	// A subscribes and confirms. B then confirms while A is still active, so
	// the repository's active version does not change for B's confirmation.
	require.NoError(t, svc.CreateSubscription(ctx, emailA, repo))
	require.NoError(t, svc.ConfirmSubscription(ctx,
		getTokenValue(t, subDB.Queries, emailA, repo, subs.TokenTypeConfirm)))

	require.NoError(t, svc.CreateSubscription(ctx, emailB, repo))
	require.NoError(t, svc.ConfirmSubscription(ctx,
		getTokenValue(t, subDB.Queries, emailB, repo, subs.TokenTypeConfirm)))

	// B unsubscribes while A remains active. The projection must observe the
	// deactivation before the resubscribe.
	require.NoError(t, svc.DeactivateSubscription(ctx,
		getTokenValue(t, subDB.Queries, emailB, repo, subs.TokenTypeUnsub)))
	replaySubscriptionChangedEvents(t, ctx, subDB, notifDB)

	b := getNotificationSubscription(t, ctx, notifDB.DB, emailB, repo)
	require.False(t, b.Active)
	require.Equal(t, int32(2), b.Version)

	a := getNotificationSubscription(t, ctx, notifDB.DB, emailA, repo)
	require.True(t, a.Active)
	require.Equal(t, int32(1), a.Version)

	// B resubscribes while A is still active. The repository version does not
	// change, so this only works if the event carries B's subscription version.
	require.NoError(t, svc.CreateSubscription(ctx, emailB, repo))
	require.NoError(t, svc.ConfirmSubscription(ctx,
		getTokenValue(t, subDB.Queries, emailB, repo, subs.TokenTypeConfirm)))
	replaySubscriptionChangedEvents(t, ctx, subDB, notifDB)

	b = getNotificationSubscription(t, ctx, notifDB.DB, emailB, repo)
	require.True(t, b.Active, "resubscribe must reactivate the notification projection")
	require.Equal(t, int32(3), b.Version)
	require.NotEmpty(t, b.UnsubToken)

	a = getNotificationSubscription(t, ctx, notifDB.DB, emailA, repo)
	require.True(t, a.Active)
	require.Equal(t, int32(1), a.Version)

	// A's later unsubscribe must not affect B, and B's later state must not
	// regress A's version.
	require.NoError(t, svc.DeactivateSubscription(ctx,
		getTokenValue(t, subDB.Queries, emailA, repo, subs.TokenTypeUnsub)))
	replaySubscriptionChangedEvents(t, ctx, subDB, notifDB)

	a = getNotificationSubscription(t, ctx, notifDB.DB, emailA, repo)
	require.False(t, a.Active)
	require.Equal(t, int32(2), a.Version)

	b = getNotificationSubscription(t, ctx, notifDB.DB, emailB, repo)
	require.True(t, b.Active)
	require.Equal(t, int32(3), b.Version)
}
