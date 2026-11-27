//go:build integration

package inbox

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/events"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/securetoken"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/subs"
	"repo-watch/tests/containers"
)

func TestConcurrentRepositoryRemovalAndUnsubscribeDoesNotDeadlock(t *testing.T) {
	tests := []struct {
		name       string
		useToken   bool
		allowedErr error
	}{
		{name: "token", useToken: true, allowedErr: sql.ErrNoRows},
		{name: "authenticated", useToken: false, allowedErr: subs.ErrSubscriptionAlreadyInactive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := containers.NewPostgresDB(t, "inbox_test")
			require.NoError(t, postgres.Migrate(t.Context(), db, migrations.MigrationsFS))

			queries := dbGen.New(db)
			worker := &inboxWorker{db: db, q: queries}
			svc := subs.NewService(subs.Deps{DB: db, Queries: queries})

			for i := range 20 {
				email, err := identity.ParseEmail(fmt.Sprintf("%s-%d@example.com", tt.name, i))
				require.NoError(t, err)
				repoID := fmt.Sprintf("owner/removal-%s-%d", tt.name, i)
				unsubToken := createActiveSubscription(t, queries, email, repoID)

				payload, err := json.Marshal(events.EventRepositoryRemovedPayload{
					RepositoryID: repoID,
					Owner:        "owner",
					Name:         fmt.Sprintf("removal-%s-%d", tt.name, i),
				})
				require.NoError(t, err)
				msg := events.Envelope{
					EventID:   fmt.Sprintf("removal-%s-%d", tt.name, i),
					EventType: events.EventRepositoryRemoved,
					Version:   events.VersionRepositoryRemoved,
					Payload:   payload,
				}

				start := make(chan struct{})
				var wg sync.WaitGroup
				var unsubscribeErr, removalErr error
				var ack rabbitmq.AckType
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					if tt.useToken {
						unsubscribeErr = svc.DeactivateSubscription(t.Context(), unsubToken)
						return
					}
					unsubscribeErr = svc.DeactivateSubscriptionByRepo(t.Context(), email, repoID)
				}()
				go func() {
					defer wg.Done()
					<-start
					ack, removalErr = worker.handler(t.Context(), msg)
				}()
				close(start)
				wg.Wait()

				require.True(t,
					unsubscribeErr == nil || errors.Is(unsubscribeErr, tt.allowedErr),
					"unexpected unsubscribe error: %v", unsubscribeErr,
				)
				require.NoError(t, removalErr)
				require.Equal(t, rabbitmq.Ack, ack)

				sub, err := queries.GetSubscription(t.Context(), dbGen.GetSubscriptionParams{
					Email: email.String(), RepositoryID: repoID,
				})
				require.NoError(t, err)
				require.False(t, sub.Active)
				require.Equal(t, int32(2), sub.ActiveVersion)

				repo, err := queries.GetRepositoryByID(t.Context(), repoID)
				require.NoError(t, err)
				require.Zero(t, repo.ActiveSubscriptionsCount)
				require.Equal(t, int32(2), repo.ActiveVersion)
			}
		})
	}
}

func createActiveSubscription(
	t *testing.T,
	queries *dbGen.Queries,
	email identity.Email,
	repoID string,
) string {
	t.Helper()

	_, err := queries.CreateRepository(t.Context(), dbGen.CreateRepositoryParams{
		ID: repoID, Owner: "owner", Name: repoID[len("owner/"):], InitTag: "v1.0.0",
	})
	require.NoError(t, err)
	sub, err := queries.CreateSubscription(t.Context(), dbGen.CreateSubscriptionParams{
		Email: email.String(), RepositoryID: repoID,
	})
	require.NoError(t, err)
	_, err = queries.ActivateSubscription(t.Context(), sub.ID)
	require.NoError(t, err)

	unsubToken, err := securetoken.Generate(16)
	require.NoError(t, err)
	_, err = queries.UpsertSubscriptionToken(t.Context(), dbGen.UpsertSubscriptionTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      subs.TokenTypeUnsub,
		TokenHash:      securetoken.Hash(unsubToken),
	})
	require.NoError(t, err)
	_, err = queries.IncrementRepositoryActiveCount(t.Context(), repoID)
	require.NoError(t, err)

	return unsubToken
}
