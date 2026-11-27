//go:build integration

package subs_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/subs"
)

func newAuthServiceForTest(t *testing.T, db *testDB) *auth.Service {
	t.Helper()

	svc, err := auth.NewService(auth.Deps{
		DB:      db.DB,
		Queries: db.Queries,
		Config: auth.Config{
			JWTSecret: []byte(strings.Repeat("s", auth.MinJWTSecretLength)),
		},
	})
	require.NoError(t, err)
	return svc
}

func queuedVerificationToken(t *testing.T, db *testDB, email string) string {
	t.Helper()

	rows, err := db.DB.QueryContext(t.Context(), `
		SELECT payload
		FROM outbox
		WHERE event_type = $1
		ORDER BY id DESC
		LIMIT 20`,
		events.EventEmailQueued,
	)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))

		var queued events.EventEmailQueuedPayload
		require.NoError(t, json.Unmarshal(payload, &queued))
		if queued.Type != emails.EmailVerifyAccount || queued.Recipient != email {
			continue
		}

		var data emails.EmailVerifyAccountData
		require.NoError(t, json.Unmarshal(queued.Data, &data))
		require.NotEmpty(t, data.VerifyToken)
		return data.VerifyToken
	}
	require.NoError(t, rows.Err())

	t.Fatal("verification token not found")
	return ""
}

func TestSignupVerifyLoginSubscribeAndListFlow(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	authSvc := newAuthServiceForTest(t, db)
	subsSvc := subs.NewService(subs.Deps{
		GithubClient: &fakeGitHub{latestReleaseTag: "v1.2.3"},
		DB:           db.DB,
		Queries:      db.Queries,
	})

	email := mustEmail(t, "alice@example.com")
	const password = "hunter2"

	require.NoError(t, authSvc.Signup(ctx, email, password))
	require.NoError(t, authSvc.VerifyEmail(ctx, queuedVerificationToken(t, db, email.String())))

	pair, err := authSvc.Login(ctx, email, password)
	require.NoError(t, err)

	session, err := authSvc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)
	require.Equal(t, email.String(), session.User.Email)

	require.NoError(t, subsSvc.CreateSubscription(ctx, email, "owner/repository", true))

	repos, err := subsSvc.GetSubscriptionsList(ctx, email)
	require.NoError(t, err)
	require.Equal(t, []string{"owner/repository"}, repos)

	require.NoError(t, subsSvc.DeactivateSubscriptionByRepo(ctx, email, "owner/repository"))

	repos, err = subsSvc.GetSubscriptionsList(ctx, email)
	require.NoError(t, err)
	require.Empty(t, repos)
}

func TestAnonymousConfirmationSurvivesLaterSignup(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	authSvc := newAuthServiceForTest(t, db)
	subsSvc := subs.NewService(subs.Deps{
		GithubClient: &fakeGitHub{latestReleaseTag: "v1.2.3"},
		DB:           db.DB,
		Queries:      db.Queries,
	})

	email := mustEmail(t, "bob@example.com")
	const password = "correct horse battery staple"

	require.NoError(t, subsSvc.CreateSubscription(ctx, email, "owner/repository", false))
	confirmToken, err := queuedConfirmToken(t, ctx, db, email.String())
	require.NoError(t, err)
	require.NoError(t, subsSvc.ConfirmSubscription(ctx, confirmToken))

	repos, err := subsSvc.GetSubscriptionsList(ctx, email)
	require.NoError(t, err)
	require.Equal(t, []string{"owner/repository"}, repos)

	require.NoError(t, authSvc.Signup(ctx, email, password))
	require.NoError(t, authSvc.VerifyEmail(ctx, queuedVerificationToken(t, db, email.String())))

	pair, err := authSvc.Login(ctx, email, password)
	require.NoError(t, err)
	_, err = authSvc.Authenticate(ctx, pair.AccessToken)
	require.NoError(t, err)

	repos, err = subsSvc.GetSubscriptionsList(ctx, email)
	require.NoError(t, err)
	require.Equal(t, []string{"owner/repository"}, repos)
}
