package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/subs"
	"repo-watch/internal/taskrunner"
)

const (
	tokenRetention              = subs.SubscriptionTokenConfirmTTL
	publishedOutboxRetention    = 2 * rabbitmq.DLQMessageTTL
	failedOutboxRetention       = time.Hour * 24 * 30
	orphanedUnsubTokenRetention = time.Hour * 24
	processedEventsRetention    = 2 * rabbitmq.DLQMessageTTL
	verificationTokenRetention  = auth.UserTokenVerifyTTL
	unverifiedUserRetention     = 7 * 24 * time.Hour
)

func CreateCleanupTasks(queries *dbGen.Queries, now func() time.Time) []taskrunner.Task {
	return []taskrunner.Task{
		{
			Name: "cleanup expired confirm tokens",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteExpiredSubscriptionConfirmTokens(ctx, now().Add(-tokenRetention))
				return err
			},
		},
		{
			Name: "cleanup expired verification tokens",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteExpiredUserTokens(ctx, now().Add(-verificationTokenRetention))
				return err
			},
		},
		{
			Name: "cleanup expired refresh tokens",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteExpiredRefreshTokens(ctx, now())
				return err
			},
		},
		{
			Name: "cleanup expired sessions",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteExpiredSessions(ctx, now())
				return err
			},
		},
		{
			Name: "cleanup unverified users",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteUnverifiedUsers(ctx, now().Add(-unverifiedUserRetention))
				return err
			},
		},
		{
			Name: "cleanup published outbox",
			Do: func(ctx context.Context) error {
				_, err := queries.DeletePublishedOutbox(ctx, now().Add(-publishedOutboxRetention))
				return err
			},
		},
		{
			Name: "cleanup failed outbox",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteFailedOutbox(ctx, now().Add(-failedOutboxRetention))
				return err
			},
		},
		{
			Name: "cleanup orphaned unsub tokens",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteOrphanedSubscriptionUnsubTokens(ctx, now().Add(-orphanedUnsubTokenRetention))
				return err
			},
		},
		{
			Name: "cleanup processed events",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteProcessedEvents(ctx, now().Add(-processedEventsRetention))
				return err
			},
		},
	}
}
