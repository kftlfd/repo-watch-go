package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/subs"
	"repo-watch/internal/taskrunner"
)

const (
	tokenRetention           = subs.ConfirmTokenValidity
	publishedOutboxRetention = time.Hour * 24 * 14
	processedEventsRetention = time.Hour * 24 * 14
)

func CreateCleanupTasks(queries *dbGen.Queries, now func() time.Time) []taskrunner.Task {
	return []taskrunner.Task{
		{
			Name: "cleanup expired confirm tokens",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteExpiredConfirmTokens(ctx, now().Add(-tokenRetention))
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
	}
}
