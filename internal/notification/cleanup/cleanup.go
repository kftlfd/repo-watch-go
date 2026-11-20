package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/notification/db/dbGen"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/taskrunner"
)

const (
	publishedOutboxRetention = 2 * rabbitmq.DLQMessageTTL
	failedOutboxRetention    = time.Hour * 24 * 30
)

func CreateCleanupTasks(
	queries *dbGen.Queries,
	now func() time.Time,
) []taskrunner.Task {
	return []taskrunner.Task{
		{
			Name: "outbox",
			Do: func(ctx context.Context) error {
				_, err := queries.DeletePublishedOutbox(ctx, now().Add(-publishedOutboxRetention))
				return err
			},
		},
		{
			Name: "failed outbox",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteFailedOutbox(ctx, now().Add(-failedOutboxRetention))
				return err
			},
		},
	}
}
