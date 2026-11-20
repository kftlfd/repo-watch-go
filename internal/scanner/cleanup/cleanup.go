package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/db/dbGen"
	"repo-watch/internal/taskrunner"
)

const (
	publishedOutboxRetention = 2 * rabbitmq.DLQMessageTTL
	failedOutboxRetention    = time.Hour * 24 * 30
	oldReleasesRetention     = time.Hour * 24 * 90
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
		{
			Name: "releases",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteOldReleases(ctx, now().Add(-oldReleasesRetention))
				return err
			},
		},
	}
}
