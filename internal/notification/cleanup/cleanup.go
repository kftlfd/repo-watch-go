package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/notification/db/dbGen"
	"repo-watch/internal/taskrunner"
)

const (
	publishedOutboxRetention = time.Hour * 24 * 14
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
	}
}
