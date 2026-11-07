package cleanup

import (
	"context"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/taskrunner"
	"time"
)

const (
	publishedOutboxRetention = time.Hour * 24 * 14
)

func CreateCleanupTasks(
	queries *database.Queries,
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
