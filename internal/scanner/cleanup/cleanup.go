package cleanup

import (
	"context"
	"time"

	"repo-watch/internal/scanner/database"
	"repo-watch/internal/taskrunner"
)

const (
	publishedOutboxRetention = time.Hour * 24 * 14
	oldReleasesRetention     = time.Hour * 24 * 90
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
		{
			Name: "releases",
			Do: func(ctx context.Context) error {
				_, err := queries.DeleteOldReleases(ctx, now().Add(-oldReleasesRetention))
				return err
			},
		},
	}
}
