package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"repo-watch/internal/emails"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/outbox"
	"repo-watch/internal/rabbitmq"
	"slices"
	"time"
)

type scannerOutbox struct {
	q *database.Queries
}

func (s *scannerOutbox) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]outbox.Row, error) {
	res, err := s.q.GetNotificationsBatch(ctx, database.GetNotificationsBatchParams{
		Limit: batchSize,
		LeasedUntil: sql.NullTime{
			Valid: true,
			Time:  leasedUntil,
		},
	})
	if err != nil {
		return nil, err
	}

	out := make([]outbox.Row, len(res))
	for i, row := range res {
		payload, err := json.Marshal(rabbitmq.EventEmailQueuedPayload{
			Recipient: row.Email,
			Type:      emails.EmailNewReleaseNotification,
			Data: emails.EmailNewReleaseNotificationPayload{
				RepoID: row.RepositoryID,
				Owner:  row.Owner,
				Name:   row.Name,
				Tag:    row.Tag,
			},
		})
		if err != nil {
			return nil, err
		}

		out[i] = outbox.Row{
			ID:        row.ID,
			EventType: rabbitmq.EventEmailQueued,
			CreatedAt: row.CreatedAt,
			Payload:   payload,
		}
	}

	return out, nil
}

func (s *scannerOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

var validEventTypes = []string{
	rabbitmq.EventReleaseDetected,
}

type Config outbox.DispatcherConfig

type Deps struct {
	Cfg     Config
	Logger  *slog.Logger
	Pub     rabbitmq.RabbitJSONPublisher
	Queries *database.Queries
}

func NewDispatcher(d Deps) (*outbox.Dispatcher, error) {
	store := &scannerOutbox{q: d.Queries}

	ddeps := outbox.DispatcherDeps{
		Config:    outbox.DispatcherConfig(d.Cfg),
		Logger:    d.Logger.With("module", "notification_outbox"),
		Publisher: d.Pub,
		Store:     store,
		GetEventID: func(r outbox.Row) string {
			return fmt.Sprintf("notification:outbox:%v", r.ID)
		},
		ValidateEventType: func(eventType string) bool {
			return slices.Contains(validEventTypes, eventType)
		},
	}

	return outbox.NewDispatcher(ddeps)
}
