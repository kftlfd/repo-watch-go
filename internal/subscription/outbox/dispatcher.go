package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"repo-watch/internal/outbox"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
	"slices"
	"time"
)

type subscriptionOutbox struct {
	q *database.Queries
}

func (s *subscriptionOutbox) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]outbox.Row, error) {
	res, err := s.q.GetOutboxBatch(ctx, database.GetOutboxBatchParams{
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
		out[i] = outbox.Row{
			ID:        row.ID,
			EventType: row.EventType,
			CreatedAt: row.CreatedAt,
			Payload:   row.Payload,
		}
	}

	return out, nil
}

func (s *subscriptionOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

var validEventTypes = []string{
	rabbitmq.EventEmailQueued,
	rabbitmq.EventSubscriptionChanged,
	rabbitmq.EventRepositoryChanged,
}

type Config outbox.DispatcherConfig

type Deps struct {
	Cfg     Config
	Logger  *slog.Logger
	Pub     rabbitmq.RabbitJSONPublisher
	Queries *database.Queries
}

func NewDispatcher(d Deps) (*outbox.Dispatcher, error) {
	store := &subscriptionOutbox{q: d.Queries}

	ddeps := outbox.DispatcherDeps{
		Config:    outbox.DispatcherConfig(d.Cfg),
		Logger:    d.Logger.With("module", "subscription_outbox"),
		Publisher: d.Pub,
		Store:     store,
		GetEventID: func(r outbox.Row) string {
			return fmt.Sprintf("subscription:outbox:%v", r.ID)
		},
		ValidateEventType: func(eventType string) bool {
			return slices.Contains(validEventTypes, eventType)
		},
	}

	return outbox.NewDispatcher(ddeps)
}
