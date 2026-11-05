package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"repo-watch/internal/outbox"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
)

var validEventTypes = []string{
	rabbitmq.EventEmailQueued,
	rabbitmq.EventSubscriptionChanged,
	rabbitmq.EventRepositoryChanged,
}

func getEventID(rowID int32) string {
	return fmt.Sprintf("subscription:outbox:%v", rowID)
}

func validateEventType(eventType string) (invalidReason string) {
	if !slices.Contains(validEventTypes, eventType) {
		return fmt.Sprintf("invalid event type: %q", eventType)
	}
	return ""
}

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
			ID:            row.ID,
			EventType:     row.EventType,
			EventID:       getEventID(row.ID),
			InvalidReason: validateEventType(row.EventType),
			CreatedAt:     row.CreatedAt,
			Payload:       row.Payload,
			Attempts:      row.Attempts,
		}
	}

	return out, nil
}

func (s *subscriptionOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

func (s *subscriptionOutbox) RecordFailure(
	ctx context.Context,
	id int32,
	lastError string,
	terminal bool,
	maxAttempts int32,
) (outbox.Failure, error) {
	res, err := s.q.RecordOutboxFailure(ctx, database.RecordOutboxFailureParams{
		ID:          id,
		LastError:   lastError,
		Terminal:    terminal,
		MaxAttempts: maxAttempts,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return outbox.Failure{Terminal: true}, nil
	}
	if err != nil {
		return outbox.Failure{}, err
	}

	return outbox.Failure{
		Attempts: res.Attempts,
		Terminal: res.FailedAt.Valid,
	}, nil
}

func NewMetrics(reg prometheus.Registerer) *outbox.Metrics {
	return outbox.NewMetrics(reg)
}

type Config outbox.DispatcherConfig

type Deps struct {
	Cfg     Config
	Logger  *slog.Logger
	Pub     rabbitmq.RabbitJSONPublisher
	Metrics outbox.MetricsRecorder
	Queries *database.Queries
}

func NewDispatcher(d Deps) (*outbox.Dispatcher, error) {
	store := &subscriptionOutbox{q: d.Queries}

	ddeps := outbox.DispatcherDeps{
		Config:    outbox.DispatcherConfig(d.Cfg),
		Logger:    d.Logger.With("module", "subscription_outbox"),
		Publisher: d.Pub,
		Store:     store,
		Metrics:   d.Metrics,
	}

	return outbox.NewDispatcher(ddeps)
}
