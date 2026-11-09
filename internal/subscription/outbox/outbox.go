package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"repo-watch/internal/dispatcher"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/db/dbGen"
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
	q *dbGen.Queries
}

func NewSubsctiptionOutbox(queries *dbGen.Queries) dispatcher.Store {
	return &subscriptionOutbox{q: queries}
}

func (s *subscriptionOutbox) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]dispatcher.Row, error) {
	res, err := s.q.GetOutboxBatch(ctx, dbGen.GetOutboxBatchParams{
		Limit: batchSize,
		LeasedUntil: sql.NullTime{
			Valid: true,
			Time:  leasedUntil,
		},
	})
	if err != nil {
		return nil, err
	}

	out := make([]dispatcher.Row, len(res))
	for i, row := range res {
		out[i] = dispatcher.Row{
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
) (dispatcher.Failure, error) {
	res, err := s.q.RecordOutboxFailure(ctx, dbGen.RecordOutboxFailureParams{
		ID:          id,
		LastError:   lastError,
		Terminal:    terminal,
		MaxAttempts: maxAttempts,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return dispatcher.Failure{Terminal: true}, nil
	}
	if err != nil {
		return dispatcher.Failure{}, err
	}

	return dispatcher.Failure{
		Attempts: res.Attempts,
		Terminal: res.FailedAt.Valid,
	}, nil
}

func (s *subscriptionOutbox) PendingStats(ctx context.Context) (dispatcher.PendingStats, error) {
	row, err := s.q.GetOutboxPendingStats(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return dispatcher.PendingStats{}, nil
		}
		return dispatcher.PendingStats{}, err
	}

	return dispatcher.PendingStats{Count: row.Pending, Oldest: row.OldestCreatedAt}, nil
}
