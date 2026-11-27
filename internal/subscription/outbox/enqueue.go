package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"repo-watch/internal/events"
	"repo-watch/internal/observability"
	"repo-watch/internal/subscription/db/dbGen"
)

// NewEmail enqueues an email.queued event in the current transaction.
func NewEmail(ctx context.Context, qtx *dbGen.Queries, payload events.EventEmailQueuedPayload) error {
	return enqueue(ctx, qtx, events.EventEmailQueued, payload)
}

// NewSubscriptionChanged enqueues a subscription.changed event in the current
// transaction.
func NewSubscriptionChanged(ctx context.Context, qtx *dbGen.Queries, payload events.EventSubscriptionChangedPayload) error {
	return enqueue(ctx, qtx, events.EventSubscriptionChanged, payload)
}

// NewRepositoryChanged enqueues a repository.changed event in the current
// transaction.
func NewRepositoryChanged(ctx context.Context, qtx *dbGen.Queries, payload events.EventRepositoryChangedPayload) error {
	return enqueue(ctx, qtx, events.EventRepositoryChanged, payload)
}

func enqueue(ctx context.Context, qtx *dbGen.Queries, eventType string, payload any) error {
	eventVersion, ok := events.CurrentVersion(eventType)
	if !ok {
		return fmt.Errorf("unknown event type: %q", eventType)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", eventType, err)
	}

	correlationID := observability.GetCorrelationID(ctx)
	traceParent, _ := observability.GetTraceParent(ctx)

	if _, err = qtx.CreateOutboxRow(ctx, dbGen.CreateOutboxRowParams{
		EventType:     eventType,
		EventVersion:  eventVersion,
		Payload:       data,
		CorrelationID: correlationID,
		TraceParent:   traceParent,
	}); err != nil {
		return fmt.Errorf("enqueue %s event: %w", eventType, err)
	}

	return nil
}
