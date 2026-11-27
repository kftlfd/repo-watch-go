package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"repo-watch/internal/events"
	"repo-watch/internal/observability"
	"repo-watch/internal/scanner/db/dbGen"
)

// NewReleaseDetected enqueues a release.detected event in the current
// transaction.
func NewReleaseDetected(ctx context.Context, qtx *dbGen.Queries, payload events.EventReleaseDetectedPayload) error {
	return enqueue(ctx, qtx, events.EventReleaseDetected, payload)
}

// NewRepositoryRemoved enqueues a repository.removed event in the current
// transaction.
func NewRepositoryRemoved(ctx context.Context, qtx *dbGen.Queries, payload events.EventRepositoryRemovedPayload) error {
	return enqueue(ctx, qtx, events.EventRepositoryRemoved, payload)
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
