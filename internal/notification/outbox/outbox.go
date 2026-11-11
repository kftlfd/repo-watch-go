package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"repo-watch/internal/dispatcher"
	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/notification/db/dbGen"
)

func getEventID(rowID int32) string {
	return fmt.Sprintf("notification:outbox:%v", rowID)
}

type NewReleasePayload struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type RepoRemovedPayload struct{}

// getEventPayload maps an internal notification row to the event that is
// published. The event payload is built at dispatch time from current code, so
// its schema version is the current one rather than something stored with the
// row.
func getEventPayload(row dbGen.Outbox) (eventType string, eventVersion int32, payload json.RawMessage, err error) {
	switch row.Type {

	case "new_release":
		var data NewReleasePayload
		if err := json.Unmarshal(row.Payload, &data); err != nil {
			return "", 0, nil, fmt.Errorf("invalid payload: %w", err)
		}

		emailData, err := json.Marshal(emails.EmailNewReleaseNotificationPayload{
			RepoID:     row.RepositoryID,
			Owner:      data.Owner,
			Name:       data.Name,
			Tag:        row.Tag,
			UnsubToken: row.UnsubToken,
		})
		if err != nil {
			return "", 0, nil, fmt.Errorf("failed to marshal email data: %w", err)
		}

		eventPayload, err := json.Marshal(events.EventEmailQueuedPayload{
			Recipient: row.Email,
			Type:      emails.EmailNewReleaseNotification,
			Data:      emailData,
		})
		if err != nil {
			return "", 0, nil, fmt.Errorf("failed to marshal event payload: %w", err)
		}

		return events.EventEmailQueued, events.VersionEmailQueued, eventPayload, nil

	case "repo_removed":
		var data RepoRemovedPayload
		if err := json.Unmarshal(row.Payload, &data); err != nil {
			return "", 0, nil, fmt.Errorf("invalid payload: %w", err)
		}

		emailData, err := json.Marshal(emails.EmailRepositoryRemovedPayload{
			RepoID: row.RepositoryID,
		})
		if err != nil {
			return "", 0, nil, fmt.Errorf("failed to marshal email data: %w", err)
		}

		eventPayload, err := json.Marshal(events.EventEmailQueuedPayload{
			Recipient: row.Email,
			Type:      emails.EmailRepositoryRemoved,
			Data:      emailData,
		})
		if err != nil {
			return "", 0, nil, fmt.Errorf("failed to marshal event payload: %w", err)
		}

		return events.EventEmailQueued, events.VersionEmailQueued, eventPayload, nil

	default:
		return "", 0, nil, fmt.Errorf("invalid event type: %q", row.Type)
	}
}

type notificationOutbox struct {
	q *dbGen.Queries
}

func NewScannerOutbox(queries *dbGen.Queries) dispatcher.Store {
	return &notificationOutbox{q: queries}
}

func (s *notificationOutbox) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]dispatcher.Row, error) {
	res, err := s.q.GetNotificationsBatch(ctx, dbGen.GetNotificationsBatchParams{
		Limit: batchSize,
		LeasedUntil: sql.NullTime{
			Valid: true,
			Time:  leasedUntil,
		},
	})
	if err != nil {
		return nil, err
	}

	out := make([]dispatcher.Row, 0, len(res))

	for _, row := range res {
		outboxRow := dispatcher.Row{
			ID:            row.ID,
			EventType:     row.Type,
			EventID:       getEventID(row.ID),
			CreatedAt:     row.CreatedAt,
			Payload:       row.Payload,
			Attempts:      row.Attempts,
			InvalidReason: "",
		}

		eventType, eventVersion, payload, err := getEventPayload(row)
		if err != nil {
			outboxRow.InvalidReason = err.Error()
		} else {
			outboxRow.EventType = eventType
			outboxRow.Version = eventVersion
			outboxRow.Payload = payload
		}

		out = append(out, outboxRow)
	}

	return out, nil
}

func (s *notificationOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

func (s *notificationOutbox) RecordFailure(
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

func (s *notificationOutbox) PendingStats(ctx context.Context) (dispatcher.PendingStats, error) {
	row, err := s.q.GetOutboxPendingStats(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return dispatcher.PendingStats{}, nil
		}
		return dispatcher.PendingStats{}, err
	}

	return dispatcher.PendingStats{Count: row.Pending, Oldest: row.OldestCreatedAt}, nil
}
