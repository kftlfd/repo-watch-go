package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"repo-watch/internal/emails"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/outbox"
	"repo-watch/internal/rabbitmq"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func getEventID(rowID int32) string {
	return fmt.Sprintf("notification:outbox:%v", rowID)
}

type NewReleasePayload struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type RepoRemovedPayload struct{}

func getEventPayload(row database.Outbox) (eventType string, payload json.RawMessage, err error) {
	switch row.Type {

	case "new_release":
		var data NewReleasePayload
		if err := json.Unmarshal(row.Payload, &data); err != nil {
			return "", nil, fmt.Errorf("invalid payload: %w", err)
		}

		emailData, err := json.Marshal(emails.EmailNewReleaseNotificationPayload{
			RepoID:     row.RepositoryID,
			Owner:      data.Owner,
			Name:       data.Name,
			Tag:        row.Tag,
			UnsubToken: row.UnsubToken,
		})
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal email data: %w", err)
		}

		eventPayload, err := json.Marshal(rabbitmq.EventEmailQueuedPayload{
			Recipient: row.Email,
			Type:      emails.EmailNewReleaseNotification,
			Data:      emailData,
		})
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal event payload: %w", err)
		}

		return rabbitmq.EventEmailQueued, eventPayload, nil

	case "repo_removed":
		var data RepoRemovedPayload
		if err := json.Unmarshal(row.Payload, &data); err != nil {
			return "", nil, fmt.Errorf("invalid payload: %w", err)
		}

		emailData, err := json.Marshal(emails.EmailRepositoryRemovedPayload{
			RepoID: row.RepositoryID,
		})
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal email data: %w", err)
		}

		eventPayload, err := json.Marshal(rabbitmq.EventEmailQueuedPayload{
			Recipient: row.Email,
			Type:      emails.EmailRepositoryRemoved,
			Data:      emailData,
		})
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal event payload: %w", err)
		}

		return rabbitmq.EventEmailQueued, eventPayload, nil

	default:
		return "", nil, fmt.Errorf("invalid event type: %q", row.Type)
	}
}

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

	out := make([]outbox.Row, 0, len(res))

	for _, row := range res {
		outboxRow := outbox.Row{
			ID:            row.ID,
			EventType:     row.Type,
			EventID:       getEventID(row.ID),
			CreatedAt:     row.CreatedAt,
			Payload:       row.Payload,
			Attempts:      row.Attempts,
			InvalidReason: "",
		}

		eventType, payload, err := getEventPayload(row)
		if err != nil {
			outboxRow.InvalidReason = err.Error()
		} else {
			outboxRow.EventType = eventType
			outboxRow.Payload = payload
		}

		out = append(out, outboxRow)
	}

	return out, nil
}

func (s *scannerOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

func (s *scannerOutbox) RecordFailure(
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
	store := &scannerOutbox{q: d.Queries}

	ddeps := outbox.DispatcherDeps{
		Config:    outbox.DispatcherConfig(d.Cfg),
		Logger:    d.Logger.With("module", "notification_outbox"),
		Publisher: d.Pub,
		Store:     store,
		Metrics:   d.Metrics,
	}

	return outbox.NewDispatcher(ddeps)
}
