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

type NewReleasePayload struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type RepoRemovedPayload struct{}

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
		switch row.Type {

		case "new_release":
			var data NewReleasePayload
			if err := json.Unmarshal(row.Payload, &data); err != nil {
				return nil, fmt.Errorf("invalid payload: %w", err)
			}

			emailData, err := json.Marshal(emails.EmailNewReleaseNotificationPayload{
				RepoID:     row.RepositoryID,
				Owner:      data.Owner,
				Name:       data.Name,
				Tag:        row.Tag,
				UnsubToken: row.UnsubToken,
			})
			if err != nil {
				return nil, err
			}

			eventPayload, err := json.Marshal(rabbitmq.EventEmailQueuedPayload{
				Recipient: row.Email,
				Type:      emails.EmailNewReleaseNotification,
				Data:      emailData,
			})
			if err != nil {
				return nil, err
			}

			out[i] = outbox.Row{
				ID:        row.ID,
				EventType: rabbitmq.EventEmailQueued,
				CreatedAt: row.CreatedAt,
				Payload:   eventPayload,
			}

		case "repo_removed":
			var data RepoRemovedPayload
			if err := json.Unmarshal(row.Payload, &data); err != nil {
				return nil, fmt.Errorf("invalid payload: %w", err)
			}

			emailData, err := json.Marshal(emails.EmailRepositoryRemovedPayload{
				RepoID: row.RepositoryID,
			})
			if err != nil {
				return nil, err
			}

			eventPayload, err := json.Marshal(rabbitmq.EventEmailQueuedPayload{
				Recipient: row.Email,
				Type:      emails.EmailNewReleaseNotification,
				Data:      emailData,
			})
			if err != nil {
				return nil, err
			}

			out[i] = outbox.Row{
				ID:        row.ID,
				EventType: rabbitmq.EventEmailQueued,
				CreatedAt: row.CreatedAt,
				Payload:   eventPayload,
			}

		default:
			return nil, fmt.Errorf("unexpected outbox event type: %q", row.Type)
		}

	}

	return out, nil
}

func (s *scannerOutbox) MarkPublished(ctx context.Context, ids []int32) error {
	return s.q.MarkOutboxPublished(ctx, ids)
}

var validEventTypes = []string{
	rabbitmq.EventEmailQueued,
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
