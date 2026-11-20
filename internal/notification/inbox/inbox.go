package inbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"repo-watch/internal/events"
	"repo-watch/internal/notification/db/dbGen"
	"repo-watch/internal/notification/outbox"
	"repo-watch/internal/rabbitmq"
)

type Deps struct {
	Logger         *slog.Logger
	RabbitConsumer rabbitmq.RabbitJSONConsumer
	DB             *sql.DB
	Queries        *dbGen.Queries
}

type inboxWorker struct {
	logger   *slog.Logger
	db       *sql.DB
	q        *dbGen.Queries
	consumer rabbitmq.RabbitJSONConsumer
}

func NewInboxWorker(d Deps) (*inboxWorker, error) {
	if d.RabbitConsumer == nil {
		return nil, fmt.Errorf("missing RabbitConsumer")
	}
	if d.Logger == nil {
		return nil, fmt.Errorf("missing logger")
	}
	if d.DB == nil {
		return nil, fmt.Errorf("missing DB")
	}

	return &inboxWorker{
		logger:   d.Logger.With("module", "inbox_worker"),
		db:       d.DB,
		q:        d.Queries,
		consumer: d.RabbitConsumer,
	}, nil
}

func (w *inboxWorker) Start(ctx context.Context) error {
	w.logger.Info("starting")
	return w.consumer.ConsumeJSON(ctx, rabbitmq.QueueNotificationInbox, w.handler, nil)
}

func (w *inboxWorker) handler(ctx context.Context, msg events.Envelope) (rabbitmq.AckType, error) {
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		return rabbitmq.NackDiscard, err
	}

	switch msg.EventType {

	case events.EventSubscriptionChanged:
		var ev events.EventSubscriptionChangedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		return w.processEvent(ctx, msg, func(ctx context.Context, q *dbGen.Queries) error {
			if err := q.CreateOrUpdateSubscription(ctx, dbGen.CreateOrUpdateSubscriptionParams{
				Email:         ev.Email,
				RepositoryID:  ev.RepositoryID,
				Active:        ev.Active,
				ActiveVersion: ev.Version,
				UnsubToken:    ev.UnsubToken,
			}); err != nil {
				return fmt.Errorf("update sub: %w", err)
			}
			return nil
		})

	case events.EventReleaseDetected:
		var ev events.EventReleaseDetectedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		payload, err := json.Marshal(outbox.NewReleasePayload{
			Owner: ev.Owner,
			Name:  ev.Name,
		})
		if err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("marshal payload: %w", err)
		}
		return w.processEvent(ctx, msg, func(ctx context.Context, q *dbGen.Queries) error {
			if err := q.CreateNewReleaseOutboxNotifications(ctx, dbGen.CreateNewReleaseOutboxNotificationsParams{
				RepositoryID: ev.RepositoryID,
				Tag:          ev.Tag,
				Payload:      payload,
			}); err != nil {
				return fmt.Errorf("create notifications: %w", err)
			}
			return nil
		})

	case events.EventRepositoryRemoved:
		var ev events.EventRepositoryRemovedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		payload, err := json.Marshal(outbox.RepoRemovedPayload{})
		if err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("marshal payload: %w", err)
		}
		return w.processEvent(ctx, msg, func(ctx context.Context, q *dbGen.Queries) error {
			if err := q.DeactivateSubsForRepoAndNotifySubs(ctx, dbGen.DeactivateSubsForRepoAndNotifySubsParams{
				RepositoryID: ev.RepositoryID,
				Tag:          "repo_removed:" + msg.EventID,
				Payload:      payload,
			}); err != nil {
				return fmt.Errorf("deactivate repo: %w", err)
			}
			return nil
		})

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}

func (w *inboxWorker) processEvent(
	ctx context.Context,
	msg events.Envelope,
	fn func(context.Context, *dbGen.Queries) error,
) (rabbitmq.AckType, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return rabbitmq.NackRequeue, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := w.q.WithTx(tx)
	claimed, err := qtx.ClaimProcessedEvent(ctx, dbGen.ClaimProcessedEventParams{
		EventID:   msg.EventID,
		EventType: msg.EventType,
	})
	if err != nil {
		return rabbitmq.NackRequeue, fmt.Errorf("claim event: %w", err)
	}
	if claimed == 0 {
		if err := tx.Commit(); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("commit tx: %w", err)
		}
		return rabbitmq.Ack, nil
	}

	if err := fn(ctx, qtx); err != nil {
		return rabbitmq.NackRequeue, err
	}
	if err := tx.Commit(); err != nil {
		return rabbitmq.NackRequeue, fmt.Errorf("commit tx: %w", err)
	}

	return rabbitmq.Ack, nil
}
