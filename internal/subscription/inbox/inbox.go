package inbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/db/dbGen"
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

	return &inboxWorker{
		logger:   d.Logger.With("module", "inbox_worker"),
		db:       d.DB,
		q:        d.Queries,
		consumer: d.RabbitConsumer,
	}, nil
}

func (w *inboxWorker) Start(ctx context.Context) error {
	w.logger.Info("starting")
	return w.consumer.ConsumeJSON(ctx, rabbitmq.QueueSubscriptionInbox, w.handler, nil)
}

func (w *inboxWorker) handler(ctx context.Context, msg events.Envelope) (rabbitmq.AckType, error) {
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		return rabbitmq.NackDiscard, err
	}

	switch msg.EventType {

	case events.EventRepositoryRemoved:
		var ev events.EventRepositoryRemovedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}

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

		if err := qtx.MarkRepositoryRemoved(ctx, ev.RepositoryID); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("update repo: %w", err)
		}
		if err := qtx.DeactivateSubscriptionsForRepoAndRemoveSubscriptionTokens(ctx, ev.RepositoryID); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("deactivate subs: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("commit tx: %w", err)
		}

		return rabbitmq.Ack, nil

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}
