package inbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
)

type Deps struct {
	Logger         *slog.Logger
	RabbitConsumer rabbitmq.RabbitJSONConsumer
	DB             *sql.DB
	Queries        *database.Queries
}

type inboxWorker struct {
	logger   *slog.Logger
	db       *sql.DB
	q        *database.Queries
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
	return w.consumer.ConsumeJSON(ctx, rabbitmq.QueueSubscriptionInbox, w.handler)
}

func (w *inboxWorker) handler(msg rabbitmq.EnvelopeJSON) (rabbitmq.AckType, error) {
	switch msg.EventType {

	case rabbitmq.EventRepositoryRemoved:
		var ev rabbitmq.EventRepositoryRemovedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}

		ctx := context.Background()
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		qtx := w.q.WithTx(tx)
		if err := qtx.MarkRepositoryRemoved(context.Background(), ev.RepositoryID); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("update repo: %w", err)
		}
		if err := qtx.DeactivateSubsForRepoAndRemoveTokens(context.Background(), ev.RepositoryID); err != nil {
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
