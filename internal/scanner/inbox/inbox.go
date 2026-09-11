package inbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/database"

	"github.com/rabbitmq/amqp091-go"
)

type Deps struct {
	Logger  *slog.Logger
	Rabbit  *amqp091.Connection
	Queries *database.Queries
}

type inboxWorker struct {
	logger   *slog.Logger
	q        *database.Queries
	consumer *rabbitmq.Consumer
}

func NewInboxWorker(d Deps) (*inboxWorker, error) {
	consumer, err := rabbitmq.NewConsumer(d.Rabbit, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &inboxWorker{
		logger:   d.Logger.With("module", "inbox_worker"),
		q:        d.Queries,
		consumer: consumer,
	}, nil
}

func (w *inboxWorker) Start(ctx context.Context) {
	w.logger.Info("starting")
	w.consumer.ConsumeJSON(ctx, rabbitmq.QueueScannerInbox, w.handler)
}

func (w *inboxWorker) handler(msg rabbitmq.EnvelopeJSON) (rabbitmq.AckType, error) {
	switch msg.EventType {

	case rabbitmq.EventRepositoryChanged:
		var ev rabbitmq.EventRepositoryChangedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}

		err := w.q.CreateOrUpdateRepo(context.Background(), database.CreateOrUpdateRepoParams{
			ID:            ev.RepositoryID,
			Owner:         ev.Owner,
			Name:          ev.Name,
			LatestSeenTag: ev.LatestSeenTag,
			Active:        ev.Active,
			ActiveVersion: ev.ActiveVersion,
		})
		if err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("update repo: %w", err)
		}

		return rabbitmq.Ack, nil

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}
