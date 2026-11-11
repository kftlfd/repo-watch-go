package inbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/db/dbGen"
)

type Deps struct {
	Logger         *slog.Logger
	RabbitConsumer rabbitmq.RabbitJSONConsumer
	Queries        *dbGen.Queries
}

type inboxWorker struct {
	logger   *slog.Logger
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
		q:        d.Queries,
		consumer: d.RabbitConsumer,
	}, nil
}

func (w *inboxWorker) Start(ctx context.Context) error {
	w.logger.Info("starting")
	return w.consumer.ConsumeJSON(ctx, rabbitmq.QueueScannerInbox, w.handler)
}

func (w *inboxWorker) handler(msg events.Envelope) (rabbitmq.AckType, error) {
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		return rabbitmq.NackDiscard, err
	}

	switch msg.EventType {

	case events.EventRepositoryChanged:
		var ev events.EventRepositoryChangedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}

		err := w.q.CreateOrUpdateRepo(context.Background(), dbGen.CreateOrUpdateRepoParams{
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
