package projection

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

type worker struct {
	logger   *slog.Logger
	q        *database.Queries
	consumer *rabbitmq.Consumer
}

func NewProjectionWorker(d Deps) (*worker, error) {
	consumer, err := rabbitmq.NewConsumer(d.Rabbit, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &worker{
		logger:   d.Logger.With("module", "projection_worker"),
		q:        d.Queries,
		consumer: consumer,
	}, nil
}

func (w *worker) Start(ctx context.Context) {
	w.logger.Info("starting")
	w.consumer.ConsumeJSON(ctx,
		rabbitmq.GetQueueName(rabbitmq.EventRepositoryChanged),
		func(
			_ context.Context,
			msg rabbitmq.EnvelopeJSON,
			_ rabbitmq.EventMeta,
		) (rabbitmq.AckType, error) {
			w.logger.Info("new event")

			if msg.EventType != rabbitmq.EventRepositoryChanged {
				return rabbitmq.NackDiscard, fmt.Errorf(
					"unexpected event type %q, expected %q", msg.EventType, rabbitmq.EventRepositoryChanged,
				)
			}

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
		},
	)
}
