package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/rabbitmq"

	"github.com/rabbitmq/amqp091-go"
)

type Deps struct {
	Logger  *slog.Logger
	Rabbit  *amqp091.Connection
	Queries *database.Queries
}

type projectionWorker struct {
	logger   *slog.Logger
	q        *database.Queries
	consumer *rabbitmq.Consumer
}

func NewProjectionWorker(d Deps) (*projectionWorker, error) {
	consumer, err := rabbitmq.NewConsumer(d.Rabbit, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &projectionWorker{
		logger:   d.Logger.With("module", "projection_worker"),
		q:        d.Queries,
		consumer: consumer,
	}, nil
}

func (w *projectionWorker) Start(ctx context.Context) {
	w.logger.Info("starting")

	w.consumer.ConsumeJSON(ctx,
		rabbitmq.GetQueueName(rabbitmq.EventRepositoryChanged),
		func(
			_ context.Context,
			msg rabbitmq.EnvelopeJSON,
			_ rabbitmq.EventMeta,
		) (rabbitmq.AckType, error) {
			w.logger.Info("new event")

			if msg.EventType != rabbitmq.EventSubscriptionChanged {
				return rabbitmq.NackDiscard, fmt.Errorf(
					"unexpected event type %q, expected %q", msg.EventType, rabbitmq.EventSubscriptionChanged,
				)
			}

			var ev rabbitmq.EventSubscriptionChangedPayload
			if err := json.Unmarshal(msg.Payload, &ev); err != nil {
				return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
			}

			err := w.q.CreateOrUpdateSubscription(context.Background(), database.CreateOrUpdateSubscriptionParams{
				Email:         ev.Email,
				RepositoryID:  ev.RepositoryID,
				Active:        ev.Active,
				ActiveVersion: ev.Version,
				UnsubToken:    ev.UnsubToken,
			})
			if err != nil {
				return rabbitmq.NackRequeue, fmt.Errorf("update sub: %w", err)
			}

			return rabbitmq.Ack, nil
		},
	)
}
