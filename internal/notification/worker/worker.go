package worker

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

type notificationsWorker struct {
	logger   *slog.Logger
	q        *database.Queries
	consumer *rabbitmq.Consumer
}

func NewNotificationsWorker(d Deps) (*notificationsWorker, error) {
	consumer, err := rabbitmq.NewConsumer(d.Rabbit, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &notificationsWorker{
		logger:   d.Logger.With("module", "notifications_worker"),
		q:        d.Queries,
		consumer: consumer,
	}, nil
}

func (w *notificationsWorker) Start(ctx context.Context) {
	w.logger.Info("starting")

	w.consumer.ConsumeJSON(ctx,
		rabbitmq.GetQueueName(rabbitmq.EventRepositoryChanged),
		func(
			_ context.Context,
			msg rabbitmq.EnvelopeJSON,
			_ rabbitmq.EventMeta,
		) (rabbitmq.AckType, error) {
			if msg.EventType != rabbitmq.EventReleaseDetected {
				return rabbitmq.NackDiscard, fmt.Errorf(
					"unexpected event type %q, expected %q", msg.EventType, rabbitmq.EventReleaseDetected,
				)
			}

			var ev rabbitmq.EventReleaseDetectedPayload
			if err := json.Unmarshal(msg.Payload, &ev); err != nil {
				return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
			}

			err := w.q.CreateNewReleaseOutboxNotifications(context.Background(), database.CreateNewReleaseOutboxNotificationsParams{
				RepositoryID: ev.RepositoryID,
				Owner:        ev.Owner,
				Name:         ev.Name,
				Tag:          ev.Tag,
			})
			if err != nil {
				return rabbitmq.NackRequeue, fmt.Errorf("create notifications: %w", err)
			}

			return rabbitmq.Ack, nil
		},
	)
}
