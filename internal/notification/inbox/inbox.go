package inbox

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
	w.consumer.ConsumeJSON(ctx, rabbitmq.QueueNotificationInbox, w.handler)
}

func (w *inboxWorker) handler(msg rabbitmq.EnvelopeJSON) (rabbitmq.AckType, error) {
	switch msg.EventType {

	case rabbitmq.EventSubscriptionChanged:
		var ev rabbitmq.EventSubscriptionChangedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		if err := w.q.CreateOrUpdateSubscription(context.Background(), database.CreateOrUpdateSubscriptionParams{
			Email:         ev.Email,
			RepositoryID:  ev.RepositoryID,
			Active:        ev.Active,
			ActiveVersion: ev.Version,
			UnsubToken:    ev.UnsubToken,
		}); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("update sub: %w", err)
		}
		return rabbitmq.Ack, nil

	case rabbitmq.EventReleaseDetected:
		var ev rabbitmq.EventReleaseDetectedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		if err := w.q.CreateNewReleaseOutboxNotifications(context.Background(), database.CreateNewReleaseOutboxNotificationsParams{
			RepositoryID: ev.RepositoryID,
			Owner:        ev.Owner,
			Name:         ev.Name,
			Tag:          ev.Tag,
		}); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("create notifications: %w", err)
		}
		return rabbitmq.Ack, nil

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}
