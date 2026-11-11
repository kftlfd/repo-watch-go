package inbox

import (
	"context"
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
	return w.consumer.ConsumeJSON(ctx, rabbitmq.QueueNotificationInbox, w.handler)
}

func (w *inboxWorker) handler(msg events.Envelope) (rabbitmq.AckType, error) {
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		return rabbitmq.NackDiscard, err
	}

	switch msg.EventType {

	case events.EventSubscriptionChanged:
		var ev events.EventSubscriptionChangedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		if err := w.q.CreateOrUpdateSubscription(context.Background(), dbGen.CreateOrUpdateSubscriptionParams{
			Email:         ev.Email,
			RepositoryID:  ev.RepositoryID,
			Active:        ev.Active,
			ActiveVersion: ev.Version,
			UnsubToken:    ev.UnsubToken,
		}); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("update sub: %w", err)
		}
		return rabbitmq.Ack, nil

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
		if err := w.q.CreateNewReleaseOutboxNotifications(context.Background(), dbGen.CreateNewReleaseOutboxNotificationsParams{
			RepositoryID: ev.RepositoryID,
			Tag:          ev.Tag,
			Payload:      payload,
		}); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("create notifications: %w", err)
		}
		return rabbitmq.Ack, nil

	case events.EventRepositoryRemoved:
		var ev events.EventRepositoryRemovedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid payload")
		}
		payload, err := json.Marshal(outbox.RepoRemovedPayload{})
		if err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("marshal payload: %w", err)
		}
		if err := w.q.DeactivateSubsForRepoAndNotifySubs(context.Background(), dbGen.DeactivateSubsForRepoAndNotifySubsParams{
			RepositoryID: ev.RepositoryID,
			Tag:          "repo_removed:" + msg.EventID,
			Payload:      payload,
		}); err != nil {
			return rabbitmq.NackRequeue, fmt.Errorf("deactivate repo: %w", err)
		}
		return rabbitmq.Ack, nil

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}
