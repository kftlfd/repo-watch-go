package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
)

type Deps struct {
	Logger         *slog.Logger
	RabbitConsumer rabbitmq.RabbitJSONConsumer
	EmailMetrics   EmailMetricsRecorder
	BaseURL        string
}

type mailerWorker struct {
	logger   *slog.Logger
	consumer rabbitmq.RabbitJSONConsumer
	metrics  EmailMetricsRecorder
	baseURL  string
}

func NewMailer(d Deps) (*mailerWorker, error) {
	if d.RabbitConsumer == nil {
		return nil, fmt.Errorf("missing RabbitConsumer")
	}
	if d.Logger == nil {
		return nil, fmt.Errorf("missing logger")
	}
	if d.EmailMetrics == nil {
		d.EmailMetrics = noopEmailMetrics{}
	}

	return &mailerWorker{
		logger:   d.Logger.With("module", "mailer_worker"),
		consumer: d.RabbitConsumer,
		metrics:  d.EmailMetrics,
		baseURL:  strings.TrimRight(d.BaseURL, "/"),
	}, nil
}

func (m *mailerWorker) Start(ctx context.Context) error {
	m.logger.Info("starting")
	return m.consumer.ConsumeJSON(ctx, rabbitmq.QueueMailerInbox, m.handler)
}

func (m *mailerWorker) handler(msg events.Envelope) (rabbitmq.AckType, error) {
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		m.metrics.Failed("unsupported_event")
		return rabbitmq.NackDiscard, err
	}

	switch msg.EventType {

	case events.EventEmailQueued:
		var ev events.EventEmailQueuedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			m.metrics.Failed("invalid_payload")
			return rabbitmq.NackDiscard, fmt.Errorf("invalid JSON payload: %w", err)
		}
		email, err := renderEmail(ev, m.baseURL)
		if err != nil {
			m.metrics.Failed(ev.Type)
			return rabbitmq.NackDiscard, err
		}
		printEmail(ev.Recipient, email)
		m.metrics.Rendered(ev.Type)
		return rabbitmq.Ack, nil

	default:
		m.metrics.Failed("unknown_event")
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}

type Email struct {
	subject, body string
}

var ErrInvalidPayloadData = errors.New("invalid payload data")

func renderEmail(payload events.EventEmailQueuedPayload, baseURL string) (Email, error) {
	switch payload.Type {

	case emails.EmailConfirmSubscription:
		var data emails.EmailConfirmSubscriptionData
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("Confirm subscription to: %v", data.RepoID),
			body:    fmt.Sprintf("To confirm the subscription, open: %v", joinURL(baseURL, "/confirm/"+data.ConfirmToken)),
		}, nil

	case emails.EmailSubscribed:
		var data emails.EmailSubscribedData
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("You subscribed to: %v", data.RepoID),
			body:    fmt.Sprintf("You will receive notification emails when repo %q has new releases", data.RepoID),
		}, nil

	case emails.EmailNewReleaseNotification:
		var data emails.EmailNewReleaseNotificationPayload
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("New release for %v: %v", data.RepoID, data.Tag),
			body:    fmt.Sprintf("Go check on GH, idk.\nUnsubscribe: %v", joinURL(baseURL, "/unsubscribe/"+data.UnsubToken)),
		}, nil

	case emails.EmailUnsubscribed:
		var data emails.EmailUnsubscribedData
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("Unsubscribed from: %v", data.RepoID),
			body:    fmt.Sprintf("You will not be notified about new releases of %v", data.RepoID),
		}, nil

	case emails.EmailRepositoryRemoved:
		var data emails.EmailRepositoryRemovedPayload
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("Repository was removed: %v", data.RepoID),
			body:    fmt.Sprintf("Repo was deleted by owner. You will not receive any further notifications for %q", data.RepoID),
		}, nil

	default:
		return Email{}, fmt.Errorf("unexpected email type: %q", payload.Type)
	}
}

func joinURL(baseURL, path string) string {
	return strings.TrimRight(baseURL, "/") + path
}

func printEmail(recipient string, email Email) {
	lines := []string{
		"==========",
		fmt.Sprintf("To:      %v", recipient),
		fmt.Sprintf("Subject: %v", email.subject),
		"",
		email.body,
		"==========",
	}

	fmt.Println(strings.Join(lines, "\n"))
}
