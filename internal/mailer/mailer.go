package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"repo-watch/internal/emails"
	"repo-watch/internal/rabbitmq"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type Deps struct {
	Logger *slog.Logger
	Rabbit *amqp091.Connection
}

type mailerWorker struct {
	logger   *slog.Logger
	consumer *rabbitmq.Consumer
}

func NewMailer(d Deps) (*mailerWorker, error) {
	consumer, err := rabbitmq.NewConsumer(d.Rabbit, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &mailerWorker{
		logger:   d.Logger.With("module", "mailer_worker"),
		consumer: consumer,
	}, nil
}

func (m *mailerWorker) Start(ctx context.Context) {
	m.logger.Info("starting")
	m.consumer.ConsumeJSON(ctx, rabbitmq.QueueMailerInbox, m.handler)
}

func (m *mailerWorker) handler(msg rabbitmq.EnvelopeJSON) (rabbitmq.AckType, error) {
	switch msg.EventType {

	case rabbitmq.EventEmailQueued:
		var ev rabbitmq.EventEmailQueuedPayload
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return rabbitmq.NackDiscard, fmt.Errorf("invalid JSON payload: %w", err)
		}
		email, err := renderEmail(ev)
		if err != nil {
			return rabbitmq.NackDiscard, err
		}
		printEmail(ev.Recipient, email)
		return rabbitmq.Ack, nil

	default:
		return rabbitmq.NackDiscard, fmt.Errorf("unexpected event type %q", msg.EventType)

	}
}

type Email struct {
	subject, body string
}

var ErrInvalidPayloadData = errors.New("invalid payload data")

func renderEmail(payload rabbitmq.EventEmailQueuedPayload) (Email, error) {
	switch payload.Type {

	case emails.EmailConfirmSubscription:
		var data emails.EmailConfirmSubscriptionData
		if err := json.Unmarshal(payload.Data, &data); err != nil {
			return Email{}, ErrInvalidPayloadData
		}
		return Email{
			subject: fmt.Sprintf("Confirm subscription to: %v", data.RepoID),
			body:    fmt.Sprintf("To confirm the subscription use this token: %v", data.ConfirmToken),
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
			body:    fmt.Sprintf("Go check on GH, idk.\nUnsubscribe token: %v", data.UnsubToken),
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

	default:
		return Email{}, fmt.Errorf("unexpected email type: %q", payload.Type)
	}
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
