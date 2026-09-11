package rabbitmq

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

const (
	exchangeDeadLetter = "repo-watch.dlx"
	ExchangeTopic      = "repo-watch.topic"

	queueDeadLetter        = "repo-watch.dlq"
	QueueSubscriptionInbox = "repo-watch.subscription.inbox"
	QueueScannerInbox      = "repo-watch.scanner.inbox"
	QueueNotificationInbox = "repo-watch.notification.inbox"
	QueueMailerInbox       = "repo-watch.mailer.inbox"
)

var queuesDef = []struct {
	name        string
	bindingKeys []string
}{
	{
		name:        QueueSubscriptionInbox,
		bindingKeys: []string{EventRepositoryRemoved},
	},
	{
		name:        QueueScannerInbox,
		bindingKeys: []string{EventRepositoryChanged},
	},
	{
		name:        QueueNotificationInbox,
		bindingKeys: []string{EventSubscriptionChanged, EventRepositoryRemoved, EventReleaseDetected},
	},
	{
		name:        QueueMailerInbox,
		bindingKeys: []string{EventEmailQueued},
	},
}

func EnsureTopology(conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(
		ExchangeTopic,
		amqp091.ExchangeTopic,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare topic exchange: %w", err)
	}

	if err = ch.ExchangeDeclare(
		exchangeDeadLetter,
		amqp091.ExchangeFanout,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare DLX: %w", err)
	}

	if _, err = ch.QueueDeclare(
		queueDeadLetter,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare DLQ: %w", err)
	}

	if err = ch.QueueBind(
		queueDeadLetter,
		"",
		exchangeDeadLetter,
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("bind DLQ: %w", err)
	}

	for _, qDef := range queuesDef {
		if _, err = ch.QueueDeclare(
			qDef.name,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			amqp091.Table{
				"x-queue-type":           amqp091.QueueTypeQuorum,
				"x-dead-letter-exchange": exchangeDeadLetter,
				// "x-delivery-limit":    int32(10),
			},
		); err != nil {
			return fmt.Errorf("declare queue %q: %w", qDef.name, err)
		}

		for _, bKey := range qDef.bindingKeys {
			if err = ch.QueueBind(
				qDef.name,
				bKey,
				ExchangeTopic,
				false, // no-wait
				nil,
			); err != nil {
				return fmt.Errorf("bind queue %q to exchange %q with key %q: %w", qDef.name, ExchangeTopic, bKey, err)
			}
		}
	}

	return nil
}
