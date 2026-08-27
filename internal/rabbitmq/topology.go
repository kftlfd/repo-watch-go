package rabbitmq

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

const (
	exchangeDefault    = ""
	exchangeDeadLetter = "repo-watch.dlx"

	queueDeadLetter = "repo-watch.dlq"
)

func GetQueueName(eventName string) string {
	return "repo-watch." + eventName
}

func EnsureTopology(conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	defer ch.Close()

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

	queues := []string{
		GetQueueName(EventSubscriptionChanged),
		GetQueueName(EventRepositoryChanged),
		GetQueueName(EventReleaseDetected),
		GetQueueName(EventEmailQueued),
	}

	for _, queueName := range queues {
		if _, err = ch.QueueDeclare(
			queueName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			amqp091.Table{
				"x-queue-type":           amqp091.QueueTypeQuorum,
				"x-dead-letter-exchange": exchangeDeadLetter,
				// "x-delivery-limit":      int32(10),
			},
		); err != nil {
			return fmt.Errorf("declare queue %q: %w", queueName, err)
		}
	}

	return nil
}
