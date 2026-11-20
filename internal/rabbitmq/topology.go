package rabbitmq

import (
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"repo-watch/internal/events"
)

const (
	exchangeDeadLetter = "repo-watch.dlx"
	ExchangeTopic      = "repo-watch.topic"

	alternateExchangeArg = "alternate-exchange"

	queueDeadLetter        = "repo-watch.dlq"
	QueueSubscriptionInbox = "repo-watch.subscription.inbox"
	QueueScannerInbox      = "repo-watch.scanner.inbox"
	QueueNotificationInbox = "repo-watch.notification.inbox"
	QueueMailerInbox       = "repo-watch.mailer.inbox"

	inboxDeliveryLimit = 10

	// retryCountHeader carries the number of delayed retries already
	// scheduled for an inbox message. It survives the retry queue TTL and is
	// authoritative for choosing the next backoff tier and the retry limit.
	retryCountHeader = "x-retry-count"

	// DLQMessageTTL is how long dead letters are kept for investigation.
	// The TTL is applied when the queue is first declared; changing it requires
	// deleting and redeclaring the queue (or using a RabbitMQ policy instead).
	DLQMessageTTL = 30 * 24 * time.Hour
	dlqMaxLength  = 10_000
)

// retryTier is one delayed retry queue for an inbox.
type retryTier struct {
	Name  string
	Delay time.Duration
}

// retryTiers defines the backoff schedule for failed inbox deliveries. The
// first failure is retried after the first tier, the second failure after the
// second tier, and every later failure keeps using the last tier until the
// retry limit is exhausted.
//
// To add another retry step:
//  1. Add an entry below with a new, stable Name and the desired Delay.
//  2. Restart the services. EnsureTopology declares a queue for every tier of
//     every inbox, so no other wiring is needed.
//  3. Do not change the Delay or Name of an existing tier. RabbitMQ rejects a
//     redeclare with different queue arguments (PRECONDITION_FAILED). To change
//     a delay, add a new tier and delete the old queue after it has drained.
var retryTiers = []retryTier{
	{Name: "30s", Delay: 30 * time.Second},
	{Name: "10m", Delay: 10 * time.Minute},
}

func retryQueueName(inbox string, tier retryTier) string {
	return fmt.Sprintf("%s.retry.%s", inbox, tier.Name)
}

func retryQueueArgs(tier retryTier, inbox string) amqp091.Table {
	return amqp091.Table{
		"x-queue-type":              amqp091.QueueTypeQuorum,
		"x-message-ttl":             int64(tier.Delay / time.Millisecond),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": inbox,
	}
}

func retryTierFor(attempt int32) retryTier {
	idx := max(int(attempt)-1, 0)
	idx = min(idx, len(retryTiers)-1)
	return retryTiers[idx]
}

func dlqQueueArgs() amqp091.Table {
	return amqp091.Table{
		"x-message-ttl": int64(DLQMessageTTL / time.Millisecond),
		"x-max-length":  int64(dlqMaxLength),
		"x-overflow":    "drop-head",
	}
}

func inboxQueueArgs() amqp091.Table {
	return amqp091.Table{
		"x-queue-type":           amqp091.QueueTypeQuorum,
		"x-dead-letter-exchange": exchangeDeadLetter,
		"x-delivery-limit":       int32(inboxDeliveryLimit),
	}
}

var inboxQueuesDef = []struct {
	name        string
	bindingKeys []string
}{
	{
		name:        QueueSubscriptionInbox,
		bindingKeys: []string{events.EventRepositoryRemoved},
	},
	{
		name:        QueueScannerInbox,
		bindingKeys: []string{events.EventRepositoryChanged},
	},
	{
		name:        QueueNotificationInbox,
		bindingKeys: []string{events.EventSubscriptionChanged, events.EventRepositoryRemoved, events.EventReleaseDetected},
	},
	{
		name:        QueueMailerInbox,
		bindingKeys: []string{events.EventEmailQueued},
	},
}

func EnsureTopology(conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	defer closeChannel(ch)

	// The dead-letter exchange doubles as the alternate exchange for the
	// topic exchange, so messages that cannot be routed to any inbox queue
	// are retained in the DLQ instead of being returned and dropped.
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
		dlqQueueArgs(),
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

	if err = ch.ExchangeDeclare(
		ExchangeTopic,
		amqp091.ExchangeTopic,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		amqp091.Table{
			alternateExchangeArg: exchangeDeadLetter,
		},
	); err != nil {
		return fmt.Errorf("declare topic exchange: %w", err)
	}

	for _, qDef := range inboxQueuesDef {
		if _, err = ch.QueueDeclare(
			qDef.name,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			inboxQueueArgs(),
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

		// Retry queues are not bound to the topic exchange. The consumer
		// publishes to them through the default exchange, and after the
		// queue-level TTL expires they dead-letter straight back into the
		// inbox queue.
		for _, tier := range retryTiers {
			retryName := retryQueueName(qDef.name, tier)
			if _, err = ch.QueueDeclare(
				retryName,
				true,  // durable
				false, // auto-delete
				false, // exclusive
				false, // no-wait
				retryQueueArgs(tier, qDef.name),
			); err != nil {
				return fmt.Errorf("declare retry queue %q: %w", retryName, err)
			}
		}
	}

	return nil
}
