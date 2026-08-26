package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nack-requeue"
	NackDiscard AckType = "nack-discard"
)

type consumer struct {
	logger *slog.Logger
	ch     *amqp091.Channel
}

func NewConsumer(conn *amqp091.Connection, logger *slog.Logger) (*consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create consumer channel: %w", err)
	}

	if err := ch.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("set consumer QoS: %w", err)
	}

	return &consumer{
		logger: logger.WithGroup("event-consumer"),
		ch:     ch,
	}, nil
}

func (c *consumer) Close() error {
	return c.ch.Close()
}

type jsonEvent struct {
	MessageID  string
	RoutingKey string
	Body       json.RawMessage
}

type jsonEventHandler func(ctx context.Context, event jsonEvent) (AckType, error)

func (c *consumer) consumeJSON(
	ctx context.Context,
	queueName string,
	handler jsonEventHandler,
) error {
	deliveries, err := c.ch.ConsumeWithContext(
		ctx,
		queueName,
		"",
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume queue %q: %w", queueName, err)
	}

	for delivery := range deliveries {
		ack, err := handler(ctx, jsonEvent{
			MessageID:  delivery.MessageId,
			RoutingKey: delivery.RoutingKey,
			Body:       delivery.Body,
		})

		if ack != Ack {
			c.logger.Error("handler nack",
				"error", err,
				"queue", queueName,
				"routing_key", delivery.RoutingKey,
				"message_id", delivery.MessageId,
			)
		}

		switch ack {
		case Ack:
			err = delivery.Ack(false)

		case NackDiscard:
			err = delivery.Reject(false)

		case NackRequeue:
			err = delivery.Nack(false, true)

		default:
			if err := delivery.Nack(false, true); err != nil {
				return fmt.Errorf("invalid ack type %q and failed to requeue: %w", ack, err)
			}
			return fmt.Errorf("invalid ack type: %q", ack)
		}

		if err != nil {
			return fmt.Errorf("consumer ack error: %w", err)
		}
	}

	return nil
}

type ConsumeEventMeta struct {
	EventID     string
	MessageType string
	CreatedAt   time.Time
}

// if returning Ack: err must be nil
// for Nack types: err must be non-nil
type EventHandler[T event] func(ConsumeEventMeta, T) (AckType, error)

func Consume[T event](c *consumer, ctx context.Context, ed eventDescriptor[T], handler EventHandler[T]) error {
	var h jsonEventHandler = func(ctx context.Context, event jsonEvent) (AckType, error) {
		var envelope envelopeJSON
		if err := json.Unmarshal(event.Body, &envelope); err != nil {
			return NackDiscard, fmt.Errorf("invalid message envelope")
		}

		if envelope.MessageType != ed.messageType {
			return NackDiscard, fmt.Errorf("unexpected message type %q, expected %q", envelope.MessageType, ed.messageType)
		}

		var payload T
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return NackDiscard, fmt.Errorf("invalid event payload")
		}

		meta := ConsumeEventMeta{
			EventID:     envelope.EventID,
			MessageType: envelope.MessageType,
			CreatedAt:   envelope.CreatedAt,
		}

		return handler(meta, payload)
	}

	return c.consumeJSON(ctx, ed.routingKey, h)
}
