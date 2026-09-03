package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nack-requeue"
	NackDiscard AckType = "nack-discard"
)

type Consumer struct {
	logger *slog.Logger
	ch     *amqp091.Channel
}

func NewConsumer(conn *amqp091.Connection, logger *slog.Logger) (*Consumer, error) {
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

	return &Consumer{
		logger: logger,
		ch:     ch,
	}, nil
}

func (c *Consumer) Close() error {
	return c.ch.Close()
}

type EventMeta struct {
	RoutingKey string
}

// if returning Ack: err must be nil
// for Nack types: err must be non-nil
type JSONEventHandler func(ctx context.Context, msg EnvelopeJSON, meta EventMeta) (AckType, error)

func (c *Consumer) ConsumeJSON(
	ctx context.Context,
	queueName string,
	handler JSONEventHandler,
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
		logArgs := []any{
			slog.String("queue", queueName),
			slog.String("routing_key", delivery.RoutingKey),
			slog.String("message_id", delivery.MessageId),
		}

		meta := EventMeta{RoutingKey: delivery.RoutingKey}
		var msg EnvelopeJSON

		ack, ok, err := handleJsonMsg(ctx, delivery.Body, &msg, meta, handler)
		if ok {
			logArgs = append(logArgs,
				slog.String("event_id", msg.EventID),
				slog.String("event_type", msg.EventType),
			)
		}
		logArgs = append(logArgs, slog.String("status", string(ack)))
		if err != nil {
			logArgs = append(logArgs, slog.Any("error", err))
		}

		c.logger.Debug("new event", logArgs...)

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

func handleJsonMsg(
	ctx context.Context,
	deliveryBody []byte,
	msg *EnvelopeJSON,
	meta EventMeta,
	handler JSONEventHandler,
) (ack AckType, ok bool, err error) {
	if err := json.Unmarshal(deliveryBody, msg); err != nil {
		return NackDiscard, false, fmt.Errorf("invalid json envelope: %w", err)
	}

	ack, err = handler(ctx, *msg, meta)
	return ack, true, err
}
