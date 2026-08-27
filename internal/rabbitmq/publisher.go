package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitJSONPublisher interface {
	PublishJSON(context.Context, EnvelopeJSON) error
}

type Publisher struct {
	ch       *amqp091.Channel
	returnCh chan amqp091.Return
}

func NewPublisher(conn *amqp091.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create publisher channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	returnCh := ch.NotifyReturn(make(chan amqp091.Return, 1))

	return &Publisher{
		ch:       ch,
		returnCh: returnCh,
	}, nil
}

func (p *Publisher) Close() error {
	return p.ch.Close()
}

func (p *Publisher) Returns() <-chan amqp091.Return {
	return p.returnCh
}

func (p *Publisher) PublishJSON(
	ctx context.Context,
	msg EnvelopeJSON,
) error {
	if msg.EventType == "" {
		return fmt.Errorf("missing message type")
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	routingKey := GetQueueName(msg.EventType)

	publishing := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    msg.EventID,
		Body:         body,
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchangeDefault,
		routingKey,
		true,  // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("publish event %q %q: %w", msg.EventType, msg.EventID, err)
	}
	if confirmation == nil {
		return errors.New("publisher confirmation was not created")
	}

	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirmation error: %w", err)
	}
	if !ack {
		return fmt.Errorf("nack")
	}

	return nil
}
