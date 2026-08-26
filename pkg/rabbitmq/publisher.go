package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type publisher struct {
	ch       *amqp091.Channel
	returnCh chan amqp091.Return
}

func NewPublisher(conn *amqp091.Connection) (*publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create publisher channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	returnCh := ch.NotifyReturn(make(chan amqp091.Return, 1))

	return &publisher{
		ch:       ch,
		returnCh: returnCh,
	}, nil
}

func (p *publisher) Close() error {
	return p.ch.Close()
}

func (p *publisher) Returns() <-chan amqp091.Return {
	return p.returnCh
}

func (p *publisher) publishJSON(
	ctx context.Context,
	exchange, routingKey string,
	msgId string,
	msgBody json.RawMessage,
) error {
	publishing := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    msgId,
		Body:         msgBody,
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,  // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("publish event %q %q: %w", routingKey, msgId, err)
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

type PublishEventMeta struct {
	ID        string
	CreatedAt time.Time
}

func Publish[T event](p *publisher, ctx context.Context, meta PublishEventMeta, ed eventDescriptor[T], ev T) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("json marshal payload: %w", err)
	}

	evEnvelope := envelopeJSON{
		EventID:     meta.ID,
		MessageType: ed.routingKey,
		CreatedAt:   meta.CreatedAt,
		Payload:     payload,
	}

	msgBody, err := json.Marshal(evEnvelope)
	if err != nil {
		return fmt.Errorf("json marshal msgBody: %w", err)
	}

	return p.publishJSON(ctx, exchangeDefault, ed.routingKey, meta.ID, msgBody)
}
