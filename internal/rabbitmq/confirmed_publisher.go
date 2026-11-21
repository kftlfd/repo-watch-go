package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ConfirmedPublisher publishes messages with mandatory routing and publisher
// confirms on a dedicated channel. Each instance owns one channel; callers
// must serialize publishes on it.
type ConfirmedPublisher struct {
	ch      *amqp091.Channel
	returns <-chan amqp091.Return

	// Timeout, when non-zero, bounds each Publish call independently of the
	// caller's context.
	Timeout time.Duration
}

func NewConfirmedPublisher(ch *amqp091.Channel) (*ConfirmedPublisher, error) {
	if ch == nil {
		return nil, fmt.Errorf("missing publisher channel")
	}
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	return &ConfirmedPublisher{
		ch:      ch,
		returns: ch.NotifyReturn(make(chan amqp091.Return, 1)),
	}, nil
}

// ReturnedError reports a mandatory message that the broker could not route.
type ReturnedError struct {
	Return amqp091.Return
}

func (e *ReturnedError) Error() string {
	return fmt.Sprintf(
		"publish to exchange %q with key %q was returned: %s",
		e.Return.Exchange, e.Return.RoutingKey, e.Return.ReplyText)
}

// Publish publishes msg, waits for a broker confirm, and then checks whether
// the broker returned the message as unroutable. The broker sends basic.return
// before the corresponding publisher confirm, so a return is already queued
// when the confirm is observed.
//
// Callers must serialize calls on one publisher. Publish consumes the return
// channel itself; no other goroutine may read it.
func (p *ConfirmedPublisher) Publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	msg amqp091.Publishing,
) error {
	if !p.Active() {
		return errors.New("channel closed")
	}

	if p.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.Timeout)
		defer cancel()
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,  // mandatory
		false, // immediate
		msg,
	)
	if err != nil {
		return fmt.Errorf("publish to exchange %q with key %q: %w", exchange, routingKey, err)
	}
	if confirmation == nil {
		return errors.New("publisher confirmation was not created")
	}

	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		return errors.New("publisher nack")
	}

	select {
	case ret := <-p.returns:
		return &ReturnedError{Return: ret}
	default:
		return nil
	}
}

func (p *ConfirmedPublisher) Active() bool {
	return p.ch != nil && !p.ch.IsClosed()
}

func (p *ConfirmedPublisher) Close() {
	if p.ch != nil {
		closeChannel(p.ch)
		p.ch = nil
	}
}
