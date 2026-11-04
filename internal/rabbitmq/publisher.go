package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitJSONPublisher interface {
	PublishJSON(context.Context, EnvelopeJSON) error
}

var _ RabbitJSONPublisher = (*publisher)(nil)

type publisher struct {
	client  Client
	logger  *slog.Logger
	metrics MetricsRecorder

	mu sync.Mutex
	ch *amqp091.Channel

	done chan struct{}
}

func NewPublisher(client Client, logger *slog.Logger, metrics MetricsRecorder) (*publisher, error) {
	if client == nil {
		return nil, fmt.Errorf("missing client")
	}
	if logger == nil {
		return nil, fmt.Errorf("missing logger")
	}
	if metrics == nil {
		metrics = noopMetrics{}
	}

	p := &publisher{
		client:  client,
		logger:  logger,
		metrics: metrics,
		done:    make(chan struct{}),
	}

	return p, nil
}

func (p *publisher) PublishJSON(
	ctx context.Context,
	msg EnvelopeJSON,
) error {
	if msg.EventType == "" {
		return fmt.Errorf("%w: missing message type", ErrInvalid)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w: json marshal: %w", ErrInvalid, err)
	}

	routingKey := msg.EventType

	publishing := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    msg.EventID,
		Body:         body,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.getChannelLocked()
	if err != nil {
		return err
	}

	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		ExchangeTopic,
		routingKey,
		true,  // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return err
	}
	if confirmation == nil {
		return errors.New("publish confirmation was not created")
	}

	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		return errors.New("publisher nack")
	}

	return nil
}

// getChannelLocked returns the current open channel, or tries to create
// a new one
func (p *publisher) getChannelLocked() (*amqp091.Channel, error) {
	select {
	case <-p.done:
		return nil, ErrClosed
	default:
	}

	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}

	if p.ch != nil {
		closeChannel(p.ch)
		p.ch = nil
	}

	conn, err := p.client.acquire()
	if err != nil {
		if errors.Is(err, ErrClosed) {
			close(p.done)
		}
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create publisher channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		closeChannel(ch)
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	returns := make(chan amqp091.Return, 1)
	ch.NotifyReturn(returns)
	go p.observeReturns(returns)

	p.ch = ch
	return ch, nil
}

func (p *publisher) observeReturns(returns <-chan amqp091.Return) {
	for ret := range returns {
		p.metrics.MessageReturned(ret.RoutingKey)
		p.logger.Error("rabbitmq message returned", "msg", ret)
	}
}
