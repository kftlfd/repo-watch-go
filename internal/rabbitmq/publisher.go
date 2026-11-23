package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"github.com/rabbitmq/amqp091-go"

	"repo-watch/internal/events"
)

type RabbitJSONPublisher interface {
	PublishJSON(context.Context, events.Envelope) error
}

var _ RabbitJSONPublisher = (*publisher)(nil)

type publisher struct {
	client  Client
	logger  *slog.Logger
	metrics MetricsRecorder

	mu   sync.Mutex
	publ *ConfirmedPublisher

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
	msg events.Envelope,
) error {
	if msg.EventType == "" {
		return fmt.Errorf("%w: missing message type", ErrInvalid)
	}

	msg.Version = events.NormalizeVersion(msg.Version)
	if err := events.Validate(msg.EventType, msg.Version); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w: json marshal: %w", ErrInvalid, err)
	}

	routingKey := msg.EventType

	correlationID := msg.CorrelationID
	if correlationID == "" {
		correlationID = msg.EventID
	}

	publishing := amqp091.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp091.Persistent,
		MessageId:     msg.EventID,
		CorrelationId: correlationID,
		Body:          body,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	publ, err := p.getPubLocked()
	if err != nil {
		return err
	}

	err = publ.Publish(ctx, ExchangeTopic, routingKey, publishing)
	if err == nil {
		return nil
	}

	p.recordPublishError(err)

	// A failed publish may leave a return queued or the channel in an unknown
	// state. Recreate the shared channel on the next attempt instead of
	// risking a stale return being attributed to a later message.
	if _, ok := errors.AsType[*ReturnedError](err); !ok {
		publ.Close()
		p.publ = nil
	}

	return err
}

// PublishRetry copies a failed delivery into a delayed retry queue. It uses
// a dedicated channel so the return check cannot race with unrelated
// publishes on the shared channel. The caller must not ack the original
// delivery unless this returns nil.
func (p *publisher) PublishRetry(
	ctx context.Context,
	queue string,
	d amqp091.Delivery,
	attempt int32,
) error {
	if queue == "" {
		return fmt.Errorf("%w: missing retry queue", ErrInvalid)
	}

	conn, err := p.client.acquire()
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create retry publisher channel: %w", err)
	}

	publ, err := NewConfirmedPublisher(ch)
	if err != nil {
		closeChannel(ch)
		return err
	}
	defer publ.Close()

	headers := amqp091.Table{}
	maps.Copy(headers, d.Headers)
	headers[RetryCountHeader] = attempt

	msg := amqp091.Publishing{
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Headers:         headers,
		Body:            d.Body,
	}

	if err := publ.Publish(ctx, "", queue, msg); err != nil {
		p.recordPublishError(err)
		return fmt.Errorf("publish retry to %q: %w", queue, err)
	}
	return nil
}

func (p *publisher) getPubLocked() (*ConfirmedPublisher, error) {
	select {
	case <-p.done:
		return nil, ErrClosed
	default:
	}

	if p.publ != nil && p.publ.Active() {
		return p.publ, nil
	}

	if p.publ != nil {
		p.publ.Close()
		p.publ = nil
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

	publ, err := NewConfirmedPublisher(ch)
	if err != nil {
		closeChannel(ch)
		return nil, err
	}

	p.publ = publ
	return publ, nil
}

func (p *publisher) recordPublishError(err error) {
	if returned, ok := errors.AsType[*ReturnedError](err); ok {
		p.metrics.MessageReturned(returned.Return.RoutingKey)
		p.logger.Error("rabbitmq message returned", "msg", returned.Return)
	}
}
