package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"repo-watch/internal/events"
	"repo-watch/internal/observability"
	"repo-watch/internal/utils"
)

type RabbitJSONConsumer interface {
	ConsumeJSON(
		ctx context.Context,
		queueName string,
		handler JSONEventHandler,
		cfg *ConsumerSessionConfig,
	) error
}

var _ RabbitJSONConsumer = (*consumer)(nil)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nack-requeue"
	NackDiscard AckType = "nack-discard"

	consumerRetryBase       = 5 * time.Second
	consumerRetryMax        = 120 * time.Second
	consumerStableThreshold = 10 * time.Second
	defaultHandlerTimeout   = time.Minute
	defaultMaxRetryAttempts = 4
	retryPublishTimeout     = 10 * time.Second
)

// RetryPublisher republishes a failed delivery to a delayed retry queue. The
// consumer acks the original delivery only after the retry publish has been
// confirmed, so a publish failure leaves the original message unacked.
type RetryPublisher interface {
	PublishRetry(ctx context.Context, queue string, delivery amqp091.Delivery, attempt int32) error
}

// ConsumerOption customizes a consumer. Options must be applied before
// ConsumeJSON is called.
type ConsumerOption func(*consumer)

// WithRetryPublisher enables delayed retries. A consumer without a retry
// publisher falls back to immediate nack-requeue.
func WithRetryPublisher(p RetryPublisher) ConsumerOption {
	return func(c *consumer) {
		c.retryPub = p
	}
}

type consumer struct {
	client   Client
	logger   *slog.Logger
	metrics  MetricsRecorder
	retryPub RetryPublisher
}

func NewConsumer(
	client Client,
	logger *slog.Logger,
	metrics MetricsRecorder,
	opts ...ConsumerOption,
) (*consumer, error) {
	if client == nil {
		return nil, fmt.Errorf("missing client")
	}
	if logger == nil {
		return nil, fmt.Errorf("missing logger")
	}
	if metrics == nil {
		metrics = noopMetrics{}
	}

	c := &consumer{
		client:  client,
		logger:  logger,
		metrics: metrics,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	return c, nil
}

// if returning Ack: err must be nil
// for Nack types: err must be non-nil
type JSONEventHandler func(ctx context.Context, msg events.Envelope) (AckType, error)

// ConsumeJSON consumes queueName and keeps consuming until the caller's
// context is cancelled or the client stops.
func (c *consumer) ConsumeJSON(
	ctx context.Context,
	queueName string,
	handler JSONEventHandler,
	cfg *ConsumerSessionConfig,
) error {
	session := &consumerSession{
		client:    c.client,
		logger:    c.logger,
		metrics:   c.metrics,
		retryPub:  c.retryPub,
		ctx:       ctx,
		queueName: queueName,
		handler:   handler,
	}
	if cfg != nil {
		session.cfg = *cfg
	}
	if session.cfg.HandlerTimeout <= 0 {
		session.cfg.HandlerTimeout = defaultHandlerTimeout
	}

	return session.run()
}

// ConsumerSessionConfig configures a single ConsumeJSON run.
type ConsumerSessionConfig struct {
	HandlerTimeout time.Duration

	// MaxRetryAttempts is the number of delayed retries after the first
	// handler failure. Zero uses defaultMaxRetryAttempts.
	MaxRetryAttempts int32
}

type consumerSession struct {
	cfg       ConsumerSessionConfig
	client    Client
	logger    *slog.Logger
	metrics   MetricsRecorder
	retryPub  RetryPublisher
	ctx       context.Context
	queueName string
	handler   JSONEventHandler
}

func (s *consumerSession) retryLimit() int32 {
	if s.cfg.MaxRetryAttempts > 0 {
		return s.cfg.MaxRetryAttempts
	}
	return defaultMaxRetryAttempts
}

func (s *consumerSession) run() error {
	backoff := consumerRetryBase

	for {
		conn, err := s.client.waitConn(s.ctx)
		if s.ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}

		startedAt := time.Now()
		err = s.consume(conn)
		if s.ctx.Err() != nil {
			return nil
		}

		if err != nil && isFatalConsumeError(err) {
			s.logger.Error("consumer stopped with fatal error",
				"queue", s.queueName,
				"error", err,
			)
			return err
		}

		// If consumer ran for a while, don't inherit a backoff accumulated during an earlier outage.
		if time.Since(startedAt) >= consumerStableThreshold {
			backoff = consumerRetryBase
		}

		if err == nil {
			s.logger.Warn("consumer delivery channel closed; restarting", "queue", s.queueName)
			backoff = consumerRetryBase
		} else {
			s.logger.Warn("consumer stopped; restarting",
				"queue", s.queueName,
				"error", err,
				"retry_in", backoff,
			)
		}

		if err := utils.SleepCtx(s.ctx, backoff); err != nil {
			return nil
		}
		backoff = min(backoff*2, consumerRetryMax)
	}
}

func (s *consumerSession) consume(conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create consumer channel: %w", err)
	}
	defer closeChannel(ch)

	if err := ch.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return fmt.Errorf("set consumer QoS: %w", err)
	}

	deliveries, err := ch.ConsumeWithContext(
		s.ctx,
		s.queueName,
		"",
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume queue %q: %w", s.queueName, err)
	}

	for delivery := range deliveries {
		err = s.handleDelivery(&delivery)
		if err != nil {
			return fmt.Errorf("handle delivery: %w", err)
		}
	}

	return nil
}

var errInvalidAck = errors.New("invalid ack")

// isFatalConsumeError reports whether err requires operator action. Server
// errors other than the temporary ones, for example a missing queue or a
// failed precondition, will not resolve by reconnecting, so the consumer
// should surface them instead of retrying forever.
func isFatalConsumeError(err error) bool {
	if errors.Is(err, errInvalidAck) {
		return true
	}

	if err, ok := errors.AsType[*amqp091.Error](err); ok {
		return err.Server && !err.Temporary()
	}

	return false
}

// invokeHandler isolates a panicking handler to the current delivery. A panic
// is a handler defect, not a transient failure, so the delivery is quarantined
// immediately instead of running through the retry tiers.
func (s *consumerSession) invokeHandler(
	ctx context.Context,
	msg events.Envelope,
) (ack AckType, err error) {
	defer func() {
		if r := recover(); r != nil {
			ack = NackDiscard
			err = fmt.Errorf("handler panic: %v", r)

			s.metrics.HandlerPanicked(s.queueName)
			s.logger.Error("event handler panicked",
				"queue", s.queueName,
				"event_id", msg.EventID,
				"event_type", msg.EventType,
				"correlation_id", observability.GetCorrelationID(ctx),
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	return s.handler(ctx, msg)
}

func (s *consumerSession) handleDelivery(delivery *amqp091.Delivery) error {
	logArgs := []any{
		slog.String("queue", s.queueName),
		slog.String("routing_key", delivery.RoutingKey),
		slog.String("message_id", delivery.MessageId),
		slog.Bool("redelivered", delivery.Redelivered),
	}

	var msg events.Envelope
	var ack AckType
	var err error

	var handlerStatus struct {
		failed     bool
		defects    bool
		invalidAck bool
	}

	correlationID := delivery.CorrelationId

	msg, err = events.DecodeEnvelope(delivery.Body)
	if err != nil {
		if correlationID == "" {
			correlationID = delivery.MessageId
		}
		ack = NackDiscard
	} else {
		if correlationID == "" {
			correlationID = msg.CorrelationID
		}
		if correlationID == "" {
			correlationID = msg.EventID
		}

		handlerCtx, cancel := context.WithTimeout(s.ctx, s.cfg.HandlerTimeout)
		handlerCtx = observability.WithCorrelationID(handlerCtx, correlationID)

		ack, err = s.invokeHandler(handlerCtx, msg)
		cancel()

		if ack == Ack && err != nil {
			ack = NackRequeue
			handlerStatus.defects = true
		}

		logArgs = append(logArgs,
			slog.String("event_id", msg.EventID),
			slog.String("event_type", msg.EventType),
		)
	}

	if correlationID != "" {
		logArgs = append(logArgs, slog.String("correlation_id", correlationID))
	}

	if err != nil {
		handlerStatus.failed = true
		logArgs = append(logArgs, slog.Any("error", err))
	}

	level := slog.LevelDebug

	switch ack {
	case Ack:
		err = delivery.Ack(false)

	case NackDiscard:
		err = delivery.Reject(false)
		level = slog.LevelError

	case NackRequeue:
		level = slog.LevelWarn
		if s.retryPub == nil {
			err = delivery.Nack(false, true)
			s.metrics.Requeued(s.queueName)
			break
		}

		attempt := Int32Header(delivery.Headers, RetryCountHeader) + 1
		if attempt > s.retryLimit() {
			level = slog.LevelError
			s.metrics.RetryExhausted(s.queueName)
			logArgs = append(logArgs,
				slog.Int("retry_attempt", int(attempt)),
				slog.String("retry_status", "exhausted"),
			)
			err = delivery.Reject(false)
			break
		}

		tier := retryTierFor(attempt)
		retryQueue := retryQueueName(s.queueName, tier)

		retryCtx, cancel := context.WithTimeout(s.ctx, retryPublishTimeout)
		publishErr := s.retryPub.PublishRetry(retryCtx, retryQueue, *delivery, attempt)
		cancel()

		logArgs = append(logArgs,
			slog.Int("retry_attempt", int(attempt)),
			slog.String("retry_queue", retryQueue),
			slog.Duration("retry_delay", tier.Delay),
		)
		if publishErr != nil {
			level = slog.LevelError
			s.metrics.RetryPublishFailed(s.queueName)
			logArgs = append(logArgs,
				slog.Any("retry_error", publishErr),
				slog.String("retry_status", "requeued"),
			)

			// Keep the event in the inbox. The quorum delivery limit is the
			// final backstop if retry publishing keeps failing.
			s.metrics.Requeued(s.queueName)
			err = delivery.Nack(false, true)
			break
		}

		s.metrics.RetryScheduled(s.queueName, tier.Name)
		logArgs = append(logArgs, slog.String("retry_status", "scheduled"))
		err = delivery.Ack(false)

	default:
		err = fmt.Errorf("%w: %q", errInvalidAck, ack)
		level = slog.LevelError
		handlerStatus.invalidAck = true
		handlerStatus.defects = true
	}

	if !handlerStatus.invalidAck {
		s.metrics.MessageHandled(s.queueName, ack)
	}
	if handlerStatus.failed || handlerStatus.invalidAck {
		s.metrics.HandlerFailed(s.queueName)
	}
	if handlerStatus.defects {
		s.metrics.HandlerDefectDetected(s.queueName)
	}

	logArgs = append(logArgs, slog.String("status", string(ack)))
	s.logger.Log(context.Background(), level, "new event", logArgs...)

	return err
}
