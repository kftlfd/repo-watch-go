package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	defaultDLQMonitorInterval = 15 * time.Second
	defaultDLQCheckTimeout    = 10 * time.Second
)

type DLQMonitorDeps struct {
	Client       Client
	Logger       *slog.Logger
	Metrics      MetricsRecorder
	Interval     time.Duration
	CheckTimeout time.Duration
}

type dlqMonitor struct {
	client       Client
	logger       *slog.Logger
	metrics      MetricsRecorder
	interval     time.Duration
	checkTimeout time.Duration
	lastDepth    int
}

func NewDLQMonitor(d DLQMonitorDeps) (*dlqMonitor, error) {
	if d.Client == nil {
		return nil, fmt.Errorf("missing DLQ monitor client")
	}
	if d.Logger == nil {
		return nil, fmt.Errorf("missing DLQ monitor logger")
	}
	if d.Interval <= 0 {
		d.Interval = defaultDLQMonitorInterval
	}
	if d.CheckTimeout <= 0 {
		d.CheckTimeout = defaultDLQCheckTimeout
	}
	if d.Metrics == nil {
		d.Metrics = noopMetrics{}
	}

	return &dlqMonitor{
		client:       d.Client,
		logger:       d.Logger.With("module", "dlq_monitor"),
		metrics:      d.Metrics,
		interval:     d.Interval,
		checkTimeout: d.CheckTimeout,
		lastDepth:    -1,
	}, nil
}

func (m *dlqMonitor) Start(ctx context.Context) error {
	m.logger.Info("starting", "interval", m.interval)

	for {
		if ctx.Err() != nil {
			return nil
		}

		if fatal, err := m.check(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			m.logger.Error("check error", "error", err)
			if fatal {
				return err
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(m.interval):
		}
	}
}

type dlqCheckResult struct {
	fatal bool
	depth int
	err   error
}

func (m *dlqMonitor) check(ctx context.Context) (fatal bool, err error) {
	conn, err := m.client.acquire()
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return true, err
		}
		if errors.Is(err, ErrUnavailable) {
			m.logger.Debug("rabbitmq is not connected; skipping dead-letter queue inspection")
			return false, nil
		}
		return false, fmt.Errorf("acquire rabbitmq connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("open channel: %w", err)
	}

	resultCh := make(chan dlqCheckResult, 1)
	go func() {
		defer closeChannel(ch)
		depth, fatal, err := declareDLQ(ch)
		resultCh <- dlqCheckResult{fatal: fatal, depth: depth, err: err}
	}()

	checkCtx, cancel := context.WithTimeout(ctx, m.checkTimeout)
	defer cancel()

	select {
	case <-checkCtx.Done():
		return false, checkCtx.Err()

	case result := <-resultCh:
		if result.err != nil {
			return result.fatal, result.err
		}
		m.observeDepth(result.depth)
		return false, nil
	}
}

func declareDLQ(ch *amqp091.Channel) (depth int, fatal bool, err error) {
	queue, err := ch.QueueDeclarePassive(
		queueDeadLetter,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		var amqpErr *amqp091.Error
		incompatibleDeclaration := errors.As(err, &amqpErr) && amqpErr.Code == amqp091.PreconditionFailed
		return 0, incompatibleDeclaration, fmt.Errorf("declare dead-letter queue: %w", err)
	}

	return queue.Messages, false, nil
}

func (m *dlqMonitor) observeDepth(depth int) {
	m.metrics.SetDLQDepth(depth)

	switch {
	case depth > 0 && depth > m.lastDepth:
		m.logger.Error(
			"dead-letter queue has messages",
			"queue", queueDeadLetter,
			"depth", depth,
			"previous_depth", m.lastDepth,
		)
	case depth == 0 && m.lastDepth > 0:
		m.logger.Info("dead-letter queue drained", "queue", queueDeadLetter)
	}

	m.lastDepth = depth
}
