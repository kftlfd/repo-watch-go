package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/utils"
)

const (
	defaultBatchSize            = 5
	defaultMaxAttempts          = 5
	defaultLeaseDuration        = 5 * time.Second
	defaultLoopInterval         = 5 * time.Second
	defaultLoopIterationTimeout = 3 * time.Second
	defaultRetryBaseBackoff     = 250 * time.Millisecond
	defaultRetryMaxBackoff      = 5 * time.Second

	storeTimeout = 5 * time.Second
)

type Row struct {
	ID        int32
	EventType string
	EventID   string
	Payload   json.RawMessage
	CreatedAt time.Time
	Attempts  int32

	// InvalidReason is set by a service-specific store when a row cannot be
	// mapped to an event. The dispatcher treats such rows as terminal failures
	// instead of failing the whole batch.
	InvalidReason string
}

type Failure struct {
	Attempts int32
	Terminal bool
}

// PendingStats describes the unpublished, non-failed rows still waiting for
// delivery. Oldest is zero when the outbox is empty.
type PendingStats struct {
	Count  int64
	Oldest time.Time
}

type Store interface {
	GetNextBatch(ctx context.Context, batchSize int32, leaseUntil time.Time) ([]Row, error)
	MarkPublished(ctx context.Context, ids []int32) error
	RecordFailure(
		ctx context.Context,
		id int32,
		lastError string,
		terminal bool,
		maxAttempts int32,
	) (Failure, error)
	PendingStats(ctx context.Context) (PendingStats, error)
}

type DispatcherConfig struct {
	BatchSize            *int32
	MaxAttempts          *int32
	LeaseDuration        *time.Duration
	LoopInterval         *time.Duration
	LoopIterationTimeout *time.Duration
	RetryBaseBackoff     *time.Duration
	RetryMaxBackoff      *time.Duration
}

type DispatcherDeps struct {
	Config    DispatcherConfig
	Logger    *slog.Logger
	Publisher rabbitmq.RabbitJSONPublisher
	Store     Store
	Metrics   MetricsRecorder
	Now       func() time.Time
	SleepCtx  func(context.Context, time.Duration) error
}

type config struct {
	batchSize            int32
	maxAttempts          int32
	leaseDuration        time.Duration
	loopInterval         time.Duration
	loopIterationTimeout time.Duration
	retryBaseBackoff     time.Duration
	retryMaxBackoff      time.Duration
}

type Dispatcher struct {
	cfg     config
	logger  *slog.Logger
	pub     rabbitmq.RabbitJSONPublisher
	store   Store
	metrics MetricsRecorder
	now     func() time.Time
	sleep   func(context.Context, time.Duration) error
}

func getConfig(conf DispatcherConfig) (config, error) {
	cfg := config{
		batchSize:            defaultBatchSize,
		maxAttempts:          defaultMaxAttempts,
		leaseDuration:        defaultLeaseDuration,
		loopInterval:         defaultLoopInterval,
		loopIterationTimeout: defaultLoopIterationTimeout,
		retryBaseBackoff:     defaultRetryBaseBackoff,
		retryMaxBackoff:      defaultRetryMaxBackoff,
	}

	if conf.BatchSize != nil {
		cfg.batchSize = *conf.BatchSize
	}
	if conf.MaxAttempts != nil {
		cfg.maxAttempts = *conf.MaxAttempts
	}
	if conf.LeaseDuration != nil {
		cfg.leaseDuration = *conf.LeaseDuration
	}
	if conf.LoopInterval != nil {
		cfg.loopInterval = *conf.LoopInterval
	}
	if conf.LoopIterationTimeout != nil {
		cfg.loopIterationTimeout = *conf.LoopIterationTimeout
	}
	if conf.RetryBaseBackoff != nil {
		cfg.retryBaseBackoff = *conf.RetryBaseBackoff
	}
	if conf.RetryMaxBackoff != nil {
		cfg.retryMaxBackoff = *conf.RetryMaxBackoff
	}

	if cfg.batchSize < 1 {
		return cfg, fmt.Errorf("invalid batch size: %v", cfg.batchSize)
	}
	if cfg.leaseDuration <= 0 {
		return cfg, fmt.Errorf("invalid lease duration: %v", cfg.leaseDuration)
	}
	if cfg.loopIterationTimeout <= 0 {
		return cfg, fmt.Errorf("invalid loop iteration timeout: %v", cfg.loopIterationTimeout)
	}
	if cfg.loopIterationTimeout >= cfg.leaseDuration {
		return cfg, fmt.Errorf(
			"loop iteration must be less than lease duration, got %v >= %v",
			cfg.loopIterationTimeout, cfg.leaseDuration)
	}
	if cfg.loopInterval <= 0 {
		return cfg, fmt.Errorf("invalid loop interval: %v", cfg.loopInterval)
	}
	if cfg.retryBaseBackoff <= 0 {
		return cfg, fmt.Errorf("invalid retry backoff: %v", cfg.retryBaseBackoff)
	}
	if cfg.retryMaxBackoff < cfg.retryBaseBackoff {
		return cfg, fmt.Errorf(
			"retry max backoff must be at least retry backoff, got %v < %v",
			cfg.retryMaxBackoff, cfg.retryBaseBackoff)
	}
	if cfg.maxAttempts < 1 {
		return cfg, fmt.Errorf("invalid max attempts: %v", cfg.maxAttempts)
	}

	return cfg, nil
}

func NewDispatcher(d DispatcherDeps) (*Dispatcher, error) {
	cfg, err := getConfig(d.Config)
	if err != nil {
		return nil, err
	}

	if d.Now == nil {
		d.Now = time.Now
	}
	if d.SleepCtx == nil {
		d.SleepCtx = utils.SleepCtx
	}
	if d.Metrics == nil {
		d.Metrics = noopMetrics{}
	}

	return &Dispatcher{
		cfg:     cfg,
		logger:  d.Logger,
		pub:     d.Publisher,
		store:   d.Store,
		metrics: d.Metrics,
		now:     d.Now,
		sleep:   d.SleepCtx,
	}, nil
}

func (d *Dispatcher) Start(ctx context.Context) error {
	d.logger.Info("starting dispatcher")

	backoff := d.cfg.retryBaseBackoff

	for {
		n, err := d.runIteration(ctx)

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			switch {
			case errors.Is(err, rabbitmq.ErrClosed):
				return err
			case errors.Is(err, rabbitmq.ErrUnavailable):
				d.metrics.Unavailable()
				d.logger.Warn("outbox paused while rabbitmq is unavailable", "error", err)
			case errors.Is(err, context.DeadlineExceeded):
				d.logger.Warn("outbox iteration timed out", "error", err)
			default:
				d.logger.Error("outbox iteration failed; retrying", "error", err)
			}

			if err := d.sleep(ctx, backoff); err != nil {
				return err
			}
			backoff = d.nextBackoff(backoff)
			continue
		}

		backoff = d.cfg.retryBaseBackoff
		if n > 0 {
			continue
		}
		if err := d.sleep(ctx, d.cfg.loopInterval); err != nil {
			return err
		}
	}
}

func (d *Dispatcher) runIteration(ctx context.Context) (processed int, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.loopIterationTimeout)
	defer cancel()

	d.recordPendingStats(ctx)

	leaseDeadline := d.now().Add(d.cfg.leaseDuration)
	batch, err := d.store.GetNextBatch(ctx, d.cfg.batchSize, leaseDeadline)
	if err != nil {
		return 0, fmt.Errorf("get next batch: %w", err)
	}

	if len(batch) < 1 {
		return 0, nil
	}

	publishedIDs := []int32{}
	errs := []error{}

	for _, row := range batch {
		if row.InvalidReason != "" {
			failure, failureErr := d.recordFailure(ctx, row, row.InvalidReason, true)
			if failureErr != nil {
				errs = append(errs, failureErr)
				break
			}
			if failure.Terminal {
				processed++
			}
			continue
		}

		published, terminal, publishErr := d.publishWithRetry(ctx, leaseDeadline, row)
		if published {
			publishedIDs = append(publishedIDs, row.ID)
			processed++
			continue
		}
		if terminal {
			processed++
		}
		if publishErr != nil {
			errs = append(errs, publishErr)
		}
		// A publish failure is likely to affect the rest of the batch. Leave
		// the remaining rows leased and try again on a later iteration.
		break
	}

	if len(publishedIDs) < 1 {
		return processed, errors.Join(errs...)
	}

	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()

	if err := d.store.MarkPublished(markCtx, publishedIDs); err != nil {
		errs = append(errs, fmt.Errorf("mark processed: %w", err))
	} else {
		d.metrics.Published(len(publishedIDs))
	}

	return processed, errors.Join(errs...)
}

func (d *Dispatcher) recordPendingStats(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	stats, err := d.store.PendingStats(ctx)
	if err != nil {
		d.logger.Warn("outbox stats query failed", "error", err)
		return
	}

	d.metrics.SetPending(stats.Count)

	var age time.Duration
	if stats.Count > 0 && !stats.Oldest.IsZero() {
		age = max(d.now().Sub(stats.Oldest), 0)
	}
	d.metrics.SetOldestAge(age)
}

func (d *Dispatcher) publishWithRetry(
	ctx context.Context,
	leaseDeadline time.Time,
	row Row,
) (published bool, terminal bool, err error) {
	var lastErr error

	for {
		if ctx.Err() != nil {
			return false, false, ctx.Err()
		}
		if !d.now().Before(leaseDeadline) {
			return false, false, fmt.Errorf(
				"outbox row %d: lease expired before publish: %w", row.ID, lastErr)
		}

		// Detach from shutdown cancellation so an in-flight publisher confirm
		// can finish; the lease deadline keeps the wait bounded.
		publishCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), leaseDeadline)
		err = d.pub.PublishJSON(publishCtx, rabbitmq.EnvelopeJSON{
			EventID:   row.EventID,
			EventType: row.EventType,
			CreatedAt: row.CreatedAt,
			Payload:   row.Payload,
		})
		cancel()

		if err == nil {
			return true, false, nil
		}

		lastErr = err

		if ctx.Err() != nil {
			return false, false, ctx.Err()
		}

		if errors.Is(err, rabbitmq.ErrClosed) || errors.Is(err, rabbitmq.ErrUnavailable) {
			return false, false, err
		}

		isTerminal := false
		if errors.Is(err, rabbitmq.ErrInvalid) {
			isTerminal = true
		}

		failure, recordErr := d.recordFailure(ctx, row, err.Error(), isTerminal)
		if recordErr != nil {
			return false, false, fmt.Errorf("record event failure %q: %w", row.EventID, recordErr)
		}
		if failure.Terminal {
			return false, true, fmt.Errorf("event %q exceeded %d attempts: %w", row.EventID, d.cfg.maxAttempts, err)
		}

		d.logger.Error(
			"outbox publish failed; retrying",
			"outbox_id", row.ID,
			"event_type", row.EventType,
			"attempt", failure.Attempts,
			"error", err,
		)
		d.metrics.Retry()

		delay := d.retryDelay(failure.Attempts)
		if d.now().Add(delay).After(leaseDeadline) {
			return false, false, fmt.Errorf(
				"outbox row %d: lease expires before retry: %w", row.ID, err)
		}
		if err := d.sleep(ctx, delay); err != nil {
			return false, false, err
		}
	}
}

func (d *Dispatcher) recordFailure(
	ctx context.Context,
	row Row,
	reason string,
	terminal bool,
) (Failure, error) {
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()

	failure, err := d.store.RecordFailure(
		storeCtx,
		row.ID,
		reason,
		terminal,
		d.cfg.maxAttempts,
	)
	if err != nil {
		return Failure{}, fmt.Errorf("record failure for outbox row %d: %w", row.ID, err)
	}

	if failure.Terminal {
		d.logger.Error(
			"outbox event dead-lettered",
			"outbox_id", row.ID,
			"event_type", row.EventType,
			"attempts", failure.Attempts,
			"error", reason,
		)
		d.metrics.DeadLettered()
	}

	return failure, nil
}

func (d *Dispatcher) retryDelay(attempts int32) time.Duration {
	delay := d.cfg.retryBaseBackoff
	for i := int32(1); i < attempts; i++ {
		if delay >= d.cfg.retryMaxBackoff/2 {
			return d.cfg.retryMaxBackoff
		}
		delay *= 2
	}
	if delay > d.cfg.retryMaxBackoff {
		return d.cfg.retryMaxBackoff
	}
	return delay
}

func (d *Dispatcher) nextBackoff(current time.Duration) time.Duration {
	if current >= d.cfg.retryMaxBackoff/2 {
		return d.cfg.retryMaxBackoff
	}
	next := current * 2
	if next > d.cfg.retryMaxBackoff {
		return d.cfg.retryMaxBackoff
	}
	return next
}
