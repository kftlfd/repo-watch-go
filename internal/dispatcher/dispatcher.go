package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/utils"
)

const (
	defaultBatchSize         = 5
	defaultMaxAttempts       = 5
	defaultPollInterval      = 5 * time.Second
	defaultLeaseDuration     = 10 * time.Second
	defaultProcessingTimeout = 6 * time.Second
	defaultRetryBaseBackoff  = 1 * time.Second
	defaultRetryMaxBackoff   = 60 * time.Second
)

type Row struct {
	ID        int32
	EventType string
	EventID   string
	Version   int32
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

// FailedStats describes terminal outbox rows that were abandoned. Oldest is
// zero when there are no failed rows.
type FailedStats struct {
	Count  int64
	Oldest time.Time
}

type Store interface {
	GetNextBatch(ctx context.Context, batchSize int32, leaseUntil time.Time) ([]Row, error)
	MarkPublished(ctx context.Context, ids []int32) error
	// RecordFailure increments attempts and, for nonterminal failures,
	// stores nextAttemptAt as the earliest time the row may be retried.
	// nextAttemptAt is ignored for terminal failures.
	RecordFailure(
		ctx context.Context,
		id int32,
		lastError string,
		terminal bool,
		maxAttempts int32,
		nextAttemptAt time.Time,
	) (Failure, error)
	PendingStats(ctx context.Context) (PendingStats, error)
	FailedStats(ctx context.Context) (FailedStats, error)
}

type DispatcherConfig struct {
	BatchSize   *int32
	MaxAttempts *int32
	// If no pending rows in store, wait PollInterval before checking again
	PollInterval *time.Duration

	// Lease that is put on store rows batch for processing
	LeaseDuration *time.Duration
	// Timeout for batch processing (includes stats, query, publish/confirm,
	// mark processed), must be lower than LeaseDuration. Ensures that no writes
	// to store happen after lease expiration. Due to timeout some events can be
	// published but not marked as such in the store, causing duplicate event
	// publication (tune BatchSize and Lease/Timeout to mitigate)
	ProcessingTimeout *time.Duration

	RetryBaseBackoff *time.Duration
	RetryMaxBackoff  *time.Duration
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
	batchSize         int32
	maxAttempts       int32
	pollInterval      time.Duration
	leaseDuration     time.Duration
	processingTimeout time.Duration
	retryBaseBackoff  time.Duration
	retryMaxBackoff   time.Duration
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
		batchSize:         defaultBatchSize,
		maxAttempts:       defaultMaxAttempts,
		leaseDuration:     defaultLeaseDuration,
		pollInterval:      defaultPollInterval,
		processingTimeout: defaultProcessingTimeout,
		retryBaseBackoff:  defaultRetryBaseBackoff,
		retryMaxBackoff:   defaultRetryMaxBackoff,
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
	if conf.PollInterval != nil {
		cfg.pollInterval = *conf.PollInterval
	}
	if conf.ProcessingTimeout != nil {
		cfg.processingTimeout = *conf.ProcessingTimeout
	}
	if conf.RetryBaseBackoff != nil {
		cfg.retryBaseBackoff = *conf.RetryBaseBackoff
	}
	if conf.RetryMaxBackoff != nil {
		cfg.retryMaxBackoff = *conf.RetryMaxBackoff
	}

	if cfg.batchSize < 1 {
		return cfg, fmt.Errorf("invalid BatchSize: %v", cfg.batchSize)
	}
	if cfg.leaseDuration <= 0 {
		return cfg, fmt.Errorf("invalid LeaseDuration: %v", cfg.leaseDuration)
	}
	if cfg.processingTimeout <= 0 {
		return cfg, fmt.Errorf("invalid ProcessingTimeout: %v", cfg.processingTimeout)
	}
	if cfg.processingTimeout >= cfg.leaseDuration {
		return cfg, fmt.Errorf(
			"invalid ProcessingTimeout, must be less than LeaseDuration, got %v >= %v",
			cfg.processingTimeout, cfg.leaseDuration)
	}
	if cfg.pollInterval <= 0 {
		return cfg, fmt.Errorf("invalid PollInterval: %v", cfg.pollInterval)
	}
	if cfg.retryBaseBackoff <= 0 {
		return cfg, fmt.Errorf("invalid RetryBaseBackoff: %v", cfg.retryBaseBackoff)
	}
	if cfg.retryMaxBackoff < cfg.retryBaseBackoff {
		return cfg, fmt.Errorf(
			"invalid RetryMaxBackoff, must be at least RetryBaseBackoff, got %v < %v",
			cfg.retryMaxBackoff, cfg.retryBaseBackoff)
	}
	if cfg.maxAttempts < 1 {
		return cfg, fmt.Errorf("invalid MaxAttempts: %v", cfg.maxAttempts)
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
		if err := d.sleep(ctx, d.cfg.pollInterval); err != nil {
			return err
		}
	}
}

func (d *Dispatcher) runIteration(ctx context.Context) (processed int, err error) {
	now := d.now()
	leaseDeadline := now.Add(d.cfg.leaseDuration)
	processDeadline := now.Add(d.cfg.processingTimeout)

	// Bounded context for stats and next-batch query
	queryCtx, cancelQueryCtx := context.WithTimeout(ctx, d.cfg.processingTimeout)
	defer cancelQueryCtx()

	d.recordStats(queryCtx)
	batch, err := d.store.GetNextBatch(queryCtx, d.cfg.batchSize, leaseDeadline)
	if err != nil {
		return 0, fmt.Errorf("get next batch: %w", err)
	}
	if len(batch) < 1 {
		return 0, nil
	}

	// Detach from parent context cancellation, bound by processingTimeout < leaseDuration
	// to ensure that store operations are not interrupted by shutdown, but no store writes
	// happen after lease expiration
	iterCtx, cancelIterCtx := context.WithDeadline(context.WithoutCancel(ctx), processDeadline)
	defer cancelIterCtx()

	processed = 0
	publishedIDs := []int32{}
	errs := []error{}

	for _, row := range batch {
		if ctx.Err() != nil || iterCtx.Err() != nil {
			break
		}

		if row.InvalidReason != "" {
			failure, failureErr := d.recordFailure(iterCtx, row, row.InvalidReason, true, time.Time{})
			if failureErr != nil {
				errs = append(errs, failureErr)
				break
			}
			if failure.Terminal {
				processed++
			}
			continue
		}

		published, terminal, publishErr := d.publishRow(iterCtx, row)
		if published {
			publishedIDs = append(publishedIDs, row.ID)
			processed++
			continue
		}
		if publishErr != nil {
			if terminal {
				processed++
			}
			errs = append(errs, publishErr)
		}
		// A publish failure (with retry scheduled) is likely to affect the rest of the batch.
		// Leave the remaining rows leased and try again on a later iteration.
		break
	}

	if len(publishedIDs) > 0 {
		err := d.store.MarkPublished(iterCtx, publishedIDs)
		if err != nil {
			errs = append(errs, fmt.Errorf("mark processed: %w", err))
		} else {
			d.metrics.Published(len(publishedIDs))
		}
	}

	return processed, errors.Join(errs...)
}

func (d *Dispatcher) recordStats(ctx context.Context) {
	if pending, err := d.store.PendingStats(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		d.logger.Warn("get pending stats error", "error", err)
	} else {
		d.metrics.SetPending(pending.Count)

		var pendingAge time.Duration
		if pending.Count > 0 && !pending.Oldest.IsZero() {
			pendingAge = max(d.now().Sub(pending.Oldest), 0)
		}
		d.metrics.SetOldestAge(pendingAge)
	}

	if failed, err := d.store.FailedStats(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		d.logger.Warn("get failed stats error", "error", err)
	} else {
		d.metrics.SetFailed(failed.Count)

		var failedAge time.Duration
		if failed.Count > 0 && !failed.Oldest.IsZero() {
			failedAge = max(d.now().Sub(failed.Oldest), 0)
		}
		d.metrics.SetOldestFailedAge(failedAge)
	}
}

func (d *Dispatcher) publishRow(ctx context.Context, row Row) (published bool, terminal bool, err error) {
	err = d.pub.PublishJSON(ctx, events.Envelope{
		EventID:   row.EventID,
		EventType: row.EventType,
		Version:   row.Version,
		CreatedAt: row.CreatedAt,
		Payload:   row.Payload,
	})

	if err == nil {
		return true, false, nil
	}

	if ctx.Err() != nil {
		return false, false, ctx.Err()
	}

	if errors.Is(err, rabbitmq.ErrClosed) || errors.Is(err, rabbitmq.ErrUnavailable) {
		return false, false, err
	}

	isTerminal := errors.Is(err, rabbitmq.ErrInvalid)

	var nextAttemptAt time.Time
	if !isTerminal && row.Attempts+1 < d.cfg.maxAttempts {
		nextAttemptAt = d.now().Add(d.retryDelay(row.Attempts + 1))
	}

	failure, recordErr := d.recordFailure(ctx, row, err.Error(), isTerminal, nextAttemptAt)
	if recordErr != nil {
		return false, false, fmt.Errorf("record event failure %q: %w", row.EventID, recordErr)
	}
	if failure.Terminal {
		return false, true, fmt.Errorf("event %q exceeded %d attempts: %w", row.EventID, d.cfg.maxAttempts, err)
	}

	d.logger.Error(
		"outbox publish failed; retry scheduled",
		"outbox_id", row.ID,
		"event_type", row.EventType,
		"attempt", failure.Attempts,
		"next_attempt_at", nextAttemptAt,
		"error", err,
	)
	d.metrics.Retry()

	// The row is durably scheduled. It is not an iteration failure; a later
	// dispatch will pick it up when next_attempt_at is due.
	return false, false, nil
}

func (d *Dispatcher) recordFailure(
	ctx context.Context,
	row Row,
	reason string,
	terminal bool,
	nextAttemptAt time.Time,
) (Failure, error) {
	failure, err := d.store.RecordFailure(
		ctx,
		row.ID,
		reason,
		terminal,
		d.cfg.maxAttempts,
		nextAttemptAt,
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
