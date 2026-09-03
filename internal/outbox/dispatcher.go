package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"repo-watch/internal/rabbitmq"
	"sync"
	"time"
)

type Row struct {
	ID        int32
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

type Store interface {
	GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]Row, error)
	MarkPublished(ctx context.Context, ids []int32) error
}

type DispatcherConfig struct {
	BatchSize            int32
	LeaseDuration        time.Duration
	LoopInterval         time.Duration
	LoopIterationTimeout time.Duration
}

type DispatcherDeps struct {
	Config            DispatcherConfig
	Logger            *slog.Logger
	Publisher         rabbitmq.RabbitJSONPublisher
	Store             Store
	GetEventID        GetEventIDFn
	ValidateEventType ValidateEventTypeFn
}
type GetEventIDFn func(Row) string
type ValidateEventTypeFn func(eventType string) bool

type Dispatcher struct {
	cfg               DispatcherConfig
	logger            *slog.Logger
	pub               rabbitmq.RabbitJSONPublisher
	store             Store
	validateEventType ValidateEventTypeFn
	getEventID        GetEventIDFn
}

func NewDispatcher(d DispatcherDeps) (*Dispatcher, error) {
	if d.Config.BatchSize < 1 {
		return nil, fmt.Errorf("invalid batch size: %v", d.Config.BatchSize)
	}
	if d.Config.LoopIterationTimeout >= d.Config.LeaseDuration {
		return nil, fmt.Errorf(
			"loop iteration must be less than lease duration, got %v >= %v",
			d.Config.LoopIterationTimeout, d.Config.LeaseDuration)
	}
	if d.GetEventID == nil {
		return nil, fmt.Errorf("missing GetEventID")
	}
	if d.ValidateEventType == nil {
		return nil, fmt.Errorf("missing ValidateEventType")
	}

	return &Dispatcher{
		cfg:               d.Config,
		logger:            d.Logger,
		pub:               d.Publisher,
		store:             d.Store,
		getEventID:        d.GetEventID,
		validateEventType: d.ValidateEventType,
	}, nil
}

func (d *Dispatcher) Start(ctx context.Context, cancelCtx context.CancelCauseFunc) {
	d.logger.Info("starting dispatcher")

	var wg sync.WaitGroup

	wg.Go(func() {
		err := d.runLoop(ctx)
		if !errors.Is(err, context.Canceled) {
			d.logger.Error("dispatcher exited", "error", err)
		}
		cancelCtx(err)
	})

	<-ctx.Done()
	wg.Wait()
}

func (d *Dispatcher) runLoop(ctx context.Context) error {
	for {
		n, err := d.runIteration(ctx)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled):
				return ctx.Err()
			case errors.Is(err, context.DeadlineExceeded):
				d.logger.Warn("iteration timeout")
			default:
				return err
			}
		}
		if n > 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return context.Canceled

		case <-time.After(d.cfg.LoopInterval):
		}
	}
}

func (d *Dispatcher) runIteration(ctx context.Context) (processed int, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.LoopIterationTimeout)
	defer cancel()

	batch, err := d.store.GetNextBatch(ctx, d.cfg.BatchSize, time.Now().Add(d.cfg.LeaseDuration))
	if err != nil {
		return 0, fmt.Errorf("get next batch: %w", err)
	}

	if len(batch) < 1 {
		return 0, nil
	}

	processed = len(batch)
	publishedIDs := []int32{}
	errs := []error{}

	for _, row := range batch {
		if d.validateEventType != nil && !d.validateEventType(row.EventType) {
			errs = append(errs, fmt.Errorf("invalid event type: %q", row.EventType))
			break
		}

		if err := d.pub.PublishJSON(ctx, rabbitmq.EnvelopeJSON{
			EventID:   d.getEventID(row),
			EventType: row.EventType,
			CreatedAt: row.CreatedAt,
			Payload:   row.Payload,
		}); err != nil {
			d.logger.Error("publish event", "error", err)
			break
		}

		publishedIDs = append(publishedIDs, row.ID)
	}

	if len(publishedIDs) < 1 {
		return processed, errors.Join(errs...)
	}

	markCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = d.store.MarkPublished(markCtx, publishedIDs); err != nil {
		errs = append(errs, fmt.Errorf("mark processed: %w", err))
	}

	return processed, errors.Join(errs...)
}
