package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
)

type fakeStore struct {
	rows   []Row
	getErr error

	pendingStats PendingStats
	pendingErr   error

	failedStats FailedStats
	failedErr   error

	getCalls         int
	batchesLeased    int
	markPublishedIDs []int32
	markPublishedErr error

	failures  []failureRecord
	attempts  map[int32]int32
	recordErr error
}

type failureRecord struct {
	ID            int32
	LastError     string
	Terminal      bool
	MaxAttempts   int32
	NextAttemptAt time.Time
	Result        Failure
}

func (s *fakeStore) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]Row, error) {
	s.getCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.getErr != nil {
		return nil, s.getErr
	}
	rows := s.rows
	s.rows = nil
	s.batchesLeased++
	return rows, nil
}

func (s *fakeStore) MarkPublished(ctx context.Context, ids []int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.markPublishedIDs = append(s.markPublishedIDs, ids...)
	return s.markPublishedErr
}

func (s *fakeStore) PendingStats(ctx context.Context) (PendingStats, error) {
	if err := ctx.Err(); err != nil {
		return PendingStats{}, err
	}
	return s.pendingStats, s.pendingErr
}

func (s *fakeStore) FailedStats(ctx context.Context) (FailedStats, error) {
	if err := ctx.Err(); err != nil {
		return FailedStats{}, err
	}
	return s.failedStats, s.failedErr
}

func (s *fakeStore) RecordFailure(
	ctx context.Context,
	id int32,
	lastError string,
	terminal bool,
	maxAttempts int32,
	nextAttemptAt time.Time,
) (Failure, error) {
	if err := ctx.Err(); err != nil {
		return Failure{}, err
	}
	if s.attempts == nil {
		s.attempts = map[int32]int32{}
	}
	s.attempts[id]++

	rec := failureRecord{
		ID:            id,
		LastError:     lastError,
		Terminal:      terminal,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: nextAttemptAt,
	}
	if s.recordErr != nil {
		s.failures = append(s.failures, rec)
		return Failure{}, s.recordErr
	}

	res := Failure{
		Attempts: s.attempts[id],
		Terminal: terminal || s.attempts[id] >= maxAttempts,
	}
	rec.Result = res
	s.failures = append(s.failures, rec)
	return res, nil
}

type fakePublisher struct {
	calls     []events.Envelope
	errs      []error
	alwaysErr error
	onPublish func(context.Context)
}

func (p *fakePublisher) PublishJSON(ctx context.Context, msg events.Envelope) error {
	idx := len(p.calls)
	p.calls = append(p.calls, msg)
	if p.onPublish != nil {
		p.onPublish(ctx)
	}
	if p.alwaysErr != nil {
		return p.alwaysErr
	}
	if idx < len(p.errs) {
		return p.errs[idx]
	}
	return nil
}

type fakeMetrics struct {
	published       int
	retries         int
	deadLettered    int
	unavailable     int
	pending         int64
	oldestAge       time.Duration
	failed          int64
	oldestFailedAge time.Duration
}

func (m *fakeMetrics) Published(count int) { m.published += count }
func (m *fakeMetrics) Retry()              { m.retries++ }
func (m *fakeMetrics) DeadLettered()       { m.deadLettered++ }
func (m *fakeMetrics) Unavailable()        { m.unavailable++ }
func (m *fakeMetrics) SetPending(count int64) {
	m.pending = count
}
func (m *fakeMetrics) SetOldestAge(age time.Duration) {
	m.oldestAge = age
}
func (m *fakeMetrics) SetFailed(count int64) {
	m.failed = count
}
func (m *fakeMetrics) SetOldestFailedAge(age time.Duration) {
	m.oldestFailedAge = age
}

func ptr[T any](v T) *T {
	return &v
}

func testConfig() DispatcherConfig {
	return DispatcherConfig{
		BatchSize:         ptr(int32(10)),
		LeaseDuration:     ptr(time.Second),
		PollInterval:      ptr(time.Millisecond),
		ProcessingTimeout: ptr(500 * time.Millisecond),
		RetryBaseBackoff:  ptr(time.Millisecond),
		RetryMaxBackoff:   ptr(10 * time.Millisecond),
		MaxAttempts:       ptr(int32(3)),
	}
}

type dispatcherTestOption func(*DispatcherDeps)

func withNow(now func() time.Time) dispatcherTestOption {
	return func(d *DispatcherDeps) {
		d.Now = now
	}
}

func withSleep(sleep func(context.Context, time.Duration) error) dispatcherTestOption {
	return func(d *DispatcherDeps) {
		d.SleepCtx = sleep
	}
}

func newTestDispatcher(
	t *testing.T,
	store Store,
	pub rabbitmq.RabbitJSONPublisher,
	cfg DispatcherConfig,
	metrics MetricsRecorder,
	opts ...dispatcherTestOption,
) *Dispatcher {
	t.Helper()

	deps := DispatcherDeps{
		Config:    cfg,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Publisher: pub,
		Store:     store,
		Metrics:   metrics,
	}
	for _, opt := range opts {
		opt(&deps)
	}

	d, err := NewDispatcher(deps)
	require.NoError(t, err)

	return d
}

func testRow(id int32) Row {
	return Row{
		ID:            id,
		EventType:     events.EventEmailQueued,
		EventID:       "event-" + strconv.Itoa(int(id)),
		Version:       events.VersionEmailQueued,
		InvalidReason: "",
		Payload:       json.RawMessage(`{}`),
		CreatedAt:     time.Now(),
	}
}

func TestRunIterationPublishesBatch(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1), testRow(2)}}
	pub := &fakePublisher{}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Equal(t, 2, processed)
	require.Len(t, pub.calls, 2)
	require.Equal(t, events.EventEmailQueued, pub.calls[0].EventType)
	require.Equal(t, events.VersionEmailQueued, pub.calls[0].Version)
	require.Equal(t, []int32{1, 2}, store.markPublishedIDs)
	require.Empty(t, store.failures)
	require.Equal(t, 2, metrics.published)
}

func TestRunIterationMarksOnlySuccessfullyPublishedIDs(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = ptr(int32(1))

	store := &fakeStore{rows: []Row{testRow(1), testRow(2), testRow(3)}}
	pub := &fakePublisher{errs: []error{nil, errors.New("broker down")}}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, cfg, metrics)

	processed, err := d.runIteration(context.Background())

	require.ErrorContains(t, err, "exceeded 1 attempts")
	require.Equal(t, 2, processed)
	require.Len(t, pub.calls, 2, "the batch stops after a publish failure")
	require.Equal(t, []int32{1}, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.Equal(t, int32(2), store.failures[0].ID)
	require.True(t, store.failures[0].Result.Terminal)
	require.Equal(t, 1, metrics.published)
	require.Equal(t, 1, metrics.deadLettered)
}

func TestRunIterationSchedulesRetry(t *testing.T) {
	now := time.Now()
	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &fakePublisher{alwaysErr: errors.New("temporary failure")}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics,
		withNow(func() time.Time { return now }),
		withSleep(func(context.Context, time.Duration) error {
			t.Fatal("row retries must not sleep inside an iteration")
			return nil
		}),
	)

	processed, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Zero(t, processed)
	require.Len(t, pub.calls, 1, "a failed row is scheduled, not retried immediately")
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.Equal(t, int32(1), store.failures[0].ID)
	require.False(t, store.failures[0].Result.Terminal)
	require.Equal(t, now.Add(time.Millisecond), store.failures[0].NextAttemptAt)
	require.Equal(t, 1, metrics.retries)
	require.Zero(t, metrics.published)
	require.Zero(t, metrics.deadLettered)
}

func TestRunIterationSchedulesRetryUsingCurrentAttempt(t *testing.T) {
	now := time.Now()
	row := testRow(1)
	row.Attempts = 2

	cfg := testConfig()
	cfg.MaxAttempts = ptr(int32(4))

	store := &fakeStore{
		rows:     []Row{row},
		attempts: map[int32]int32{1: 2},
	}
	pub := &fakePublisher{alwaysErr: errors.New("temporary failure")}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, cfg, metrics,
		withNow(func() time.Time { return now }),
	)

	_, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Len(t, store.failures, 1)
	// retryDelay(3) with 1ms base and 10ms cap is 4ms.
	require.Equal(t, now.Add(4*time.Millisecond), store.failures[0].NextAttemptAt)
}

func TestRunIterationDeadLettersInvalidEventType(t *testing.T) {
	row := testRow(1)
	row.EventType = "bogus"
	invalidReason := `invalid event type: "bogus"`
	row.InvalidReason = invalidReason

	store := &fakeStore{rows: []Row{row}}
	pub := &fakePublisher{}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Empty(t, pub.calls)
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.True(t, store.failures[0].Result.Terminal)
	require.Contains(t, store.failures[0].LastError, invalidReason)
	require.Equal(t, 1, metrics.deadLettered)
}

func TestRunIterationDeadLettersInvalidPayload(t *testing.T) {
	row := testRow(1)
	row.InvalidReason = "invalid payload: unexpected end of JSON input"

	store := &fakeStore{rows: []Row{row}}
	pub := &fakePublisher{}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Empty(t, pub.calls)
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.True(t, store.failures[0].Result.Terminal)
	require.Equal(t, row.InvalidReason, store.failures[0].LastError)
	require.Equal(t, 1, metrics.deadLettered)
}

func TestRunIterationDoesNotConsumeAttemptWhenClientIsUnavailable(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1), testRow(2)}}
	pub := &fakePublisher{alwaysErr: fmt.Errorf(
		"%w: connection closed", rabbitmq.ErrUnavailable)}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())

	require.ErrorIs(t, err, rabbitmq.ErrUnavailable)
	require.Zero(t, processed)
	require.Len(t, pub.calls, 1, "the batch stops when the broker is unavailable")
	require.Empty(t, store.markPublishedIDs)
	require.Empty(t, store.failures, "an outage must not count against the attempt budget")
	require.Zero(t, metrics.unavailable, "the loop, not a single iteration, records unavailable")
	require.Zero(t, metrics.retries)
	require.Zero(t, metrics.deadLettered)
}

func TestRunIterationKeepsEarlierPublishesWhenBrokerBecomesUnavailable(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1), testRow(2), testRow(3)}}
	pub := &fakePublisher{errs: []error{
		nil,
		fmt.Errorf("%w: channel closed", rabbitmq.ErrUnavailable),
	}}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())

	require.ErrorIs(t, err, rabbitmq.ErrUnavailable)
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 2)
	require.Equal(t, []int32{1}, store.markPublishedIDs)
	require.Empty(t, store.failures)
	require.Equal(t, 1, metrics.published)
	require.Zero(t, metrics.unavailable, "the loop, not a single iteration, records unavailable")
}

func TestRunIterationConsumesAttemptWhenConnectionIsLostDuringPublish(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = ptr(int32(1))

	store := &fakeStore{rows: []Row{testRow(1), testRow(2)}}
	// A connection loss during publishing surfaces as a plain publish error,
	// not as ErrUnavailable from the client. It is ambiguous whether the
	// broker persisted the event, so the row consumes an attempt.
	pub := &fakePublisher{alwaysErr: errors.New("amqp: connection closed")}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, cfg, metrics)

	processed, err := d.runIteration(context.Background())

	require.ErrorContains(t, err, "exceeded 1 attempts")
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 1, "the batch stops after the connection is lost")
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.Equal(t, int32(1), store.failures[0].ID)
	require.True(t, store.failures[0].Result.Terminal)
	require.Equal(t, 1, metrics.deadLettered)
	require.Zero(t, metrics.unavailable)
	require.Zero(t, metrics.retries)
}

func TestRunLoopRecordsUnavailableIterations(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &fakePublisher{alwaysErr: fmt.Errorf(
		"%w: connection closed", rabbitmq.ErrUnavailable)}
	metrics := &fakeMetrics{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var delays []time.Duration
	d := newTestDispatcher(t, store, pub, testConfig(), metrics, withSleep(func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		cancel()
		return context.Canceled
	}))

	err := d.Start(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Millisecond}, delays)
	require.Equal(t, 1, metrics.unavailable)
	require.Zero(t, metrics.retries)
	require.Zero(t, metrics.deadLettered)
}

func TestRunIterationDeadLettersAfterAttemptLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = ptr(int32(2))

	row := testRow(1)
	row.Attempts = 1

	store := &fakeStore{
		rows:     []Row{row},
		attempts: map[int32]int32{1: 1},
	}
	pub := &fakePublisher{alwaysErr: errors.New("broker down")}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, cfg, metrics)

	processed, err := d.runIteration(context.Background())

	require.ErrorContains(t, err, "exceeded 2 attempts")
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 1)
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.True(t, store.failures[0].Result.Terminal)
	require.True(t, store.failures[0].NextAttemptAt.IsZero())
	require.Zero(t, metrics.retries)
	require.Equal(t, 1, metrics.deadLettered)
}

func TestRunIterationRecordsPendingStats(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		pendingStats: PendingStats{
			Count:  4,
			Oldest: now.Add(-90 * time.Second),
		},
	}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), metrics,
		withNow(func() time.Time { return now }),
	)

	_, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(4), metrics.pending)
	require.Equal(t, 90*time.Second, metrics.oldestAge)
}

func TestPendingStatsFailureDoesNotStopPublishing(t *testing.T) {
	store := &fakeStore{
		rows:       []Row{testRow(1)},
		pendingErr: errors.New("stats unavailable"),
	}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []int32{1}, store.markPublishedIDs)
}

func TestRunIterationRecordsFailedStats(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		failedStats: FailedStats{
			Count:  3,
			Oldest: now.Add(-2 * time.Minute),
		},
	}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), metrics,
		withNow(func() time.Time { return now }),
	)

	_, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), metrics.failed)
	require.Equal(t, 2*time.Minute, metrics.oldestFailedAge)
}

func TestRunIterationRecordsEmptyFailedStatsAsZeroAge(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		failedStats: FailedStats{
			Count:  0,
			Oldest: now.Add(-time.Hour),
		},
	}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), metrics,
		withNow(func() time.Time { return now }),
	)

	_, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Zero(t, metrics.failed)
	require.Zero(t, metrics.oldestFailedAge)
}

func TestFailedStatsFailureDoesNotStopPublishing(t *testing.T) {
	store := &fakeStore{
		rows:      []Row{testRow(1)},
		failedErr: errors.New("stats unavailable"),
	}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), metrics)

	processed, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []int32{1}, store.markPublishedIDs)
}

type blockingPublisher struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingPublisher) PublishJSON(ctx context.Context, _ events.Envelope) error {
	close(p.started)
	<-p.release

	// The dispatcher must detach an in-flight confirmation from shutdown
	// cancellation, otherwise this context would already be canceled.
	return ctx.Err()
}

func TestRunIterationWaitsForInFlightPublisherConfirm(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &blockingPublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	d := newTestDispatcher(t, store, pub, testConfig(), &fakeMetrics{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := d.runIteration(ctx)
		done <- err
	}()

	<-pub.started
	cancel()
	close(pub.release)

	require.NoError(t, <-done)
	require.Equal(t, []int32{1}, store.markPublishedIDs)
}

func TestRunLoopRetriesTransientErrorsWithBackoff(t *testing.T) {
	store := &fakeStore{getErr: errors.New("database down")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var delays []time.Duration
	d := newTestDispatcher(t, store, &fakePublisher{}, testConfig(), &fakeMetrics{}, withSleep(func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 3 {
			cancel()
			return context.Canceled
		}
		return nil
	}))

	err := d.Start(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 3, store.getCalls)
	require.Equal(t, []time.Duration{
		time.Millisecond,
		2 * time.Millisecond,
		4 * time.Millisecond,
	}, delays)
}

func TestRunIterationDoesNotLeaseBatchWhenShutdown(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &fakePublisher{}
	d := newTestDispatcher(t, store, pub, testConfig(), &fakeMetrics{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	processed, err := d.runIteration(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, processed)
	require.Zero(t, store.batchesLeased, "a shutting down dispatcher must not lease rows")
	require.Empty(t, pub.calls)
	require.Empty(t, store.markPublishedIDs)
}

func TestRunIterationStopsBatchOnShutdown(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1), testRow(2), testRow(3)}}
	pub := &fakePublisher{}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub.onPublish = func(context.Context) { cancel() }

	processed, err := d.runIteration(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 1, "rows after shutdown must not be published")
	require.Equal(t, []int32{1}, store.markPublishedIDs, "confirmed publishes are still marked")
	require.Empty(t, store.failures)
	require.Equal(t, 1, metrics.published)
}

func TestRunLoopDoesNotLeaseAnotherBatchAfterShutdown(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1), testRow(2)}}
	pub := &fakePublisher{}
	d := newTestDispatcher(t, store, pub, testConfig(), &fakeMetrics{},
		withSleep(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub.onPublish = func(context.Context) { cancel() }

	err := d.Start(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.batchesLeased, "shutdown must not abandon a freshly leased batch")
	require.Equal(t, []int32{1}, store.markPublishedIDs)
}

// contextPublisher blocks until its context is done, like a publisher confirm
// that never arrives.
type contextPublisher struct{}

func (p *contextPublisher) PublishJSON(ctx context.Context, _ events.Envelope) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRunIterationPublishTimeoutKeepsRowLeased(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseDuration = ptr(time.Second)
	cfg.ProcessingTimeout = ptr(25 * time.Millisecond)

	store := &fakeStore{rows: []Row{testRow(1), testRow(2)}}
	d := newTestDispatcher(t, store, &contextPublisher{}, cfg, &fakeMetrics{})

	start := time.Now()
	processed, err := d.runIteration(context.Background())

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second, "processing must stop before the lease expires")
	require.Zero(t, processed)
	require.Empty(t, store.markPublishedIDs)
	require.Empty(t, store.failures, "a timed out publish must not consume an attempt")
}
