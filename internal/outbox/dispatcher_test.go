package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"repo-watch/internal/rabbitmq"

	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	rows   []Row
	getErr error

	getCalls         int
	markPublishedIDs []int32
	markPublishedErr error

	failures  []failureRecord
	attempts  map[int32]int32
	recordErr error
}

type failureRecord struct {
	ID          int32
	LastError   string
	Terminal    bool
	MaxAttempts int32
	Result      Failure
}

func (s *fakeStore) GetNextBatch(ctx context.Context, batchSize int32, leasedUntil time.Time) ([]Row, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	rows := s.rows
	s.rows = nil
	return rows, nil
}

func (s *fakeStore) MarkPublished(ctx context.Context, ids []int32) error {
	s.markPublishedIDs = append(s.markPublishedIDs, ids...)
	return s.markPublishedErr
}

func (s *fakeStore) RecordFailure(
	ctx context.Context,
	id int32,
	lastError string,
	terminal bool,
	maxAttempts int32,
) (Failure, error) {
	if s.attempts == nil {
		s.attempts = map[int32]int32{}
	}
	s.attempts[id]++

	rec := failureRecord{
		ID:          id,
		LastError:   lastError,
		Terminal:    terminal,
		MaxAttempts: maxAttempts,
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
	calls     []rabbitmq.EnvelopeJSON
	errs      []error
	alwaysErr error
	onPublish func()
}

func (p *fakePublisher) PublishJSON(ctx context.Context, msg rabbitmq.EnvelopeJSON) error {
	idx := len(p.calls)
	p.calls = append(p.calls, msg)
	if p.onPublish != nil {
		p.onPublish()
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
	published    int
	retries      int
	deadLettered int
}

func (m *fakeMetrics) Published(count int) { m.published += count }
func (m *fakeMetrics) Retry()              { m.retries++ }
func (m *fakeMetrics) DeadLettered()       { m.deadLettered++ }

func ptr[T any](v T) *T {
	return &v
}

func testConfig() DispatcherConfig {
	return DispatcherConfig{
		BatchSize:            ptr(int32(10)),
		LeaseDuration:        ptr(time.Second),
		LoopInterval:         ptr(time.Millisecond),
		LoopIterationTimeout: ptr(500 * time.Millisecond),
		RetryBaseBackoff:     ptr(time.Millisecond),
		RetryMaxBackoff:      ptr(10 * time.Millisecond),
		MaxAttempts:          ptr(int32(3)),
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

func TestNewDispatcherAppliesDefaults(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	d, err := NewDispatcher(DispatcherDeps{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Publisher: &fakePublisher{},
		Store:     store,
	})
	require.NoError(t, err)
	require.Equal(t, int32(5), d.cfg.batchSize)
	require.Equal(t, int32(5), d.cfg.maxAttempts)
	require.Equal(t, 5*time.Second, d.cfg.leaseDuration)
	require.Equal(t, 5*time.Second, d.cfg.loopInterval)
	require.Equal(t, 3*time.Second, d.cfg.loopIterationTimeout)
	require.Equal(t, 250*time.Millisecond, d.cfg.retryBaseBackoff)
	require.Equal(t, 5*time.Second, d.cfg.retryMaxBackoff)

	// The default config has no metrics recorder; publishing must still work.
	processed, err := d.runIteration(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
}

func testRow(id int32) Row {
	return Row{
		ID:            id,
		EventType:     "valid",
		EventID:       "event-" + strconv.Itoa(int(id)),
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
	d := newTestDispatcher(t, store, pub, cfg, metrics, withSleep(func(context.Context, time.Duration) error {
		return nil
	}))

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

func TestRunIterationRetriesPublishWithinLease(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &fakePublisher{errs: []error{errors.New("temporary failure"), nil}}
	metrics := &fakeMetrics{}
	var delays []time.Duration
	d := newTestDispatcher(t, store, pub, testConfig(), metrics, withSleep(func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}))

	processed, err := d.runIteration(context.Background())

	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 2)
	require.Equal(t, []int32{1}, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.False(t, store.failures[0].Result.Terminal)
	require.Equal(t, []time.Duration{time.Millisecond}, delays)
	require.Equal(t, 1, metrics.retries)
	require.Equal(t, 1, metrics.published)
	require.Zero(t, metrics.deadLettered)
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

func TestRunIterationStopsRetryingAfterLeaseExpiry(t *testing.T) {
	store := &fakeStore{rows: []Row{testRow(1)}}
	metrics := &fakeMetrics{}

	now := time.Now()
	pub := &fakePublisher{errs: []error{errors.New("broker down")}}
	d := newTestDispatcher(t, store, pub, testConfig(), metrics,
		withNow(func() time.Time { return now }),
		withSleep(func(context.Context, time.Duration) error {
			t.Fatal("sleep must not be called after the lease expires")
			return nil
		}),
	)
	pub.onPublish = func() {
		now = now.Add(2 * time.Second)
	}

	processed, err := d.runIteration(context.Background())

	require.ErrorContains(t, err, "lease expires before retry")
	require.Zero(t, processed)
	require.Len(t, pub.calls, 1)
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 1)
	require.False(t, store.failures[0].Result.Terminal)
	require.Zero(t, metrics.deadLettered)
}

func TestRunIterationDeadLettersAfterAttemptLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = ptr(int32(2))

	store := &fakeStore{rows: []Row{testRow(1)}}
	pub := &fakePublisher{alwaysErr: errors.New("broker down")}
	metrics := &fakeMetrics{}
	d := newTestDispatcher(t, store, pub, cfg, metrics, withSleep(func(context.Context, time.Duration) error {
		return nil
	}))

	processed, err := d.runIteration(context.Background())

	require.ErrorContains(t, err, "exceeded 2 attempts")
	require.Equal(t, 1, processed)
	require.Len(t, pub.calls, 2)
	require.Empty(t, store.markPublishedIDs)
	require.Len(t, store.failures, 2)
	require.False(t, store.failures[0].Result.Terminal)
	require.True(t, store.failures[1].Result.Terminal)
	require.Equal(t, 1, metrics.retries)
	require.Equal(t, 1, metrics.deadLettered)
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
