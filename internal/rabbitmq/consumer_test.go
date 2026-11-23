package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"repo-watch/internal/events"
	"repo-watch/internal/observability"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type fakeAcknowledger struct {
	ackCalled     bool
	nackCalled    bool
	nackRequeue   bool
	rejectCalled  bool
	rejectRequeue bool
	err           error
}

func (a *fakeAcknowledger) Ack(uint64, bool) error {
	a.ackCalled = true
	return a.err
}

func (a *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nackCalled = true
	a.nackRequeue = requeue
	return a.err
}

func (a *fakeAcknowledger) Reject(_ uint64, requeue bool) error {
	a.rejectCalled = true
	a.rejectRequeue = requeue
	return a.err
}

type fakeConsumerMetrics struct {
	handled              []AckType
	failures             int
	defects              int
	panics               int
	requeues             int
	retriesScheduled     []string
	retriesExhausted     int
	retryPublishFailures int
	returns              []string
}

func (m *fakeConsumerMetrics) MessageHandled(_ string, ack AckType) {
	m.handled = append(m.handled, ack)
}

func (m *fakeConsumerMetrics) HandlerFailed(string)         { m.failures++ }
func (m *fakeConsumerMetrics) HandlerDefectDetected(string) { m.defects++ }
func (m *fakeConsumerMetrics) HandlerPanicked(string)       { m.panics++ }
func (m *fakeConsumerMetrics) Requeued(string)              { m.requeues++ }
func (m *fakeConsumerMetrics) RetryScheduled(_ string, tier string) {
	m.retriesScheduled = append(m.retriesScheduled, tier)
}
func (m *fakeConsumerMetrics) RetryExhausted(string)     { m.retriesExhausted++ }
func (m *fakeConsumerMetrics) RetryPublishFailed(string) { m.retryPublishFailures++ }
func (m *fakeConsumerMetrics) MessageReturned(routingKey string) {
	m.returns = append(m.returns, routingKey)
}
func (m *fakeConsumerMetrics) SetConnected(bool) {}
func (m *fakeConsumerMetrics) ConnectionLost()   {}

func testDelivery(acknowledger *fakeAcknowledger, body string) *amqp091.Delivery {
	return &amqp091.Delivery{
		Acknowledger: acknowledger,
		RoutingKey:   "repository.changed",
		MessageId:    "message-1",
		Body:         []byte(body),
	}
}

func testDeliveryWithHeaders(
	acknowledger *fakeAcknowledger,
	body string,
	headers amqp091.Table,
) *amqp091.Delivery {
	delivery := testDelivery(acknowledger, body)
	delivery.Headers = headers
	return delivery
}

type retryCall struct {
	queue   string
	attempt int32
}

type fakeRetryPublisher struct {
	calls []retryCall
	err   error
}

func (p *fakeRetryPublisher) PublishRetry(
	_ context.Context,
	queue string,
	_ amqp091.Delivery,
	attempt int32,
) error {
	p.calls = append(p.calls, retryCall{queue: queue, attempt: attempt})
	return p.err
}

func newTestConsumer(t *testing.T, metrics MetricsRecorder) *consumer {
	t.Helper()

	c, err := NewConsumer(newTestClient(t), testLogger(), metrics)
	require.NoError(t, err)
	return c
}

func newTestSession(
	t *testing.T,
	metrics MetricsRecorder,
	handler JSONEventHandler,
	cfg *ConsumerSessionConfig,
) *consumerSession {
	t.Helper()

	c := newTestConsumer(t, metrics)
	session := &consumerSession{
		client:    c.client,
		logger:    c.logger,
		metrics:   c.metrics,
		retryPub:  c.retryPub,
		ctx:       t.Context(),
		queueName: "test-queue",
		handler:   handler,
		cfg:       ConsumerSessionConfig{HandlerTimeout: defaultHandlerTimeout},
	}
	if cfg != nil {
		session.cfg = *cfg
	}
	return session
}

func TestHandleDeliveryDiscardsInvalidEnvelope(t *testing.T) {
	called := false
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		called = true
		return Ack, nil
	}, nil)

	err := s.handleDelivery(testDelivery(acknowledger, `{"id":`))

	require.NoError(t, err)
	require.False(t, called)
	require.True(t, acknowledger.rejectCalled)
	require.False(t, acknowledger.rejectRequeue)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.Equal(t, []AckType{NackDiscard}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Zero(t, metrics.defects)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliveryDiscardsMissingEventID(t *testing.T) {
	called := false
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		called = true
		return Ack, nil
	}, nil)

	err := s.handleDelivery(testDelivery(acknowledger, `{"type":"repository.changed","payload":{}}`))

	require.NoError(t, err)
	require.False(t, called)
	require.True(t, acknowledger.rejectCalled)
	require.False(t, acknowledger.rejectRequeue)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.Equal(t, []AckType{NackDiscard}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Zero(t, metrics.defects)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliveryDelegatesToHandler(t *testing.T) {
	wantErr := errors.New("transient database failure")
	var got events.Envelope
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(_ context.Context, msg events.Envelope) (AckType, error) {
		got = msg
		return NackRequeue, wantErr
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.Equal(t, "event-1", got.EventID)
	require.Equal(t, events.EventRepositoryChanged, got.EventType)
	require.True(t, acknowledger.nackCalled)
	require.True(t, acknowledger.nackRequeue)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Equal(t, []AckType{NackRequeue}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Zero(t, metrics.defects)
	require.Equal(t, 1, metrics.requeues)
}

func TestHandleDeliveryPropagatesCorrelationID(t *testing.T) {
	tests := []struct {
		name          string
		correlationID string
		body          string
		want          string
	}{
		{
			name:          "amqp correlation id",
			correlationID: "corr-amqp",
			body:          `{"id":"event-1","type":"repository.changed","payload":{}}`,
			want:          "corr-amqp",
		},
		{
			name: "envelope correlation id",
			body: `{"id":"event-1","type":"repository.changed","correlation_id":"corr-envelope","payload":{}}`,
			want: "corr-envelope",
		},
		{
			name: "event id fallback",
			body: `{"id":"event-1","type":"repository.changed","payload":{}}`,
			want: "event-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string

			s := newTestSession(t, &fakeConsumerMetrics{}, func(ctx context.Context, _ events.Envelope) (AckType, error) {
				got = observability.GetCorrelationID(ctx)
				return Ack, nil
			}, nil)

			delivery := testDelivery(&fakeAcknowledger{}, tt.body)
			delivery.CorrelationId = tt.correlationID

			require.NoError(t, s.handleDelivery(delivery))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHandleDeliveryRecoversHandlerPanic(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		panic("boom")
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.False(t, acknowledger.nackCalled)
	require.False(t, acknowledger.ackCalled)
	require.True(t, acknowledger.rejectCalled)
	require.False(t, acknowledger.rejectRequeue)
	require.Equal(t, []AckType{NackDiscard}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Equal(t, 1, metrics.panics)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliverySchedulesFirstRetryTier(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	retryPub := &fakeRetryPublisher{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return NackRequeue, errors.New("transient database failure")
	}, nil)
	s.retryPub = retryPub

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.True(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Len(t, retryPub.calls, 1)
	require.Equal(t, "test-queue.retry.30s", retryPub.calls[0].queue)
	require.Equal(t, int32(1), retryPub.calls[0].attempt)
	require.Equal(t, []AckType{NackRequeue}, metrics.handled)
	require.Equal(t, []string{"30s"}, metrics.retriesScheduled)
	require.Zero(t, metrics.requeues)
	require.Zero(t, metrics.retriesExhausted)
	require.Zero(t, metrics.retryPublishFailures)
}

func TestHandleDeliverySchedulesLastRetryTier(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	retryPub := &fakeRetryPublisher{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return NackRequeue, errors.New("transient database failure")
	}, nil)
	s.retryPub = retryPub

	err := s.handleDelivery(testDeliveryWithHeaders(
		acknowledger,
		`{"id":"event-1","type":"repository.changed","payload":{}}`,
		amqp091.Table{RetryCountHeader: int32(1)},
	))

	require.NoError(t, err)
	require.True(t, acknowledger.ackCalled)
	require.Len(t, retryPub.calls, 1)
	require.Equal(t, "test-queue.retry.10m", retryPub.calls[0].queue)
	require.Equal(t, int32(2), retryPub.calls[0].attempt)
	require.Equal(t, []string{"10m"}, metrics.retriesScheduled)
}

func TestHandleDeliveryDeadLettersAfterRetryLimit(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	retryPub := &fakeRetryPublisher{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return NackRequeue, errors.New("transient database failure")
	}, nil)
	s.retryPub = retryPub

	err := s.handleDelivery(testDeliveryWithHeaders(
		acknowledger,
		`{"id":"event-1","type":"repository.changed","payload":{}}`,
		amqp091.Table{RetryCountHeader: int32(defaultMaxRetryAttempts)},
	))

	require.NoError(t, err)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.True(t, acknowledger.rejectCalled)
	require.False(t, acknowledger.rejectRequeue)
	require.Empty(t, retryPub.calls)
	require.Equal(t, 1, metrics.retriesExhausted)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliveryRequeuesWhenRetryPublishFails(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	retryPub := &fakeRetryPublisher{err: errors.New("publish failed")}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return NackRequeue, errors.New("transient database failure")
	}, nil)
	s.retryPub = retryPub

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.False(t, acknowledger.ackCalled)
	require.True(t, acknowledger.nackCalled)
	require.True(t, acknowledger.nackRequeue)
	require.False(t, acknowledger.rejectCalled)
	require.Equal(t, 1, metrics.retryPublishFailures)
	require.Equal(t, 1, metrics.requeues)
	require.Empty(t, metrics.retriesScheduled)
}

func TestHandleDeliveryAcksSuccessfulHandler(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return Ack, nil
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.True(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Equal(t, []AckType{Ack}, metrics.handled)
	require.Zero(t, metrics.failures)
	require.Zero(t, metrics.defects)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliveryReturnsAckError(t *testing.T) {
	ackErr := errors.New("ack failed")
	acknowledger := &fakeAcknowledger{err: ackErr}
	s := newTestSession(t, &fakeConsumerMetrics{}, func(context.Context, events.Envelope) (AckType, error) {
		return Ack, nil
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.ErrorIs(t, err, ackErr)
}

func TestHandleDeliveryRequeuesAckWithErrorAsDefect(t *testing.T) {
	handlerErr := errors.New("post-ack failure")
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return Ack, handlerErr
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.True(t, acknowledger.nackCalled)
	require.True(t, acknowledger.nackRequeue)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Equal(t, []AckType{NackRequeue}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Equal(t, 1, metrics.defects)
	require.Equal(t, 1, metrics.requeues)
}

func TestHandleDeliveryReturnsInvalidAckAsFatalDefect(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	s := newTestSession(t, metrics, func(context.Context, events.Envelope) (AckType, error) {
		return AckType("not-a-valid-ack"), nil
	}, nil)

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.ErrorIs(t, err, errInvalidAck)
	require.True(t, isFatalConsumeError(err))
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Empty(t, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Equal(t, 1, metrics.defects)
	require.Zero(t, metrics.requeues)
}

func TestHandleDeliveryAppliesHandlerTimeout(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	s := newTestSession(t, &fakeConsumerMetrics{}, func(ctx context.Context, _ events.Envelope) (AckType, error) {
		<-ctx.Done()
		return NackRequeue, ctx.Err()
	}, &ConsumerSessionConfig{HandlerTimeout: 10 * time.Millisecond})

	err := s.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
	)

	require.NoError(t, err)
	require.True(t, acknowledger.nackCalled)
	require.True(t, acknowledger.nackRequeue)
}

func TestIsFatalConsumeError(t *testing.T) {
	require.True(t, isFatalConsumeError(errInvalidAck))
	require.True(t, isFatalConsumeError(fmt.Errorf("wrapped: %w", errInvalidAck)))

	require.True(t, isFatalConsumeError(&amqp091.Error{
		Code:   amqp091.NotFound,
		Server: true,
	}))
	require.False(t, isFatalConsumeError(&amqp091.Error{
		Code:   amqp091.ContentTooLarge,
		Server: true,
	}))
	require.False(t, isFatalConsumeError(&amqp091.Error{
		Code:   amqp091.NotFound,
		Server: false,
	}))
	require.False(t, isFatalConsumeError(errors.New("plain error")))
}

func TestNewConsumerValidatesDependencies(t *testing.T) {
	_, err := NewConsumer(nil, testLogger(), nil)
	require.ErrorContains(t, err, "missing client")

	_, err = NewConsumer(newTestClient(t), nil, nil)
	require.ErrorContains(t, err, "missing logger")

	c, err := NewConsumer(newTestClient(t), testLogger(), nil)
	require.NoError(t, err)
	require.NotNil(t, c.metrics)
}

func TestConsumeJSONReturnsNilWhenContextIsCancelled(t *testing.T) {
	c := newTestConsumer(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, c.ConsumeJSON(ctx, "test-queue", func(context.Context, events.Envelope) (AckType, error) {
		return Ack, nil
	}, nil))
}

func TestConsumeJSONReturnsClosedWhenClientIsClosed(t *testing.T) {
	client := newTestClient(t)
	client.shutdown()

	c, err := NewConsumer(client, testLogger(), nil)
	require.NoError(t, err)

	err = c.ConsumeJSON(context.Background(), "test-queue", func(context.Context, events.Envelope) (AckType, error) {
		return Ack, nil
	}, nil)
	require.ErrorIs(t, err, ErrClosed)
}
