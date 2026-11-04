package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
	handled     []AckType
	failures    int
	defects     int
	requeues    int
	returns     []string
	dlqDepth    int
	dlqDepthSet int
}

func (m *fakeConsumerMetrics) MessageHandled(_ string, ack AckType) {
	m.handled = append(m.handled, ack)
}

func (m *fakeConsumerMetrics) HandlerFailed(string)         { m.failures++ }
func (m *fakeConsumerMetrics) HandlerDefectDetected(string) { m.defects++ }
func (m *fakeConsumerMetrics) Requeued(string)              { m.requeues++ }
func (m *fakeConsumerMetrics) MessageReturned(routingKey string) {
	m.returns = append(m.returns, routingKey)
}
func (m *fakeConsumerMetrics) SetDLQDepth(depth int) {
	m.dlqDepth = depth
	m.dlqDepthSet++
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

func newTestConsumer(t *testing.T, metrics MetricsRecorder) *consumer {
	t.Helper()

	c, err := NewConsumer(newTestClient(t), testLogger(), metrics)
	require.NoError(t, err)
	return c
}

func TestHandleDeliveryDiscardsInvalidEnvelope(t *testing.T) {
	called := false
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	c := newTestConsumer(t, metrics)

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":`),
		"test-queue",
		func(EnvelopeJSON) (AckType, error) {
			called = true
			return Ack, nil
		},
	)

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
	var got EnvelopeJSON
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	c := newTestConsumer(t, metrics)

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
		"test-queue",
		func(msg EnvelopeJSON) (AckType, error) {
			got = msg
			return NackRequeue, wantErr
		},
	)

	require.NoError(t, err)
	require.Equal(t, "event-1", got.EventID)
	require.Equal(t, EventRepositoryChanged, got.EventType)
	require.True(t, acknowledger.nackCalled)
	require.True(t, acknowledger.nackRequeue)
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Equal(t, []AckType{NackRequeue}, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Zero(t, metrics.defects)
	require.Equal(t, 1, metrics.requeues)
}

func TestHandleDeliveryAcksSuccessfulHandler(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	c := newTestConsumer(t, metrics)

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
		"test-queue",
		func(EnvelopeJSON) (AckType, error) {
			return Ack, nil
		},
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
	c := newTestConsumer(t, &fakeConsumerMetrics{})

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
		"test-queue",
		func(EnvelopeJSON) (AckType, error) {
			return Ack, nil
		},
	)

	require.ErrorIs(t, err, ackErr)
}

func TestHandleDeliveryRequeuesAckWithErrorAsDefect(t *testing.T) {
	handlerErr := errors.New("post-ack failure")
	acknowledger := &fakeAcknowledger{}
	metrics := &fakeConsumerMetrics{}
	c := newTestConsumer(t, metrics)

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
		"test-queue",
		func(EnvelopeJSON) (AckType, error) {
			return Ack, handlerErr
		},
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
	c := newTestConsumer(t, metrics)

	err := c.handleDelivery(
		testDelivery(acknowledger, `{"id":"event-1","type":"repository.changed","payload":{}}`),
		"test-queue",
		func(EnvelopeJSON) (AckType, error) {
			return AckType("not-a-valid-ack"), nil
		},
	)

	require.ErrorIs(t, err, invalidAck)
	require.True(t, isFatalConsumeError(err))
	require.False(t, acknowledger.ackCalled)
	require.False(t, acknowledger.nackCalled)
	require.False(t, acknowledger.rejectCalled)
	require.Empty(t, metrics.handled)
	require.Equal(t, 1, metrics.failures)
	require.Equal(t, 1, metrics.defects)
	require.Zero(t, metrics.requeues)
}

func TestIsFatalConsumeError(t *testing.T) {
	require.True(t, isFatalConsumeError(invalidAck))
	require.True(t, isFatalConsumeError(fmt.Errorf("wrapped: %w", invalidAck)))

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

	require.NoError(t, c.ConsumeJSON(ctx, "test-queue", func(EnvelopeJSON) (AckType, error) {
		return Ack, nil
	}))
}

func TestConsumeJSONReturnsClosedWhenClientIsClosed(t *testing.T) {
	client := newTestClient(t)
	client.shutdown()

	c, err := NewConsumer(client, testLogger(), nil)
	require.NoError(t, err)

	err = c.ConsumeJSON(context.Background(), "test-queue", func(EnvelopeJSON) (AckType, error) {
		return Ack, nil
	})
	require.ErrorIs(t, err, ErrClosed)
}
