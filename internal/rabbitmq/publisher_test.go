package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"repo-watch/internal/events"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestPublisherRejectsInvalidMessages(t *testing.T) {
	pub, err := NewPublisher(newTestClient(t), testLogger(), nil)
	require.NoError(t, err)

	err = pub.PublishJSON(context.Background(), events.Envelope{})
	require.ErrorIs(t, err, ErrInvalid)

	err = pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{`),
	})
	require.ErrorIs(t, err, ErrInvalid)

	err = pub.PublishJSON(context.Background(), events.Envelope{
		EventType: "unknown.event",
		Payload:   json.RawMessage(`{}`),
	})
	require.ErrorIs(t, err, ErrInvalid)

	err = pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Version:   events.VersionEmailQueued + 1,
		Payload:   json.RawMessage(`{}`),
	})
	require.ErrorIs(t, err, ErrInvalid)
}

func TestPublisherReturnsUnavailableWhenClientHasNoConnection(t *testing.T) {
	client := newTestClient(t)
	pub, err := NewPublisher(client, testLogger(), nil)
	require.NoError(t, err)

	err = pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{}`),
	})

	require.ErrorIs(t, err, ErrUnavailable)
}

func TestPublisherReturnsClosedWhenClientIsClosed(t *testing.T) {
	client := newTestClient(t)
	client.shutdown()

	pub, err := NewPublisher(client, testLogger(), nil)
	require.NoError(t, err)

	err = pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{}`),
	})

	require.ErrorIs(t, err, ErrClosed)
}

func TestPublisherReturnsClosedAfterClientIsClosed(t *testing.T) {
	client := newTestClient(t)
	pub, err := NewPublisher(client, testLogger(), nil)
	require.NoError(t, err)

	require.ErrorIs(t, pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{}`),
	}), ErrUnavailable)

	client.shutdown()

	// Once the publisher observes ErrClosed it stays closed and does not ask
	// the client for a connection again.
	require.ErrorIs(t, pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{}`),
	}), ErrClosed)

	require.ErrorIs(t, pub.PublishJSON(context.Background(), events.Envelope{
		EventType: events.EventEmailQueued,
		Payload:   json.RawMessage(`{}`),
	}), ErrClosed)
}

func TestNewPublisherValidatesDependencies(t *testing.T) {
	_, err := NewPublisher(nil, testLogger(), nil)
	require.ErrorContains(t, err, "missing client")

	_, err = NewPublisher(newTestClient(t), nil, nil)
	require.ErrorContains(t, err, "missing logger")

	pub, err := NewPublisher(newTestClient(t), testLogger(), nil)
	require.NoError(t, err)
	require.NotNil(t, pub.metrics)
}

func TestPublisherRecordsReturnedMessage(t *testing.T) {
	metrics := &fakeConsumerMetrics{}
	pub, err := NewPublisher(newTestClient(t), testLogger(), metrics)
	require.NoError(t, err)

	pub.recordPublishError(&ReturnedError{Return: amqp091.Return{RoutingKey: "repository.changed"}})
	require.Equal(t, []string{"repository.changed"}, metrics.returns)

	pub.recordPublishError(errors.New("other publish failure"))
	require.Equal(t, []string{"repository.changed"}, metrics.returns)
}
