//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/events"
	"repo-watch/internal/rabbitmq"
	"repo-watch/tests/containers"
)

func TestReplayRoutesByEnvelopeType(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	_, url := containers.NewRabbitMQ(t)
	conn, ch := newTestChannel(t, url)

	require.NoError(t, ch.ExchangeDeclare(
		"repo-watch.topic", amqp091.ExchangeTopic, true, false, false, false, nil,
	))
	_, err := ch.QueueDeclare(
		"test.sink", true, false, false, false, nil,
	)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(
		"test.sink", events.EventReleaseDetected, "repo-watch.topic", false, nil,
	))
	_, err = ch.QueueDeclare(
		"test.dlq", true, false, false, false,
		amqp091.Table{"x-queue-type": amqp091.QueueTypeQuorum},
	)
	require.NoError(t, err)

	envelope := events.Envelope{
		EventID:   "event-release",
		EventType: events.EventReleaseDetected,
		Version:   events.VersionReleaseDetected,
		CreatedAt: time.Now(),
		Payload:   json.RawMessage(`{"repository_id":"owner/repo","tag":"v1"}`),
	}
	publishToQueue(t, ch, "test.dlq", envelope)

	require.NoError(t, run(config{
		url:             url,
		queue:           "test.dlq",
		exchange:        "repo-watch.topic",
		quarantineQueue: rabbitmq.QueueDLQQuarantine,
		limit:           10,
		apply:           true,
		yes:             true,
		maxReplays:      defaultMaxReplays,
		timeout:         5 * time.Second,
	}))

	replayed, ok, err := ch.Get("test.sink", true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "event-release", replayed.MessageId)

	var got events.Envelope
	require.NoError(t, json.Unmarshal(replayed.Body, &got))
	require.Equal(t, events.EventReleaseDetected, got.EventType)

	_, ok, err = ch.Get("test.dlq", true)
	require.NoError(t, err)
	require.False(t, ok, "the replayed message must be removed from the DLQ")

	require.NoError(t, conn.Close())
}

func TestReplayQuarantinesUnsafeEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	_, url := containers.NewRabbitMQ(t)
	conn, ch := newTestChannel(t, url)

	_, err := ch.QueueDeclare(
		"test.dlq", true, false, false, false,
		amqp091.Table{"x-queue-type": amqp091.QueueTypeQuorum},
	)
	require.NoError(t, err)

	envelope := events.Envelope{
		EventID:   "event-email",
		EventType: events.EventEmailQueued,
		Version:   events.VersionEmailQueued,
		CreatedAt: time.Now(),
		Payload:   json.RawMessage(`{"recipient":"alice@example.com","type":"subscribed","data":{}}`),
	}
	publishToQueue(t, ch, "test.dlq", envelope)

	require.NoError(t, run(config{
		url:             url,
		queue:           "test.dlq",
		exchange:        "repo-watch.topic",
		quarantineQueue: "test.quarantine",
		limit:           10,
		apply:           true,
		yes:             true,
		maxReplays:      defaultMaxReplays,
		timeout:         5 * time.Second,
	}))

	quarantined, ok, err := ch.Get("test.quarantine", true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "event-email", quarantined.MessageId)
	require.Equal(t, "unsafe-event-type", quarantined.Headers["x-quarantine-reason"])

	_, ok, err = ch.Get("test.dlq", true)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, conn.Close())
}

func TestInspectRequeuesAllMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	_, url := containers.NewRabbitMQ(t)
	conn, ch := newTestChannel(t, url)

	_, err := ch.QueueDeclare(
		"test.dlq", true, false, false, false,
		amqp091.Table{"x-queue-type": amqp091.QueueTypeQuorum},
	)
	require.NoError(t, err)

	for _, id := range []string{"event-1", "event-2"} {
		publishToQueue(t, ch, "test.dlq", events.Envelope{
			EventID:   id,
			EventType: events.EventReleaseDetected,
			Version:   events.VersionReleaseDetected,
			CreatedAt: time.Now(),
			Payload:   json.RawMessage(`{"repository_id":"owner/repo","tag":"v1"}`),
		})
	}

	require.NoError(t, run(config{
		url:             url,
		queue:           "test.dlq",
		exchange:        "repo-watch.topic",
		quarantineQueue: rabbitmq.QueueDLQQuarantine,
		limit:           10,
		maxReplays:      defaultMaxReplays,
		timeout:         5 * time.Second,
	}))

	found := 0
	for {
		_, ok, err := ch.Get("test.dlq", true)
		require.NoError(t, err)
		if !ok {
			break
		}
		found++
	}
	require.Equal(t, 2, found, "inspection must requeue every message")

	require.NoError(t, conn.Close())
}

func newTestChannel(t *testing.T, url string) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()

	conn, err := amqp091.Dial(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	return conn, ch
}

func publishToQueue(t *testing.T, ch *amqp091.Channel, queue string, envelope events.Envelope) {
	t.Helper()

	body, err := json.Marshal(envelope)
	require.NoError(t, err)

	require.NoError(t, ch.Confirm(false))
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		context.Background(),
		"",
		queue,
		true,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    envelope.EventID,
			Body:         body,
		},
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := confirmation.WaitContext(ctx)
	require.NoError(t, err)
	require.True(t, ack)
}
