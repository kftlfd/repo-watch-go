//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/events"
	"repo-watch/tests/containers"
)

func TestConsumerSchedulesDelayedRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	// Use short tiers so the test does not have to wait for the real 30s
	// schedule. The two different delays also verify that x-retry-count
	// survives dead-lettering and selects the next tier. Topology is declared
	// after this override.
	originalTiers := retryTiers
	retryTiers = []retryTier{
		{Name: "100ms", Delay: 100 * time.Millisecond},
		{Name: "750ms", Delay: 750 * time.Millisecond},
	}
	t.Cleanup(func() { retryTiers = originalTiers })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, amqpURL := containers.NewRabbitMQ(t)

	client, err := Connect(ClientConfig{
		URL:    amqpURL,
		Logger: testLogger(),
	})
	require.NoError(t, err)
	defer client.Close()

	publisher, err := NewPublisher(client, testLogger(), nil)
	require.NoError(t, err)

	consumer, err := NewConsumer(client, testLogger(), nil, WithRetryPublisher(publisher))
	require.NoError(t, err)

	var calls atomic.Int32
	retried := make(chan struct{}, 1)

	go func() {
		_ = consumer.ConsumeJSON(ctx, QueueNotificationInbox, func(context.Context, events.Envelope) (AckType, error) {
			if calls.Add(1) <= 2 {
				return NackRequeue, errors.New("transient failure")
			}
			select {
			case retried <- struct{}{}:
			default:
			}
			return Ack, nil
		}, nil)
	}()

	start := time.Now()
	publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer publishCancel()
	require.NoError(t, publisher.PublishJSON(publishCtx, events.Envelope{
		EventID:   "retry-event",
		EventType: events.EventReleaseDetected,
		Version:   events.VersionReleaseDetected,
		Payload: []byte(
			`{"repository_id":"owner/repo","owner":"owner","name":"repo","tag":"v1"}`,
		),
	}))

	select {
	case <-retried:
	case <-time.After(10 * time.Second):
		t.Fatal("event was not retried")
	}

	require.Equal(t, int32(3), calls.Load())
	// 100ms first tier plus 750ms second tier. If x-retry-count were lost,
	// the second retry would use the 100ms tier again.
	require.GreaterOrEqual(t, time.Since(start), 700*time.Millisecond)

	// If the retry queue disappears, the publish must fail so the consumer
	// keeps the original delivery instead of acking it and losing the event.
	missingQueue := retryQueueName(QueueMailerInbox, retryTiers[0])
	conn, err := client.acquire()
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDelete(missingQueue, false, false, false)
	require.NoError(t, err)
	require.NoError(t, ch.Close())

	err = publisher.PublishRetry(ctx, missingQueue, amqp091.Delivery{
		MessageId: "retry-missing-queue",
		Body:      []byte(`{}`),
	}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned")
}
