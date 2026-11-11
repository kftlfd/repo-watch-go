//go:build integration

package rabbitmq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/events"
	"repo-watch/tests/containers"
)

func TestClientRecoversAfterRabbitMQRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	container, amqpURL := containers.NewRabbitMQ(t)

	client, err := Connect(ClientConfig{
		URL:           amqpURL,
		Logger:        testLogger(),
		ReconnectBase: 100 * time.Millisecond,
		ReconnectMax:  time.Second,
	})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Ready())

	// The client must have declared the topology before reporting ready.
	conn, err := client.acquire()
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclarePassive(QueueMailerInbox, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Close())

	publisher, err := NewPublisher(client, testLogger(), nil)
	require.NoError(t, err)

	received := make(chan events.Envelope, 10)
	consumer, err := NewConsumer(client, testLogger(), nil)
	require.NoError(t, err)
	go func() {
		_ = consumer.ConsumeJSON(ctx, QueueMailerInbox, func(msg events.Envelope) (AckType, error) {
			received <- msg
			return Ack, nil
		})
	}()

	publish := func(id string) {
		require.Eventually(t, func() bool {
			publishCtx, publishCancel := context.WithTimeout(ctx, 2*time.Second)
			defer publishCancel()

			return publisher.PublishJSON(publishCtx, events.Envelope{
				EventID:   id,
				EventType: events.EventEmailQueued,
				Payload:   json.RawMessage(`{}`),
			}) == nil
		}, 30*time.Second, 100*time.Millisecond)
	}

	awaitMessage := func(id string) {
		require.Eventually(t, func() bool {
			select {
			case msg := <-received:
				return msg.EventID == id
			case <-time.After(50 * time.Millisecond):
				return false
			}
		}, 30*time.Second, 50*time.Millisecond)
	}

	publish("before-restart")
	awaitMessage("before-restart")

	require.NoError(t, container.Stop(ctx, nil))
	require.Eventually(t, func() bool {
		return client.Ready() != nil
	}, 30*time.Second, 100*time.Millisecond, "client did not notice the connection loss")

	require.NoError(t, container.Start(ctx))
	require.Eventually(t, func() bool {
		return client.Ready() == nil
	}, 60*time.Second, 100*time.Millisecond, "client did not reconnect")

	publish("after-restart")
	awaitMessage("after-restart")
}
