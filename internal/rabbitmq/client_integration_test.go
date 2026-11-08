package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func startRabbitMQ(t *testing.T, ctx context.Context) (*rabbitmq.RabbitMQContainer, string) {
	t.Helper()

	// Restarting a container with an auto-assigned host port can change the
	// published port. Production URLs are stable, so pin the AMQP port here to
	// keep exercising the client rather than the test harness.
	hostPort := freeTCPPort(t)
	container, err := rabbitmq.Run(ctx, "rabbitmq:3.13-management",
		testcontainers.WithHostConfigModifier(func(hc *dockercontainer.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("5672/tcp"): []network.PortBinding{{
					HostIP:   netip.MustParseAddr("127.0.0.1"),
					HostPort: hostPort,
				}},
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	return container, fmt.Sprintf("amqp://guest:guest@127.0.0.1:%s", hostPort)
}

func TestClientRecoversAfterRabbitMQRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	container, amqpURL := startRabbitMQ(t, ctx)

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

	received := make(chan EnvelopeJSON, 10)
	consumer, err := NewConsumer(client, testLogger(), nil)
	require.NoError(t, err)
	go func() {
		_ = consumer.ConsumeJSON(ctx, QueueMailerInbox, func(msg EnvelopeJSON) (AckType, error) {
			received <- msg
			return Ack, nil
		})
	}()

	publish := func(id string) {
		require.Eventually(t, func() bool {
			publishCtx, publishCancel := context.WithTimeout(ctx, 2*time.Second)
			defer publishCancel()

			return publisher.PublishJSON(publishCtx, EnvelopeJSON{
				EventID:   id,
				EventType: EventEmailQueued,
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

func freeTCPPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	return port
}
