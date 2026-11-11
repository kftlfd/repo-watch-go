package containers

import (
	"fmt"
	"net"
	"net/netip"
	"testing"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

const rabbitmqImage = "rabbitmq:3.13-management"

func NewRabbitMQ(t *testing.T) (container *rabbitmq.RabbitMQContainer, amqpDsn string) {
	t.Helper()

	// Pin the AMQP port so it doesn't change when container restarts
	hostPort := freeTCPPort(t)

	container, err := rabbitmq.Run(t.Context(), rabbitmqImage,
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

func freeTCPPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	return port
}
