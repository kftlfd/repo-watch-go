package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestClient creates a client without connecting. Connect installs the
// internal context in production, so tests that call shutdown must do the same.
func newTestClient(t *testing.T) *client {
	t.Helper()

	c, err := newClient(ClientConfig{
		URL:    "amqp://example",
		Logger: testLogger(),
	})
	require.NoError(t, err)

	c.ctx, c.cancel = context.WithCancel(context.Background())

	return c
}

func TestNewClientValidatesConfig(t *testing.T) {
	_, err := newClient(ClientConfig{})
	require.ErrorContains(t, err, "missing rabbitmq url")

	_, err = newClient(ClientConfig{URL: "amqp://example"})
	require.ErrorContains(t, err, "missing rabbitmq logger")

	c, err := newClient(ClientConfig{
		URL:    "amqp://example",
		Logger: testLogger(),
	})
	require.NoError(t, err)
	require.Equal(t, defaultReconnectBase, c.cfg.ReconnectBase)
	require.Equal(t, defaultReconnectMax, c.cfg.ReconnectMax)
	require.Equal(t, defaultConnectTimeout, c.cfg.ConnectTimeout)
}

func TestClientNotReadyWithoutConnection(t *testing.T) {
	c := newTestClient(t)
	require.ErrorIs(t, c.Ready(), ErrUnavailable)

	_, err := c.acquire()
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestClientReadyAfterClose(t *testing.T) {
	c := newTestClient(t)
	c.shutdown()

	require.ErrorIs(t, c.Ready(), ErrClosed)
}

func TestConnectFailsFastWhenBrokerIsUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	_, err = Connect(ClientConfig{
		URL:            fmt.Sprintf("amqp://guest:guest@%s", addr),
		Logger:         testLogger(),
		ConnectTimeout: 500 * time.Millisecond,
	})
	require.Error(t, err)
}

func TestNextReconnectBackoff(t *testing.T) {
	c := newTestClient(t)

	require.Equal(t, 2*time.Second, c.nextReconnectBackoff(time.Second, 30*time.Second))
	require.Equal(t, 30*time.Second, c.nextReconnectBackoff(20*time.Second, 30*time.Second))
	require.Equal(t, 30*time.Second, c.nextReconnectBackoff(30*time.Second, 30*time.Second))
	require.Equal(t, defaultReconnectBase, c.nextReconnectBackoff(0, 30*time.Second))
}

func TestClientSetConnectionRejectsAfterShutdown(t *testing.T) {
	c := newTestClient(t)
	c.shutdown()

	require.ErrorIs(t, c.setConnection(nil), ErrClosed)
}

func TestClientWaitConnReturnsClosedAfterShutdown(t *testing.T) {
	c := newTestClient(t)
	c.shutdown()

	_, err := c.waitConn(context.Background())
	require.ErrorIs(t, err, ErrClosed)
}
