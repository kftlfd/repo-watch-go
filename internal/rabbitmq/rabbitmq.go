package rabbitmq

import (
	"errors"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	defaultReconnectBase  = 500 * time.Millisecond
	defaultReconnectMax   = 30 * time.Second
	defaultConnectTimeout = 10 * time.Second
	defaultHeartbeat      = 10 * time.Second
)

var (
	// ErrUnavailable means the client does not currently hold a live
	// connection, but the client keeps reconnecting in the background
	ErrUnavailable = errors.New("rabbitmq temporarily unavailable")

	// ErrClosed means the client has stopped and will not reconnect
	ErrClosed = errors.New("rabbitmq client is closed")

	// ErrInvalid classifies a message that cannot be published at all, for
	// example because it cannot be serialized.
	ErrInvalid = errors.New("rabbitmq publish invalid")
)

// closeChannel closes ch defensively. amqp091-go can panic when a channel is
// closed while its connection is shutting down, which is expected during
// RabbitMQ restarts and must not crash the service.
func closeChannel(ch *amqp091.Channel) {
	if ch == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	if ch.IsClosed() {
		return
	}

	_ = ch.Close()
}
