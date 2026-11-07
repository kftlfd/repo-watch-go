package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"repo-watch/internal/utils"
)

type Client interface {
	acquire() (*amqp091.Connection, error)
	waitConn(ctx context.Context) (*amqp091.Connection, error)
}

var _ Client = (*client)(nil)

type ClientConfig struct {
	URL     string
	Logger  *slog.Logger
	Metrics MetricsRecorder

	ConnectTimeout time.Duration
	ReconnectBase  time.Duration
	ReconnectMax   time.Duration
}

// client owns the RabbitMQ connection. Connect dials synchronously and
// declares the topology before returning, then keeps reconnecting in the
// background until Close is called.
type client struct {
	cfg    ClientConfig
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	conn        *amqp091.Connection
	connCloseCh chan *amqp091.Error

	mu           sync.RWMutex
	changed      chan struct{}
	done         chan struct{}
	shutdownOnce sync.Once
}

func newClient(cfg ClientConfig) (*client, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("missing rabbitmq url")
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("missing rabbitmq logger")
	}
	if cfg.Metrics == nil {
		cfg.Metrics = noopMetrics{}
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.ReconnectBase <= 0 {
		cfg.ReconnectBase = defaultReconnectBase
	}
	if cfg.ReconnectMax < cfg.ReconnectBase {
		cfg.ReconnectMax = max(defaultReconnectMax, cfg.ReconnectBase)
	}

	return &client{
		cfg:     cfg,
		logger:  cfg.Logger.With("module", "rabbitmq"),
		changed: make(chan struct{}),
	}, nil
}

// Connect establishes the initial connection and declares the topology
// synchronously. The client lives until Close is called.
func Connect(cfg ClientConfig) (*client, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	err = c.connect()
	if err != nil {
		c.shutdown()
		return nil, err
	}
	c.logger.Info("connection established")

	c.mu.Lock()
	c.done = make(chan struct{})
	doneCh := c.done
	c.mu.Unlock()

	go func() {
		defer close(doneCh)
		c.maintain()
	}()

	return c, nil
}

func (c *client) connect() error {
	conn, err := amqp091.DialConfig(c.cfg.URL, amqp091.Config{
		Heartbeat: defaultHeartbeat,
		Dial:      amqp091.DefaultDial(c.cfg.ConnectTimeout),
	})
	if err != nil {
		return err
	}

	if err := c.ensureTopology(conn); err != nil {
		_ = conn.Close()
		return fmt.Errorf("declare topology: %w", err)
	}

	if err := c.setConnection(conn); err != nil {
		_ = conn.Close()
		return err
	}

	return nil
}

func (c *client) setConnection(conn *amqp091.Connection) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx.Err() != nil {
		return ErrClosed
	}

	c.conn = conn
	c.connCloseCh = make(chan *amqp091.Error, 1)
	conn.NotifyClose(c.connCloseCh)

	// notify callers of waitConn
	close(c.changed)
	c.changed = make(chan struct{})

	c.cfg.Metrics.SetConnected(true)
	return nil
}

func (c *client) ensureTopology(conn *amqp091.Connection) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.ConnectTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- EnsureTopology(conn)
	}()

	select {
	case <-ctx.Done():
		// Closing the connection unblocks the declaration goroutine
		_ = conn.Close()
		return ctx.Err()

	case err := <-done:
		return err
	}
}

func (c *client) maintain() {
	for {
		var amqpErr *amqp091.Error

		select {
		case <-c.ctx.Done():
		case amqpErr = <-c.connCloseCh:
		}

		if c.ctx.Err() != nil {
			return
		}

		c.mu.Lock()
		conn := c.conn
		c.conn = nil
		c.mu.Unlock()

		if conn != nil {
			_ = conn.Close()
		}

		c.cfg.Metrics.SetConnected(false)
		c.cfg.Metrics.ConnectionLost()

		if amqpErr == nil {
			c.logger.Error("connection closed unexpectedly")
		} else {
			c.logger.Error("connection lost", "error", amqpErr)
		}

		c.reconnect()
	}
}

func (c *client) reconnect() {
	backoff := c.cfg.ReconnectBase

	for {
		if c.ctx.Err() != nil {
			return
		}

		c.logger.Warn("reconnecting to rabbitmq", "retry_in", backoff)
		if err := utils.SleepCtx(c.ctx, backoff); err != nil {
			return
		}
		backoff = c.nextReconnectBackoff(backoff, c.cfg.ReconnectMax)

		err := c.connect()
		if err == nil {
			c.logger.Warn("connection recovered")
			return
		}

		c.logger.Error("rabbitmq reconnect failed", "error", err)
	}
}

func (c *client) nextReconnectBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		return defaultReconnectBase
	}
	if current >= max {
		return max
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

// Close stops the reconnect loop and closes the connection. It is safe to call
// more than once.
func (c *client) Close() error {
	c.shutdown()

	c.mu.RLock()
	doneCh := c.done
	c.mu.RUnlock()

	if doneCh != nil {
		<-doneCh
	}

	return nil
}

func (c *client) shutdown() {
	c.cancel()
	c.shutdownOnce.Do(func() {
		c.mu.Lock()
		conn := c.conn
		c.conn = nil
		c.mu.Unlock()

		if conn != nil {
			_ = conn.Close()
		}

		c.cfg.Metrics.SetConnected(false)
	})
}

// Ready reports whether the client currently holds a live connection with the
// topology already declared.
func (c *client) Ready() error {
	_, err := c.acquire()
	return err
}

// acquire returns the current live connection or ErrUnavailable while the
// client is reconnecting.
func (c *client) acquire() (*amqp091.Connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if c.conn == nil || c.conn.IsClosed() {
		return nil, ErrUnavailable
	}

	return c.conn, nil
}

// waitConn blocks until a live connection is available. It returns ErrClosed
// when the client stops and the caller's context error when ctx ends.
func (c *client) waitConn(ctx context.Context) (*amqp091.Connection, error) {
	for {
		if c.ctx.Err() != nil {
			return nil, ErrClosed
		}

		c.mu.RLock()
		conn := c.conn
		changed := c.changed
		c.mu.RUnlock()

		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, ErrClosed
		case <-changed:
		}
	}
}
