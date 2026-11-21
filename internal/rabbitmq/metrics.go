package rabbitmq

import "github.com/prometheus/client_golang/prometheus"

type MetricsRecorder interface {
	MessageHandled(queue string, ack AckType)
	HandlerFailed(queue string)
	HandlerDefectDetected(queue string)
	Requeued(queue string)
	RetryScheduled(queue string, tier string)
	RetryExhausted(queue string)
	RetryPublishFailed(queue string)
	MessageReturned(routingKey string)
	SetConnected(connected bool)
	ConnectionLost()
}

type metrics struct {
	messages             *prometheus.CounterVec
	failures             *prometheus.CounterVec
	defects              *prometheus.CounterVec
	requeues             *prometheus.CounterVec
	retryScheduled       *prometheus.CounterVec
	retryExhausted       *prometheus.CounterVec
	retryPublishFailures *prometheus.CounterVec
	publishReturns       *prometheus.CounterVec
	connected            prometheus.Gauge
	connectionLost       prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		messages: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_messages_total",
				Help: "Total messages handled by consumers, by queue and outcome.",
			},
			[]string{"queue", "status"},
		),
		failures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_failures_total",
				Help: "Total consumer handler failures, by queue.",
			},
			[]string{"queue"},
		),
		defects: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_handler_defects_total",
				Help: "Total consumer handler defects, by queue.",
			},
			[]string{"queue"},
		),
		requeues: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_requeues_total",
				Help: "Total messages requeued by consumers, by queue.",
			},
			[]string{"queue"},
		),
		retryScheduled: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_retries_scheduled_total",
				Help: "Total failed deliveries scheduled into delayed retry queues, by queue and tier.",
			},
			[]string{"queue", "tier"},
		),
		retryExhausted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_retries_exhausted_total",
				Help: "Total messages dead-lettered because the retry limit was exhausted, by queue.",
			},
			[]string{"queue"},
		),
		retryPublishFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_consumer_retry_publish_failures_total",
				Help: "Total failures to publish a delivery to a delayed retry queue, by queue.",
			},
			[]string{"queue"},
		),
		publishReturns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_publisher_returns_total",
				Help: "Total messages returned by RabbitMQ because they could not be routed, by routing key.",
			},
			[]string{"routing_key"},
		),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "rabbitmq_connected",
			Help: "Whether the service currently has a live RabbitMQ connection.",
		}),
		connectionLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rabbitmq_connection_losses_total",
			Help: "Total number of lost RabbitMQ connections.",
		}),
	}

	reg.MustRegister(
		m.messages,
		m.failures,
		m.requeues,
		m.defects,
		m.retryScheduled,
		m.retryExhausted,
		m.retryPublishFailures,
		m.publishReturns,
		m.connected,
		m.connectionLost,
	)

	return m
}

func (m *metrics) MessageHandled(queue string, ack AckType) {
	m.messages.WithLabelValues(queue, string(ack)).Inc()
}

func (m *metrics) HandlerFailed(queue string) {
	m.failures.WithLabelValues(queue).Inc()
}

func (m *metrics) HandlerDefectDetected(queue string) {
	m.defects.WithLabelValues(queue).Inc()
}

func (m *metrics) Requeued(queue string) {
	m.requeues.WithLabelValues(queue).Inc()
}

func (m *metrics) RetryScheduled(queue string, tier string) {
	m.retryScheduled.WithLabelValues(queue, tier).Inc()
}

func (m *metrics) RetryExhausted(queue string) {
	m.retryExhausted.WithLabelValues(queue).Inc()
}

func (m *metrics) RetryPublishFailed(queue string) {
	m.retryPublishFailures.WithLabelValues(queue).Inc()
}

func (m *metrics) MessageReturned(routingKey string) {
	m.publishReturns.WithLabelValues(routingKey).Inc()
}

func (m *metrics) SetConnected(connected bool) {
	if connected {
		m.connected.Set(1)
		return
	}

	m.connected.Set(0)
}

func (m *metrics) ConnectionLost() {
	m.connectionLost.Inc()
}

type noopMetrics struct{}

func (noopMetrics) MessageHandled(string, AckType) {}
func (noopMetrics) HandlerFailed(string)           {}
func (noopMetrics) HandlerDefectDetected(string)   {}
func (noopMetrics) Requeued(string)                {}
func (noopMetrics) RetryScheduled(string, string)  {}
func (noopMetrics) RetryExhausted(string)          {}
func (noopMetrics) RetryPublishFailed(string)      {}
func (noopMetrics) MessageReturned(string)         {}
func (noopMetrics) SetConnected(bool)              {}
func (noopMetrics) ConnectionLost()                {}
