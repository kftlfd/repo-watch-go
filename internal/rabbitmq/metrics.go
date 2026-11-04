package rabbitmq

import "github.com/prometheus/client_golang/prometheus"

type MetricsRecorder interface {
	MessageHandled(queue string, ack AckType)
	HandlerFailed(queue string)
	HandlerDefectDetected(queue string)
	Requeued(queue string)
	MessageReturned(routingKey string)
	SetDLQDepth(depth int)
	SetConnected(connected bool)
	ConnectionLost()
}

type metrics struct {
	messages       *prometheus.CounterVec
	failures       *prometheus.CounterVec
	defects        *prometheus.CounterVec
	requeues       *prometheus.CounterVec
	publishReturns *prometheus.CounterVec
	dlqDepth       prometheus.Gauge
	connected      prometheus.Gauge
	connectionLost prometheus.Counter
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
		publishReturns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rabbitmq_publisher_returns_total",
				Help: "Total messages returned by RabbitMQ because they could not be routed, by routing key.",
			},
			[]string{"routing_key"},
		),
		dlqDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "rabbitmq_dlq_depth",
			Help: "Current number of messages in the dead-letter queue.",
		}),
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
		m.publishReturns,
		m.dlqDepth,
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

func (m *metrics) MessageReturned(routingKey string) {
	m.publishReturns.WithLabelValues(routingKey).Inc()
}

func (m *metrics) SetDLQDepth(depth int) {
	m.dlqDepth.Set(float64(depth))
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
func (noopMetrics) MessageReturned(string)         {}
func (noopMetrics) SetDLQDepth(int)                {}
func (noopMetrics) SetConnected(bool)              {}
func (noopMetrics) ConnectionLost()                {}
