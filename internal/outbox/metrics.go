package outbox

import "github.com/prometheus/client_golang/prometheus"

type MetricsRecorder interface {
	Published(count int)
	Retry()
	DeadLettered()
	Unavailable()
}

type Metrics struct {
	published    prometheus.Counter
	retries      prometheus.Counter
	deadLettered prometheus.Counter
	unavailable  prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_events_published_total",
			Help: "Total number of outbox events published successfully.",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_retries_total",
			Help: "Total number of outbox publish attempts that were retried.",
		}),
		deadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_events_dead_lettered_total",
			Help: "Total number of outbox events abandoned after reaching the attempt limit.",
		}),
		unavailable: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_unavailable_total",
			Help: "Total number of outbox publish attempts skipped because RabbitMQ was unavailable.",
		}),
	}

	reg.MustRegister(m.published, m.retries, m.deadLettered, m.unavailable)

	return m
}

func (m *Metrics) Published(count int) {
	if count > 0 {
		m.published.Add(float64(count))
	}
}

func (m *Metrics) Retry() {
	m.retries.Inc()
}

func (m *Metrics) DeadLettered() {
	m.deadLettered.Inc()
}

func (m *Metrics) Unavailable() {
	m.unavailable.Inc()
}

type noopMetrics struct{}

func (noopMetrics) Published(int) {}
func (noopMetrics) Retry()        {}
func (noopMetrics) DeadLettered() {}
func (noopMetrics) Unavailable()  {}
