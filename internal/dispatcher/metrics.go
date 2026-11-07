package dispatcher

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type MetricsRecorder interface {
	Published(count int)
	Retry()
	DeadLettered()
	Unavailable()
	SetPending(count int64)
	SetOldestAge(age time.Duration)
}

type Metrics struct {
	published    prometheus.Counter
	retries      prometheus.Counter
	deadLettered prometheus.Counter
	unavailable  prometheus.Counter
	pending      prometheus.Gauge
	oldestAge    prometheus.Gauge
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
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events",
			Help: "Current number of unpublished, non-failed outbox rows.",
		}),
		oldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest unpublished, non-failed outbox row; 0 when empty.",
		}),
	}

	reg.MustRegister(
		m.published,
		m.retries,
		m.deadLettered,
		m.unavailable,
		m.pending,
		m.oldestAge,
	)

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

func (m *Metrics) SetPending(count int64) {
	m.pending.Set(float64(count))
}

func (m *Metrics) SetOldestAge(age time.Duration) {
	age = max(age, 0)
	m.oldestAge.Set(age.Seconds())
}

type noopMetrics struct{}

func (noopMetrics) Published(int)              {}
func (noopMetrics) Retry()                     {}
func (noopMetrics) DeadLettered()              {}
func (noopMetrics) Unavailable()               {}
func (noopMetrics) SetPending(int64)           {}
func (noopMetrics) SetOldestAge(time.Duration) {}
