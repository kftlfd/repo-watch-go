package taskrunner

import "github.com/prometheus/client_golang/prometheus"

type MetricsRecorder interface {
	TaskPanicked(task string)
}

type Metrics struct {
	panics *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		panics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskrunner_panics_total",
			Help: "Total background tasks that panicked, by task.",
		}, []string{"task"}),
	}

	reg.MustRegister(m.panics)

	return m
}

func (m *Metrics) TaskPanicked(task string) {
	m.panics.WithLabelValues(task).Inc()
}

type noopMetrics struct{}

func (noopMetrics) TaskPanicked(string) {}
