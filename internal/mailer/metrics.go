package mailer

import "github.com/prometheus/client_golang/prometheus"

type EmailMetricsRecorder interface {
	Rendered(emailType string)
	Failed(emailType string)
}

type EmailMetrics struct {
	rendered *prometheus.CounterVec
	failed   *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *EmailMetrics {
	m := &EmailMetrics{
		rendered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailer_emails_rendered_total",
			Help: "Total email events rendered successfully, by email type.",
		}, []string{"type"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailer_email_failures_total",
			Help: "Total email events that could not be rendered or delivered, by email type.",
		}, []string{"type"}),
	}

	reg.MustRegister(
		m.rendered,
		m.failed,
	)

	return m
}

func (m *EmailMetrics) Rendered(emailType string) {
	m.rendered.WithLabelValues(emailType).Inc()
}

func (m *EmailMetrics) Failed(emailType string) {
	m.failed.WithLabelValues(emailType).Inc()
}

type noopEmailMetrics struct{}

func (noopEmailMetrics) Rendered(string) {}
func (noopEmailMetrics) Failed(string)   {}
