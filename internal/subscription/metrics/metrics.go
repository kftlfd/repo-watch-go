package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type client struct {
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

func NewClient(reg prometheus.Registerer) *client {
	m := &client{
		httpRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests.",
			},
			[]string{"method", "path", "status"},
		),

		httpDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds",
				Help: "HTTP request duration in seconds.",
			},
			[]string{"method", "path"},
		),
	}

	reg.MustRegister(
		m.httpRequests,
		m.httpDuration,
	)

	return m
}

func (m *client) HTTPRequest(
	method string,
	path string,
	status int,
	duration time.Duration,
) {
	statusCode := strconv.Itoa(status)

	m.httpRequests.
		WithLabelValues(method, path, statusCode).
		Inc()

	m.httpDuration.
		WithLabelValues(method, path).
		Observe(duration.Seconds())
}
