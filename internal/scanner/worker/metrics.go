package worker

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics interface {
	ScanOk()
	ScanErr()
}

type client struct {
	repoScans *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *client {
	m := &client{
		repoScans: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "repo_scans",
				Help: "Total number of repo scans.",
			},
			[]string{"status"},
		),
	}

	reg.MustRegister(
		m.repoScans,
	)

	return m
}

func (m *client) ScanOk() {
	m.repoScans.WithLabelValues("ok").Inc()
}

func (m *client) ScanErr() {
	m.repoScans.WithLabelValues("err").Inc()
}
