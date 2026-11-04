package rabbitmq

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var _ MetricsRecorder = (*metrics)(nil)

func TestMetricsRecordsRabbitMQEvents(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.MessageHandled("test-queue", Ack)
	m.HandlerFailed("test-queue")
	m.HandlerDefectDetected("test-queue")
	m.Requeued("test-queue")
	m.MessageReturned("repository.changed")
	m.SetDLQDepth(3)
	m.SetConnected(true)
	m.ConnectionLost()

	families, err := reg.Gather()
	require.NoError(t, err)

	values := make(map[string]float64, len(families))
	for _, family := range families {
		require.Len(t, family.Metric, 1)

		switch {
		case family.Metric[0].Counter != nil:
			values[family.GetName()] = family.Metric[0].GetCounter().GetValue()
		case family.Metric[0].Gauge != nil:
			values[family.GetName()] = family.Metric[0].GetGauge().GetValue()
		}
	}

	require.Equal(t, 1.0, values["rabbitmq_consumer_messages_total"])
	require.Equal(t, 1.0, values["rabbitmq_consumer_failures_total"])
	require.Equal(t, 1.0, values["rabbitmq_consumer_handler_defects_total"])
	require.Equal(t, 1.0, values["rabbitmq_consumer_requeues_total"])
	require.Equal(t, 1.0, values["rabbitmq_publisher_returns_total"])
	require.Equal(t, 3.0, values["rabbitmq_dlq_depth"])
	require.Equal(t, 1.0, values["rabbitmq_connected"])
	require.Equal(t, 1.0, values["rabbitmq_connection_losses_total"])
}
