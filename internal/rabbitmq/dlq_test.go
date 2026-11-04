package rabbitmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDLQMonitorValidatesDependencies(t *testing.T) {
	_, err := NewDLQMonitor(DLQMonitorDeps{Logger: testLogger()})
	require.ErrorContains(t, err, "missing DLQ monitor client")

	_, err = NewDLQMonitor(DLQMonitorDeps{Client: newTestClient(t)})
	require.ErrorContains(t, err, "missing DLQ monitor logger")

	m, err := NewDLQMonitor(DLQMonitorDeps{
		Client:  newTestClient(t),
		Logger:  testLogger(),
		Metrics: &fakeConsumerMetrics{},
	})
	require.NoError(t, err)
	require.Equal(t, defaultDLQMonitorInterval, m.interval)
	require.Equal(t, defaultDLQCheckTimeout, m.checkTimeout)
}

func TestDLQMonitorSkipsInspectionWhenClientIsUnavailable(t *testing.T) {
	metrics := &fakeConsumerMetrics{}
	m, err := NewDLQMonitor(DLQMonitorDeps{
		Client:  newTestClient(t),
		Logger:  testLogger(),
		Metrics: metrics,
	})
	require.NoError(t, err)

	fatal, err := m.check(context.Background())
	require.NoError(t, err)
	require.False(t, fatal)
	require.Zero(t, metrics.dlqDepthSet)
}

func TestDLQMonitorCheckReturnsClosedClientAsFatal(t *testing.T) {
	client := newTestClient(t)
	client.shutdown()

	m, err := NewDLQMonitor(DLQMonitorDeps{
		Client: client,
		Logger: testLogger(),
	})
	require.NoError(t, err)

	fatal, err := m.check(context.Background())
	require.ErrorIs(t, err, ErrClosed)
	require.True(t, fatal)
}

func TestDLQMonitorObserveDepthTracksChanges(t *testing.T) {
	metrics := &fakeConsumerMetrics{}
	m, err := NewDLQMonitor(DLQMonitorDeps{
		Client:  newTestClient(t),
		Logger:  testLogger(),
		Metrics: metrics,
	})
	require.NoError(t, err)

	require.Equal(t, -1, m.lastDepth)

	m.observeDepth(0)
	require.Equal(t, 0, metrics.dlqDepth)
	require.Equal(t, 0, m.lastDepth)

	m.observeDepth(3)
	require.Equal(t, 3, metrics.dlqDepth)
	require.Equal(t, 3, m.lastDepth)
}
