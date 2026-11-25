package taskrunner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeMetrics struct {
	panicked []string
}

func (m *fakeMetrics) TaskPanicked(task string) {
	m.panicked = append(m.panicked, task)
}

func TestRunTasksContinuesAfterPanic(t *testing.T) {
	ran := false
	metrics := &fakeMetrics{}

	err := runTasks(context.Background(), []Task{
		{
			Name: "panicking task",
			Do: func(context.Context) error {
				panic("boom")
			},
		},
		{
			Name: "following task",
			Do: func(context.Context) error {
				ran = true
				return nil
			},
		},
	}, time.Second, metrics)

	require.ErrorContains(t, err, "panicking task: panic: boom")
	require.True(t, ran)
	require.Equal(t, []string{"panicking task"}, metrics.panicked)
}
