package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"repo-watch/internal/utils"
)

type Task struct {
	Name string
	Do   func(ctx context.Context) error
}

func CreateTaskRunner(
	logger *slog.Logger,
	tasks []Task,
	interval time.Duration,
	taskTimeout time.Duration,
	metrics MetricsRecorder,
) func(context.Context) error {
	if metrics == nil {
		metrics = noopMetrics{}
	}

	return func(ctx context.Context) error {
		logger.Info("starting",
			slog.String("interval", interval.String()),
			slog.String("task_timeout", taskTimeout.String()),
		)

		for {
			err := runTasks(ctx, tasks, taskTimeout, metrics)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("iteration failed", "error", err)
			} else {
				logger.Debug("iteration completed")
			}

			if err := utils.SleepCtx(ctx, interval); err != nil {
				return nil
			}
		}
	}
}

func runTasks(
	ctx context.Context,
	tasks []Task,
	taskTimeout time.Duration,
	metrics MetricsRecorder,
) error {
	var errs []error

	for _, task := range tasks {
		if err := runTask(ctx, task, taskTimeout, metrics); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", task.Name, err))
		}
	}

	return errors.Join(errs...)
}

func runTask(
	ctx context.Context,
	task Task,
	timeout time.Duration,
	metrics MetricsRecorder,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TaskPanicked(task.Name)
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return task.Do(ctx)
}
