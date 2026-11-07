package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

const shutdownTimeout = 10 * time.Second

var errSignal = errors.New("termination signal")

func Run(
	logger *slog.Logger,
	launch func(*Group, *slog.Logger) error,
) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	signalCtx, signalStop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer signalStop()

	go func() {
		defer signalStop()
		select {
		case <-ctx.Done():
		case <-signalCtx.Done():
			cancel(errSignal)
		}
	}()

	g := newGroup(ctx, cancel, logger)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer g.wait()
		logger.Info("starting up")
		if err := launch(g, logger); err != nil {
			cancel(err)
		}
	}()

	<-ctx.Done()
	cause := context.Cause(ctx)
	logger.Info("shutdown triggered", "cause", cause)

	select {
	case <-time.After(shutdownTimeout):
		logger.Error("shutdown timeout", "cause", cause, "timeout", shutdownTimeout.String())
		os.Exit(1)

	case <-done:
		if !errors.Is(cause, errSignal) {
			logger.Error("exited with error", "error", cause)
			os.Exit(1)
		}
	}

	logger.Info("shutdown complete")
}

type Group struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	logger   *slog.Logger
	wg       sync.WaitGroup
	cleanups []func()
}

func newGroup(ctx context.Context, cancel context.CancelCauseFunc, logger *slog.Logger) *Group {
	return &Group{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}
}

func (g *Group) Ctx() context.Context {
	return g.ctx
}

func (g *Group) Go(name string, fn func(context.Context) error) {
	g.wg.Go(func() {
		if err := fn(g.ctx); err != nil && g.ctx.Err() == nil {
			g.logger.Error("worker failed", "worker", name, "error", err)
			g.cancel(fmt.Errorf("%s: %w", name, err))
		}
	})
}

func (g *Group) Cleanup(fn func()) {
	if fn != nil {
		g.cleanups = append(g.cleanups, fn)
	}
}

func (g *Group) wait() {
	g.wg.Wait()

	for _, cleanup := range slices.Backward(g.cleanups) {
		cleanup()
	}
}
