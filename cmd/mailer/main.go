package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"repo-watch/internal/logger"
	"repo-watch/internal/mailer"
	"repo-watch/internal/rabbitmq"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	_ = godotenv.Load()

	logger := logger.NewLogger("service", "mailer")

	err := run(logger)

	if err != nil {
		logger.Error("exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	logger.Info("starting up")

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	signalCtx, signalStop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer signalStop()

	go func() {
		<-signalCtx.Done()
		cancel(context.Canceled)
		signalStop()
	}()

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	metricsReg := prometheus.NewRegistry()
	rabbitMetrics := rabbitmq.NewMetrics(metricsReg)

	logger.Info("connecting to rabbitmq")
	rabbit, err := rabbitmq.Connect(rabbitmq.ClientConfig{
		URL:     cfg.RabbitMQUrl,
		Logger:  logger,
		Metrics: rabbitMetrics,
	})
	if err != nil {
		return fmt.Errorf("connect rabbitmq: %w", err)
	}
	defer rabbit.Close()

	rabbitConsumer, err := rabbitmq.NewConsumer(rabbit, logger, rabbitMetrics)
	if err != nil {
		return fmt.Errorf("create rabbit consumer: %w", err)
	}

	logger.Info("initiating components")

	mailerWorker, err := mailer.NewMailer(mailer.Deps{
		Logger:         logger,
		RabbitConsumer: rabbitConsumer,
	})
	if err != nil {
		return fmt.Errorf("create mailer worker: %w", err)
	}

	var wg sync.WaitGroup

	runWorker := func(name string, fn func(context.Context) error) {
		wg.Go(func() {
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				logger.Error("worker failed", "worker", name, "error", err)
				cancel(fmt.Errorf("%s: %w", name, err))
			}
		})
	}

	runWorker("mailer", mailerWorker.Start)

	logger.Info("all components initiated")

	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	<-ctx.Done()

	logger.Info("shutting down")

	select {
	case <-time.After(10 * time.Second):
		return fmt.Errorf("shutdown timeout")

	case <-allDone:
		logger.Info("all components stopped")
	}

	if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
		return fmt.Errorf("ctx cause: %w", cause)
	}

	return nil
}
