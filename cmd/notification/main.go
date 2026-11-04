package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"repo-watch/internal/admin"
	"repo-watch/internal/httpserver"
	"repo-watch/internal/logger"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/notification/database/migrations"
	"repo-watch/internal/notification/inbox"
	"repo-watch/internal/notification/outbox"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	_ = godotenv.Load()

	logger := logger.NewLogger("service", "notification")

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

	logger.Info("preparing DB")
	if err = postgres.EnsureDbExists(ctx, cfg.DbBaseUrl, cfg.DbName); err != nil {
		return fmt.Errorf("ensure DB exists: %w", err)
	}
	db, err := postgres.Connect(cfg.DbUrl)
	if err != nil {
		return fmt.Errorf("DB connect: %w", err)
	}
	defer db.Close()
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("DB ping: %w", err)
	}
	if err := postgres.Migrate(ctx, db, migrations.MigrationsFS); err != nil {
		return fmt.Errorf("DB migrate: %w", err)
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
	rabbitPublisher, err := rabbitmq.NewPublisher(rabbit, logger, rabbitMetrics)
	if err != nil {
		return fmt.Errorf("create rabbit publisher: %w", err)
	}

	logger.Info("initiating components")

	queries := database.New(db)

	inboxWorker, err := inbox.NewInboxWorker(inbox.Deps{
		Logger:         logger,
		RabbitConsumer: rabbitConsumer,
		Queries:        queries,
	})
	if err != nil {
		return fmt.Errorf("create inbox worker: %w", err)
	}

	dlqMonitor, err := rabbitmq.NewDLQMonitor(rabbitmq.DLQMonitorDeps{
		Client:  rabbit,
		Logger:  logger,
		Metrics: rabbitMetrics,
	})
	if err != nil {
		return fmt.Errorf("create dlq monitor: %w", err)
	}

	outboxDispatcher, err := outbox.NewDispatcher(outbox.Deps{
		Cfg:     cfg.OutboxCfg,
		Logger:  logger,
		Pub:     rabbitPublisher,
		Metrics: outbox.NewMetrics(metricsReg),
		Queries: queries,
	})
	if err != nil {
		return fmt.Errorf("outbox dispatcher: %w", err)
	}

	if !cfg.IsDev {
		gin.SetMode(gin.ReleaseMode)
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

	wg.Go(func() {
		srv := httpserver.NewServer(httpserver.ServerDeps{
			Logger: logger.With("module", "server_admin"),
			Handler: admin.NewHandler(admin.Deps{
				Metrics: metricsReg,
				Ready:   rabbit.Ready,
			}),
			Port: cfg.PortAdmin,
		})
		srv.Start(ctx, cancel)
	})

	runWorker("outbox dispatcher", outboxDispatcher.Start)
	runWorker("inbox", inboxWorker.Start)
	runWorker("dlq monitor", dlqMonitor.Start)

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
