package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"repo-watch/internal/admin"
	"repo-watch/internal/github"
	"repo-watch/internal/httpserver"
	"repo-watch/internal/logger"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/api"
	"repo-watch/internal/subscription/database"
	"repo-watch/internal/subscription/database/migrations"
	"repo-watch/internal/subscription/inbox"
	"repo-watch/internal/subscription/metrics"
	"repo-watch/internal/subscription/outbox"
	"repo-watch/internal/subscription/subs"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	_ = godotenv.Load()

	logger := logger.NewLogger("service", "subscription")

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

	logger.Info("preparing rabbitmq")
	rabbit, err := rabbitmq.Connect(cfg.RabbitMQUrl)
	if err != nil {
		return fmt.Errorf("RabbitMQ connect: %w", err)
	}
	defer rabbit.Close()
	if err := rabbitmq.EnsureTopology(rabbit); err != nil {
		return fmt.Errorf("ensure rabbit topology: %w", err)
	}

	logger.Info("initiating components")

	githubClient := github.NewClient(github.WithBaseURL(cfg.GhApiUrl))

	queries := database.New(db)

	subsSvc := subs.NewService(subs.Deps{
		DB:           db,
		GithubClient: githubClient,
		Queries:      queries,
	})

	pub, err := rabbitmq.NewPublisher(rabbit)
	if err != nil {
		return fmt.Errorf("rabbit publisher: %w", err)
	}
	defer pub.Close()

	outboxDispatcher, err := outbox.NewDispatcher(outbox.Deps{
		Cfg:     cfg.OutboxCfg,
		Logger:  logger,
		Pub:     pub,
		Queries: queries,
	})
	if err != nil {
		return fmt.Errorf("outbox dispatcher: %w", err)
	}

	inboxWorker, err := inbox.NewInboxWorker(inbox.Deps{
		Logger:  logger,
		DB:      db,
		Rabbit:  rabbit,
		Queries: queries,
	})

	metricsReg := prometheus.NewRegistry()
	metricsClient := metrics.NewClient(metricsReg)

	if !cfg.IsDev {
		gin.SetMode(gin.ReleaseMode)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		srv := httpserver.NewServer(httpserver.ServerDeps{
			Logger:  logger.With("module", "admin_server"),
			Handler: admin.NewHandler(metricsReg),
			Port:    cfg.PortAdmin,
		})
		srv.Start(ctx, cancel)
	})

	wg.Go(func() {
		for {
			select {
			case ret := <-pub.Returns():
				logger.Error("rabbitmq message returned", "msg", ret)
			case <-ctx.Done():
				return
			}
		}
	})

	wg.Go(func() {
		outboxDispatcher.Start(ctx, cancel)
	})

	wg.Go(func() {
		inboxWorker.Start(ctx)
	})

	wg.Go(func() {
		apiLogger := logger.With("module", "api_server")
		apiHandler := api.NewHandler(api.HandlerDeps{
			Logger:      apiLogger,
			SubsService: subsSvc,
			Metrics:     metricsClient,
		})
		apiServer := httpserver.NewServer(httpserver.ServerDeps{
			Logger:  apiLogger,
			Handler: apiHandler,
			Port:    cfg.PortAPI,
		})
		apiServer.Start(ctx, cancel)
	})

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
