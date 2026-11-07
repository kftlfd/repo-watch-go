package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"repo-watch/internal/admin"
	"repo-watch/internal/app"
	"repo-watch/internal/dispatcher"
	"repo-watch/internal/httpserver"
	"repo-watch/internal/logger"
	"repo-watch/internal/notification/cleanup"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/notification/database/migrations"
	"repo-watch/internal/notification/inbox"
	"repo-watch/internal/notification/outbox"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/taskrunner"
)

func main() {
	_ = godotenv.Load()

	logger := logger.NewLogger("service", "notification")

	app.Run(logger, run)
}

func run(g *app.Group, logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if !cfg.IsDev {
		gin.SetMode(gin.ReleaseMode)
	}

	metricsReg := prometheus.NewRegistry()
	rabbitMetrics := rabbitmq.NewMetrics(metricsReg)

	logger.Info("preparing DB")

	if err = postgres.EnsureDbExists(g.Ctx(), cfg.DbBaseUrl, cfg.DbName); err != nil {
		return fmt.Errorf("ensure DB exists: %w", err)
	}
	db, err := postgres.Connect(cfg.DbUrl)
	if err != nil {
		return fmt.Errorf("DB connect: %w", err)
	}
	g.Cleanup(func() { _ = db.Close() })
	if err = db.PingContext(g.Ctx()); err != nil {
		return fmt.Errorf("DB ping: %w", err)
	}
	if err := postgres.Migrate(g.Ctx(), db, migrations.MigrationsFS); err != nil {
		return fmt.Errorf("DB migrate: %w", err)
	}

	logger.Info("preparing rabbitmq")

	rabbit, err := rabbitmq.Connect(rabbitmq.ClientConfig{
		URL:     cfg.RabbitMQUrl,
		Logger:  logger,
		Metrics: rabbitMetrics,
	})
	if err != nil {
		return fmt.Errorf("connect rabbitmq: %w", err)
	}
	g.Cleanup(func() { _ = rabbit.Close() })

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

	cleanupJob := taskrunner.CreateTaskRunner(
		logger.With("module", "cleanup"),
		cleanup.CreateCleanupTasks(queries, time.Now),
		time.Hour,
		time.Minute,
	)

	dlqMonitor, err := rabbitmq.NewDLQMonitor(rabbitmq.DLQMonitorDeps{
		Client:  rabbit,
		Logger:  logger,
		Metrics: rabbitMetrics,
	})
	if err != nil {
		return fmt.Errorf("create dlq monitor: %w", err)
	}

	outboxDispatcher, err := dispatcher.NewDispatcher(dispatcher.DispatcherDeps{
		Logger:    logger.With("module", "notification_outbox"),
		Store:     outbox.NewScannerOutbox(queries),
		Publisher: rabbitPublisher,
		Metrics:   dispatcher.NewMetrics(metricsReg),
	})
	if err != nil {
		return fmt.Errorf("outbox dispatcher: %w", err)
	}

	adminServer := httpserver.NewServer(httpserver.ServerDeps{
		Logger: logger.With("module", "server_admin"),
		Handler: admin.NewHandler(admin.Deps{
			Metrics: metricsReg,
			Ready:   rabbit.Ready,
		}),
		Port: cfg.PortAdmin,
	})

	g.Go("admin server", adminServer.Start)
	g.Go("outbox dispatcher", outboxDispatcher.Start)
	g.Go("inbox", inboxWorker.Start)
	g.Go("cleanup", cleanupJob)
	g.Go("dlq monitor", dlqMonitor.Start)

	return nil
}
