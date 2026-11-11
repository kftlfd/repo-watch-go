package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"repo-watch/internal/admin"
	"repo-watch/internal/admin/ready"
	"repo-watch/internal/app"
	"repo-watch/internal/dispatcher"
	"repo-watch/internal/github"
	"repo-watch/internal/httpserver"
	"repo-watch/internal/logger"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/api"
	"repo-watch/internal/subscription/cleanup"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
	"repo-watch/internal/subscription/inbox"
	"repo-watch/internal/subscription/outbox"
	"repo-watch/internal/subscription/subs"
	"repo-watch/internal/subscription/web"
	"repo-watch/internal/taskrunner"
)

func main() {
	_ = godotenv.Load()

	cfg := loadConfig()

	logger := logger.NewLogger(cfg.LogLevel, "service", "subscription")

	app.Run(logger, cfg.run)
}

func (cfg *Config) run(g *app.Group, logger *slog.Logger) error {
	if err := cfg.validateConfig(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	if !cfg.IsDev {
		gin.SetMode(gin.ReleaseMode)
	}

	metricsReg := prometheus.NewRegistry()
	rabbitMetrics := rabbitmq.NewMetrics(metricsReg)

	logger.Info("preparing DB")

	if err := postgres.EnsureDbExists(g.Ctx(), cfg.DbBaseUrl, cfg.DbName); err != nil {
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

	githubClient := github.NewClient(
		github.WithBaseURL(cfg.GhApiUrl),
		github.WithApiKey(cfg.GithubToken),
	)

	queries := dbGen.New(db)

	subsSvc := subs.NewService(subs.Deps{
		DB:           db,
		GithubClient: githubClient,
		Queries:      queries,
	})

	outboxDispatcher, err := dispatcher.NewDispatcher(dispatcher.DispatcherDeps{
		Logger:    logger.With("module", "subscription_outbox"),
		Store:     outbox.NewSubscriptionOutbox(queries),
		Publisher: rabbitPublisher,
		Metrics:   dispatcher.NewMetrics(metricsReg),
	})
	if err != nil {
		return fmt.Errorf("outbox dispatcher: %w", err)
	}

	inboxWorker, err := inbox.NewInboxWorker(inbox.Deps{
		Logger:         logger,
		DB:             db,
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

	adminServer := httpserver.NewServer(httpserver.ServerDeps{
		Port:   cfg.PortAdmin,
		Logger: logger.With("module", "admin_server"),
		Handler: admin.NewHandler(admin.Deps{
			Metrics: metricsReg,
			Ready: ready.All(
				ready.Database(db),
				ready.Func("rabbitmq", rabbit.Ready),
			),
		}),
	})

	httpLogger := logger.With("module", "http_server")
	httpMetrics := httpserver.NewMetrics(metricsReg)

	router := gin.New()
	router.Use(httpserver.ObservabilityMiddleware(httpLogger, httpMetrics))
	router.Use(httpserver.Recovery())

	api.RegisterRoutes(router.Group("/api"), api.Deps{
		SubsService: subsSvc,
		DocsHost:    cfg.APIHost,
	})
	web.RegisterRoutes(router.Group("/"), web.Deps{
		SubsService: subsSvc,
	})

	router.NoRoute(func(c *gin.Context) {
		if c.Request.URL.Path == "/api" || strings.HasPrefix(c.Request.URL.Path, "/api/") {
			api.NotFound(c)
		} else {
			web.NotFound(c)
		}
	})

	apiServer := httpserver.NewServer(httpserver.ServerDeps{
		Port:    cfg.PortAPI,
		Logger:  httpLogger,
		Handler: router,
	})

	g.Go("admin server", adminServer.Start)
	g.Go("outbox dispatcher", outboxDispatcher.Start)
	g.Go("inbox", inboxWorker.Start)
	g.Go("cleanup", cleanupJob)
	g.Go("api server", apiServer.Start)

	return nil
}
