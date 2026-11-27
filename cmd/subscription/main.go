package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
	"repo-watch/internal/observability"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/api"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/cleanup"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/db/migrations"
	"repo-watch/internal/subscription/inbox"
	"repo-watch/internal/subscription/outbox"
	"repo-watch/internal/subscription/subs"
	"repo-watch/internal/subscription/web"
	"repo-watch/internal/taskrunner"
)

const migrateTimeout = 5 * time.Minute

func main() {
	_ = godotenv.Load()

	cfg := loadConfig()

	logger := observability.NewLogger(cfg.LogLevel, "service", "subscription")

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		app.Run(logger, cfg.runMigrate)
		return
	}

	app.Run(logger, cfg.run)
}

func (cfg *Config) runMigrate(g *app.Group, logger *slog.Logger) error {
	if err := cfg.validateDbConfig(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	ctx, cancel := context.WithTimeout(g.Ctx(), migrateTimeout)
	db, err := postgres.Connect(ctx, cfg.Db, cfg.Db.AutoCreate, true, migrations.MigrationsFS)
	cancel()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	logger.Info("migrations applied")
	return nil
}

func (cfg *Config) run(g *app.Group, logger *slog.Logger) error {
	if err := cfg.validateConfig(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	tracingShutdown, err := observability.SetupTracing(g.Ctx(), observability.TracingConfig{
		ServiceName:  "subscription",
		ErrorHandler: func(err error) { logger.Warn("tracing error", "error", err) },
	})
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	g.Cleanup(func() { _ = tracingShutdown() })

	if !cfg.IsDev {
		gin.SetMode(gin.ReleaseMode)
	}

	metricsReg := prometheus.NewRegistry()
	rabbitMetrics := rabbitmq.NewMetrics(metricsReg)

	logger.Info("preparing DB")

	ctx, cancel := context.WithTimeout(g.Ctx(), migrateTimeout)
	db, err := postgres.Connect(ctx, cfg.Db, cfg.Db.AutoCreate, cfg.Db.AutoMigrate, migrations.MigrationsFS)
	cancel()
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	g.Cleanup(func() { _ = db.Close() })

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

	rabbitPublisher, err := rabbitmq.NewPublisher(rabbit, logger, rabbitMetrics)
	if err != nil {
		return fmt.Errorf("create rabbit publisher: %w", err)
	}

	rabbitConsumer, err := rabbitmq.NewConsumer(
		rabbit,
		logger,
		rabbitMetrics,
		rabbitmq.WithRetryPublisher(rabbitPublisher),
	)
	if err != nil {
		return fmt.Errorf("create rabbit consumer: %w", err)
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

	authSvc, err := auth.NewService(auth.Deps{
		DB:      db,
		Queries: queries,
		Config: auth.Config{
			JWTSecret:  cfg.JWTSecret,
			AccessTTL:  cfg.AccessTokenTTL,
			RefreshTTL: cfg.RefreshTokenTTL,
		},
	})
	if err != nil {
		return fmt.Errorf("create auth service: %w", err)
	}

	cookies := auth.NewCookieManager(auth.CookieConfig{BaseURL: cfg.AppBaseURL})
	csrf, err := httpserver.NewCSRF(httpserver.CSRFConfig{BaseURL: cfg.AppBaseURL, Secret: cfg.JWTSecret})
	if err != nil {
		return fmt.Errorf("create csrf protection: %w", err)
	}

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
		taskrunner.NewMetrics(metricsReg),
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
	router.Use(httpserver.ObservabilityMiddleware(httpLogger, httpMetrics, false))
	router.Use(httpserver.Recovery())

	if err := api.RegisterRoutes(router.Group("/api"), api.Deps{
		SubsService: subsSvc,
		AuthService: authSvc,
		DocsHost:    cfg.APIHost,
	}); err != nil {
		return fmt.Errorf("register api routes: %w", err)
	}
	web.RegisterRoutes(router.Group("/"), web.Deps{
		SubsService: subsSvc,
		AuthService: authSvc,
		Cookies:     cookies,
		CSRF:        csrf,
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
