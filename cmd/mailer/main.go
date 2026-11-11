package main

import (
	"fmt"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"repo-watch/internal/admin"
	"repo-watch/internal/admin/ready"
	"repo-watch/internal/app"
	"repo-watch/internal/httpserver"
	"repo-watch/internal/logger"
	"repo-watch/internal/mailer"
	"repo-watch/internal/rabbitmq"
)

func main() {
	_ = godotenv.Load()

	cfg := loadConfig()

	logger := logger.NewLogger(cfg.LogLevel, "service", "mailer")

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

	logger.Info("initiating components")

	mailerWorker, err := mailer.NewMailer(mailer.Deps{
		Logger:         logger,
		RabbitConsumer: rabbitConsumer,
		BaseURL:        cfg.AppBaseURL,
		EmailMetrics:   mailer.NewMetrics(metricsReg),
	})
	if err != nil {
		return fmt.Errorf("create mailer worker: %w", err)
	}

	adminServer := httpserver.NewServer(httpserver.ServerDeps{
		Logger: logger.With("module", "admin_server"),
		Handler: admin.NewHandler(admin.Deps{
			Metrics: metricsReg,
			Ready:   ready.All(ready.Func("rabbitmq", rabbit.Ready)),
		}),
		Port: cfg.PortAdmin,
	})

	g.Go("admin server", adminServer.Start)
	g.Go("mailer", mailerWorker.Start)

	return nil
}
