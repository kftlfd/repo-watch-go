package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	service "repo-watch/internal/subscription"
	"repo-watch/pkg/logger"
	"repo-watch/pkg/postgres"
	"repo-watch/pkg/rabbitmq"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_ = godotenv.Load()

	logger := logger.NewLogger()

	if err := run(ctx, logger); err != nil {
		logger.Error("exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := service.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

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
	if err := postgres.Migrate(ctx, db, service.MigrationsFS); err != nil {
		return fmt.Errorf("DB migrate: %w", err)
	}

	rabbit, err := rabbitmq.Connect(cfg.RabbitMQUrl)
	if err != nil {
		return fmt.Errorf("RabbitMQ connect: %w", err)
	}
	defer rabbit.Close()

	return service.Run(ctx, service.Deps{
		Cfg:    cfg,
		Logger: logger,
		DB:     db,
		Rabbit: rabbit,
	})
}
