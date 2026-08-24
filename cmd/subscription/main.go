package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"repo-watch/internal/subscription/config"
	"repo-watch/internal/subscription/database"
	"repo-watch/internal/subscription/database/migrations"
	"repo-watch/internal/subscription/server"
	"repo-watch/internal/subscription/subs"
	"repo-watch/pkg/github"
	"repo-watch/pkg/logger"
	"repo-watch/pkg/postgres"
	"repo-watch/pkg/rabbitmq"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	logger := logger.NewLogger()

	err := run(logger)

	if err != nil {
		logger.Error("exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		stop()
	}()

	cfg, err := config.Load()
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
	if err := postgres.Migrate(ctx, db, migrations.MigrationsFS); err != nil {
		return fmt.Errorf("DB migrate: %w", err)
	}

	rabbit, err := rabbitmq.Connect(cfg.RabbitMQUrl)
	if err != nil {
		return fmt.Errorf("RabbitMQ connect: %w", err)
	}
	defer rabbit.Close()

	gh := github.NewClient()

	q := database.New(db)

	subsSvc := subs.NewService(subs.Deps{
		DB:           db,
		GithubClient: gh,
		Queries:      q,
	})

	srv := server.NewServer(server.ServerDeps{
		Logger:      logger,
		SubsService: subsSvc,
		Port:        "8080",
	})

	var wg sync.WaitGroup

	wg.Go(func() {
		srv.Start(ctx, cancel)
	})

	<-ctx.Done()

	shutdownTimer := time.NewTimer(10 * time.Second)
	defer shutdownTimer.Stop()

	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-shutdownTimer.C:
		return fmt.Errorf("shutdown timeout")
	case <-allDone:
	}

	if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
		return fmt.Errorf("ctx cause: %w", cause)
	}

	return nil
}
