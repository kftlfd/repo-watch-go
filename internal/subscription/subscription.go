package service

import (
	"context"
	"database/sql"
	"log/slog"
	"repo-watch/internal/subscription/config"
	"repo-watch/internal/subscription/database/migrations"
	"repo-watch/internal/subscription/server"

	"github.com/rabbitmq/amqp091-go"
)

var MigrationsFS = migrations.MigrationsFS

func LoadConfig() (config.SubscriptionConfig, error) {
	return config.Load()
}

type Deps struct {
	Cfg    config.SubscriptionConfig
	Logger *slog.Logger
	DB     *sql.DB
	Rabbit *amqp091.Connection
}

func Run(ctx context.Context, d Deps) error {
	server := server.New(server.Deps{
		Logger: d.Logger,
	})

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- server.Start()
	}()

	select {
	case err := <-serverErrCh:
		return err

	case <-ctx.Done():
		return server.Shutdown()
	}
}
