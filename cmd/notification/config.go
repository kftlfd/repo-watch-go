package main

import (
	"fmt"
	"os"
	"repo-watch/internal/notification/outbox"
	"repo-watch/internal/postgres"
	"time"
)

type Config struct {
	IsDev     bool
	PortAdmin string

	DbBaseUrl string
	DbUrl     string
	DbName    string

	RabbitMQUrl string

	OutboxCfg outbox.Config
}

func loadConfig() (Config, error) {
	cfg := Config{
		IsDev:     os.Getenv("ENV") == "dev",
		PortAdmin: "9031",

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    "notification_db",

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		OutboxCfg: outbox.Config{
			BatchSize:            5,
			LoopInterval:         10 * time.Second,
			LeaseDuration:        60 * time.Second,
			LoopIterationTimeout: 30 * time.Second,
		},
	}

	if cfg.DbBaseUrl == "" {
		return cfg, fmt.Errorf("no POSTGRES_URL")
	}
	cfg.DbUrl = postgres.CreateDbUrl(cfg.DbBaseUrl, cfg.DbName)

	if cfg.RabbitMQUrl == "" {
		return cfg, fmt.Errorf("no RABBITMQ_URL")
	}

	return cfg, nil
}
