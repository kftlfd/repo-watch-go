package main

import (
	"fmt"
	"os"
	"repo-watch/internal/postgres"
	"repo-watch/internal/scanner/outbox"
	"repo-watch/internal/scanner/worker"
	"time"
)

type Config struct {
	IsDev     bool
	PortAdmin string

	DbBaseUrl string
	DbUrl     string
	DbName    string

	RabbitMQUrl string

	ScannerCfg worker.Config
	OutboxCfg  outbox.Config
}

func loadConfig() (Config, error) {
	cfg := Config{
		IsDev:     os.Getenv("ENV") == "dev",
		PortAdmin: "9021",

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    "scanner_db",

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		ScannerCfg: worker.Config{
			BatchSize:        3,
			Interval:         2 * time.Minute,
			IterationTimeout: 30 * time.Second,
			LeaseDuration:    60 * time.Second,
		},

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
