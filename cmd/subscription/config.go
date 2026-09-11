package main

import (
	"fmt"
	"os"
	"repo-watch/internal/postgres"
	"repo-watch/internal/subscription/outbox"
	"time"
)

type Config struct {
	IsDev     bool
	PortAPI   string
	PortAdmin string

	DbBaseUrl string
	DbUrl     string
	DbName    string

	RabbitMQUrl string

	OutboxCfg outbox.Config

	GhApiUrl string
}

func loadConfig() (Config, error) {
	cfg := Config{
		IsDev:     os.Getenv("ENV") == "dev",
		PortAPI:   "9010",
		PortAdmin: "9011",

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    "subscription_db",

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		OutboxCfg: outbox.Config{
			BatchSize:            5,
			LoopInterval:         5 * time.Second,
			LeaseDuration:        5 * time.Second,
			LoopIterationTimeout: 3 * time.Second,
		},

		GhApiUrl: os.Getenv("GH_API_URL"),
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
