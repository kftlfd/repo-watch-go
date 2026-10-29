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

	Concurrency ConcurrencyCfg

	GhApiUrl    string
	GithubToken string
}

type ConcurrencyCfg struct {
	ScannerWorkers int
}

func loadConfig() (Config, error) {
	cfg := Config{
		IsDev:     os.Getenv("ENV") == "dev",
		PortAdmin: "9021",

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    "scanner_db",

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		ScannerCfg: worker.Config{
			BatchSize:        5,
			Interval:         10 * time.Second,
			IterationTimeout: 10 * time.Second,
			LeaseDuration:    20 * time.Second,

			GithubMaxAttempts: 3,
			GithubRetryBase:   500 * time.Millisecond,
		},

		OutboxCfg: outbox.Config{
			BatchSize:            5,
			LoopInterval:         5 * time.Second,
			LeaseDuration:        5 * time.Second,
			LoopIterationTimeout: 3 * time.Second,
		},

		Concurrency: ConcurrencyCfg{
			ScannerWorkers: 1,
		},

		GhApiUrl:    os.Getenv("GH_API_URL"),
		GithubToken: os.Getenv("GITHUB_TOKEN"),
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
