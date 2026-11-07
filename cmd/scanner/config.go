package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"repo-watch/internal/logger"
	"repo-watch/internal/postgres"
	"repo-watch/internal/scanner/worker"
)

type Config struct {
	LogLevel    slog.Level
	IsDev       bool
	PortAdmin   string
	DbBaseUrl   string
	DbUrl       string
	DbName      string
	RabbitMQUrl string
	ScannerCfg  worker.Config
	Concurrency ConcurrencyCfg
	GhApiUrl    string
	GithubToken string
}

type ConcurrencyCfg struct {
	ScannerWorkers int
}

func loadConfig() *Config {
	return &Config{
		LogLevel: logger.ParseLevel(os.Getenv("LOG_LEVEL")),

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

		Concurrency: ConcurrencyCfg{
			ScannerWorkers: 1,
		},

		GhApiUrl:    os.Getenv("GH_API_URL"),
		GithubToken: os.Getenv("GITHUB_TOKEN"),
	}
}

func (cfg *Config) validateConfig() error {
	if cfg.DbBaseUrl == "" {
		return fmt.Errorf("no POSTGRES_URL")
	}
	cfg.DbUrl = postgres.CreateDbUrl(cfg.DbBaseUrl, cfg.DbName)

	if cfg.RabbitMQUrl == "" {
		return fmt.Errorf("no RABBITMQ_URL")
	}

	return nil
}
