package main

import (
	"fmt"
	"log/slog"
	"os"

	"repo-watch/internal/logger"
	"repo-watch/internal/postgres"
	"repo-watch/internal/utils"
)

type Config struct {
	LogLevel    slog.Level
	IsDev       bool
	PortAdmin   string
	DbBaseUrl   string
	DbUrl       string
	DbName      string
	RabbitMQUrl string
}

func loadConfig() *Config {
	return &Config{
		LogLevel: logger.ParseLevel(os.Getenv("LOG_LEVEL")),

		IsDev:     os.Getenv("ENV") == "dev",
		PortAdmin: utils.Env("PORT_ADMIN", "4000"),

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    utils.Env("DB_NAME", "notification_db"),

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
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
