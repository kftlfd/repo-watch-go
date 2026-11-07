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
	PortAPI     string
	PortAdmin   string
	DbBaseUrl   string
	DbUrl       string
	DbName      string
	RabbitMQUrl string
	GhApiUrl    string
	GithubToken string
}

func loadConfig() *Config {
	return &Config{
		LogLevel: logger.ParseLevel(os.Getenv("LOG_LEVEL")),

		IsDev:     os.Getenv("ENV") == "dev",
		PortAPI:   utils.Env("PORT", "3000"),
		PortAdmin: utils.Env("PORT_ADMIN", "4000"),

		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    utils.Env("DB_NAME", "subscription_db"),

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		GhApiUrl:    utils.Env("GH_API_URL", "https://api.github.com"),
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
