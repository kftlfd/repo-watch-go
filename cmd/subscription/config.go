package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"repo-watch/internal/observability"
	"repo-watch/internal/postgres"
	"repo-watch/internal/utils"
)

type Config struct {
	LogLevel    slog.Level
	IsDev       bool
	PortAPI     string
	PortAdmin   string
	APIHost     string
	AppBaseURL  string
	Db          postgres.Config
	RabbitMQUrl string
	GhApiUrl    string
	GithubToken string

	JWTSecret       []byte
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

func loadConfig() *Config {
	return &Config{
		LogLevel: observability.ParseLevel(os.Getenv("LOG_LEVEL")),

		IsDev:      os.Getenv("ENV") == "dev",
		PortAPI:    utils.Env("PORT", "3000"),
		PortAdmin:  utils.Env("PORT_ADMIN", "4000"),
		APIHost:    utils.Env("API_HOST", "localhost:3000"),
		AppBaseURL: utils.Env("APP_BASE_URL", "http://localhost:3000"),

		Db: postgres.Config{
			URL:      os.Getenv("DB_URL"),
			Database: utils.Env("DB_NAME", "subscription_db"),
			SSLMode:  os.Getenv("DB_SSL_MODE"),

			AutoCreate:  utils.EnvBool("DB_AUTO_CREATE", false),
			AutoMigrate: utils.EnvBool("DB_AUTO_MIGRATE", false),
		},

		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),

		GhApiUrl:    utils.Env("GH_API_URL", "https://api.github.com"),
		GithubToken: os.Getenv("GITHUB_TOKEN"),

		JWTSecret:       []byte(os.Getenv("JWT_SECRET")),
		AccessTokenTTL:  utils.EnvDuration("ACCESS_TOKEN_TTL", 0),
		RefreshTokenTTL: utils.EnvDuration("REFRESH_TOKEN_TTL", 0),
	}
}

func (cfg *Config) validateConfig() error {
	if err := cfg.validateDbConfig(); err != nil {
		return err
	}

	if cfg.RabbitMQUrl == "" {
		return fmt.Errorf("no RABBITMQ_URL")
	}

	return nil
}

func (cfg *Config) validateDbConfig() error {
	url, err := cfg.Db.Resolve()
	if err != nil {
		return fmt.Errorf("validate DB config: %w", err)
	}

	cfg.Db.URL = url

	return nil
}
