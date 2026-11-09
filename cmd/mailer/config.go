package main

import (
	"fmt"
	"log/slog"
	"os"

	"repo-watch/internal/logger"
	"repo-watch/internal/utils"
)

type Config struct {
	LogLevel    slog.Level
	PortAdmin   string
	RabbitMQUrl string
	AppBaseURL  string
}

func loadConfig() *Config {
	return &Config{
		LogLevel:    logger.ParseLevel(os.Getenv("LOG_LEVEL")),
		PortAdmin:   utils.Env("PORT_ADMIN", "4000"),
		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
		AppBaseURL:  utils.Env("APP_BASE_URL", "http://localhost:3000"),
	}
}

func (cfg *Config) validateConfig() error {
	if cfg.RabbitMQUrl == "" {
		return fmt.Errorf("no RABBITMQ_URL")
	}

	return nil
}
