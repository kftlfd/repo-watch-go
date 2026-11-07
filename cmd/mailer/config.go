package main

import (
	"fmt"
	"log/slog"
	"os"
	"repo-watch/internal/logger"
)

type Config struct {
	LogLevel    slog.Level
	PortAdmin   string
	RabbitMQUrl string
}

func loadConfig() *Config {
	return &Config{
		LogLevel:    logger.ParseLevel(os.Getenv("LOG_LEVEL")),
		PortAdmin:   "9041",
		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
	}
}

func (cfg *Config) validateConfig() error {
	if cfg.RabbitMQUrl == "" {
		return fmt.Errorf("no RABBITMQ_URL")
	}

	return nil
}
