package config

import (
	"fmt"
	"os"
	"repo-watch/pkg/postgres"
)

type SubscriptionConfig struct {
	DbUrl       string
	RabbitMQUrl string
}

func Load() (SubscriptionConfig, error) {
	cfg := SubscriptionConfig{
		DbUrl:       os.Getenv("DB_URL"),
		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
	}

	if cfg.DbUrl == "" {
		return cfg, fmt.Errorf("no DB_URL")
	}
	cfg.DbUrl = postgres.CreateDbUrl(cfg.DbUrl, "subscription_db")

	if cfg.RabbitMQUrl == "" {
		return cfg, fmt.Errorf("no RABBITMQ_URL")
	}

	return cfg, nil
}
