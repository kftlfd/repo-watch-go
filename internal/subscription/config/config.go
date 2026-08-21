package config

import (
	"fmt"
	"os"
	"repo-watch/pkg/postgres"
)

type SubscriptionConfig struct {
	DbBaseUrl string
	DbUrl     string
	DbName    string

	RabbitMQUrl        string
}

func Load() (SubscriptionConfig, error) {
	cfg := SubscriptionConfig{
		DbBaseUrl: os.Getenv("POSTGRES_URL"),
		DbName:    "subscription_db",

		RabbitMQUrl:        os.Getenv("RABBITMQ_URL"),
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
