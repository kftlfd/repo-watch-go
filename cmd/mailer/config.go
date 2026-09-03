package main

import (
	"fmt"
	"os"
)

type Config struct {
	RabbitMQUrl string
}

func loadConfig() (Config, error) {
	cfg := Config{
		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
	}

	if cfg.RabbitMQUrl == "" {
		return cfg, fmt.Errorf("no RABBITMQ_URL")
	}

	return cfg, nil
}
