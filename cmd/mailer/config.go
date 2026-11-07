package main

import (
	"fmt"
	"os"
)

type Config struct {
	PortAdmin string

	RabbitMQUrl string
}

func loadConfig() (Config, error) {
	cfg := Config{
		PortAdmin:   "9041",
		RabbitMQUrl: os.Getenv("RABBITMQ_URL"),
	}

	if cfg.RabbitMQUrl == "" {
		return cfg, fmt.Errorf("no RABBITMQ_URL")
	}

	return cfg, nil
}
