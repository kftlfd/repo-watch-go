package main

import (
	"log"
	"os"
	"os/signal"
	"repo-watch/internal/subscription/config"
	"repo-watch/internal/subscription/service"
	"repo-watch/pkg/postgres"
	"repo-watch/pkg/rabbitmq"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	db, err := postgres.Connect(cfg.DbUrl)
	if err != nil {
		panic(err)
	}

	rabbit, err := rabbitmq.Connect(cfg.RabbitMQUrl)
	if err != nil {
		panic(err)
	}

	service.NewService(service.Deps{
		Db:     db,
		Rabbit: rabbit,
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(
		sigCh,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer signal.Stop(sigCh)
	sig := <-sigCh

	log.Println("exiting:", sig.String())

}
