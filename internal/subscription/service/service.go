package service

import (
	"database/sql"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type Deps struct {
	Db     *sql.DB
	Rabbit *amqp091.Connection
}

func NewService(_ Deps) {
	fmt.Println("ready")
}
