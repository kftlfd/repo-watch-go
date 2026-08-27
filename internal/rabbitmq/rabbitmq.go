package rabbitmq

import (
	"github.com/rabbitmq/amqp091-go"
)

func Connect(rabbitMQUrl string) (*amqp091.Connection, error) {
	conn, err := amqp091.Dial(rabbitMQUrl)
	return conn, err
}
