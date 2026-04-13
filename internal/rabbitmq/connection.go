package rabbitmq

import (
	"os"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

var Connection *amqp.Connection

func Init() error {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://promocao:1234@localhost:5672/"
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("failed to connect to RabbitMQ: %v", err)
		return err
	}
	Connection = conn
	return nil
}
