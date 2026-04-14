package rabbitmq

import (
	"log"
	"os"
	"promocao/internal/exchange"

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
	ch, err := Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel error: %v", err)
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange.Name, // nome da exchange
		"topic",       // tipo
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	return nil
}
