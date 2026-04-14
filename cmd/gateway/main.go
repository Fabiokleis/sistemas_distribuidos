package main

import (
	"log"

	"promocao/internal/gateway"
	mq "promocao/internal/rabbitmq"
)

func main() {
	if err := mq.Init(); err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	gateway.Run()
}
