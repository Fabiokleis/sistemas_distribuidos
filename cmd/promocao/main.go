package main

import (
	"log"

	"promocao/internal/promocao"
	mq "promocao/internal/rabbitmq"
)

func main() {
	if err := mq.Init(); err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	promocao.Run()
}
