package main

import (
	"log"

	mq "promocao/internal/rabbitmq"
	"promocao/internal/ranking"
)

func main() {
	if err := mq.Init(); err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ranking.Run()
}
