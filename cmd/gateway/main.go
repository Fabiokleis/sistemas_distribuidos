package main

import (
	"log"

	gwt "promocao/internal/gateway"
	mq "promocao/internal/rabbitmq"
)

func main() {
	if err := mq.Init(); err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	g := gwt.New()
	g.LoadKeys()
	go g.Run()
	r := gwt.Router{}
	r.Serve(&g)
}
