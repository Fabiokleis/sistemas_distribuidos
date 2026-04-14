package rabbitmq

import (
	"fmt"
	"log"

	"promocao/internal/exchange"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SetupConsumer(ch *amqp.Channel, routingKeys ...string) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	for _, key := range routingKeys {
		err = ch.QueueBind(q.Name, key, exchange.Name, false, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to bind queue to key %s: %w", key, err)
		}
		log.Printf("[+] queue %v binded routing key %v\n", q.Name, key)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to consume: %w", err)
	}

	return msgs, nil
}

func PublishEvent(ch *amqp.Channel, routingKey string, payload []byte) error {
	return ch.Publish(
		exchange.Name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: exchange.ContentType,
			Body:        payload,
		},
	)
}
