package client

import (
	"log"
	"promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConsumeAndDisplayIntegration(t *testing.T) {

	if err := mq.Init(); err != nil {
		log.Fatalf("failed to connect rabbitmq error: %v", err)
	}
	defer mq.Connection.Close()
	conn := mq.Connection

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel error: %v", err)
	}
	defer ch.Close()

	go Run()
	time.Sleep(500 * time.Millisecond)

	for _, cat := range exchange.Categories {

		promo := &events.PromotionPublishedEvent{
			PromotionId: "test-id-123",
			Category:    cat,
			Description: "Promoção de teste para " + cat,
		}

		envelope := &events.EventEnvelope{
			Timestamp: timestamppb.Now(),
			Payload:   &events.EventEnvelope_PromotionPublished{PromotionPublished: promo},
		}

		body, _ := proto.Marshal(envelope)

		routingKey := exchange.KeyNotificationPrefix + cat

		if err := mq.PublishEvent(ch, routingKey, body); err != nil {
			t.Errorf("failed to publish: %v", err)
		}
	}

	time.Sleep(1 * time.Second)
}
