package gateway

import (
	"testing"
	"time"

	"promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublishPromotion(t *testing.T) {
	if err := mq.Init(); err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ch, err := mq.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(q.Name, exchange.KeyPromotionReceived, exchange.Name, false, nil)
	if err != nil {
		t.Fatalf("failed to bind queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	testCategory := "book"
	testDescription := "Test Book 50% Off Gateway"

	publishPromotion(ch, testCategory, testDescription)

	select {
	case d := <-msgs:
		var envelope events.EventEnvelope
		if err := proto.Unmarshal(d.Body, &envelope); err != nil {
			t.Fatalf("failed to unmarshal protobuf message: %v", err)
		}

		payload, ok := envelope.Payload.(*events.EventEnvelope_NewPromotion)
		if !ok {
			t.Fatalf("expected NewPromotion event, got different type")
		}

		if payload.NewPromotion.Category != testCategory {
			t.Errorf("expected category %s, got %s", testCategory, payload.NewPromotion.Category)
		}
		if payload.NewPromotion.Description != testDescription {
			t.Errorf("expected description %s, got %s", testDescription, payload.NewPromotion.Description)
		}
		if payload.NewPromotion.PromotionId == "" {
			t.Errorf("expected a non-empty Promotion ID")
		}

	case <-time.After(3 * time.Second):
		t.Fatal("timeout: ms gateway test timed out waiting for message")
	}
}

func TestPublicationConsumer(t *testing.T) {
	if err := mq.Init(); err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ch, err := mq.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(exchange.Name, "topic", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}

	go setupPublicationConsumer(ch)

	time.Sleep(500 * time.Millisecond)

	testID := "promo-test-id-999"
	publishedEvent := &events.PromotionPublishedEvent{
		PromotionId: testID,
		Category:    "game",
		Description: "GTA V 90% Off",
	}

	envelope := &events.EventEnvelope{
		Timestamp: timestamppb.Now(),
		Payload:   &events.EventEnvelope_PromotionPublished{PromotionPublished: publishedEvent},
	}

	bodyBytes, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("failed to marshal published event: %v", err)
	}

	mq.PublishEvent(ch, exchange.KeyPromotionPublished, bodyBytes)
	if err != nil {
		t.Fatalf("failed to publish mock event: %v", err)
	}

	time.Sleep(1 * time.Second)

	mu.RLock()
	promo, exists := promos[testID]
	mu.RUnlock()

	if !exists {
		t.Fatalf("expected promotion %s to be in the local map, but it was not found", testID)
	}

	if promo.Description != "GTA V 90% Off" {
		t.Errorf("expected description 'GTA V 90%% Off', got %s", promo.Description)
	}
}
