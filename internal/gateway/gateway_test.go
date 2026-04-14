package gateway

import (
	"crypto/rsa"
	"log"
	"log/slog"
	"testing"
	"time"

	"promocao/internal/crypto"
	"promocao/internal/exchange"
	ex "promocao/internal/exchange"
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

	err = ch.QueueBind(q.Name, ex.KeyPromotionReceived, ex.Name, false, nil)
	if err != nil {
		t.Fatalf("failed to bind queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	testCategory := "book"
	testDescription := "Test Book 50% Off Gateway"

	loadKeys()
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

	go setupPublicationConsumer(ch)

	time.Sleep(500 * time.Millisecond)

	testID := "promo-test-id-999"
	publishedEvent := &events.PromotionPublishedEvent{
		PromotionId: testID,
		Category:    "game",
		Description: "GTA V 90% Off",
	}

	var promocaoPrivateKey *rsa.PrivateKey
	promocaoPrivateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Promocao + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	outInnerBytes, _ := proto.Marshal(publishedEvent)
	signature, err := crypto.SignPayload(promocaoPrivateKey, outInnerBytes)
	if err != nil {
		slog.Error("failed to sign outgoing promotion", "error", err)
		return
	}

	envelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Promocao,
		Signature:  signature,
		Payload:    &events.EventEnvelope_PromotionPublished{PromotionPublished: publishedEvent},
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
