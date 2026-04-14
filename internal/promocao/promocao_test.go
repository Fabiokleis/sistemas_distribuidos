package promocao

import (
	"crypto/rsa"
	"log"
	"testing"
	"time"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPromocaoIntegration(t *testing.T) {
	if err := mq.Init(); err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ch, err := mq.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	msgs, err := mq.SetupConsumer(ch, ex.KeyPromotionPublished)
	if err != nil {
		t.Fatalf("failed to setup test consumer: %v", err)
	}

	go Run()
	time.Sleep(500 * time.Millisecond)

	testID := "promo-test-123"
	newPromo := &events.NewPromotionEvent{
		PromotionId: testID,
		Category:    "eletronicos",
		Description: "Smartphone com 20% de desconto",
	}

	var gtwPrivateKey *rsa.PrivateKey
	gtwPrivateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Gateway + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	outInnerBytes, _ := proto.Marshal(newPromo)
	signature, err := crypto.SignPayload(gtwPrivateKey, outInnerBytes)

	envelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Gateway,
		Signature:  signature,
		Payload:    &events.EventEnvelope_NewPromotion{NewPromotion: newPromo},
	}

	bodyBytes, _ := proto.Marshal(envelope)

	err = mq.PublishEvent(ch, ex.KeyPromotionReceived, bodyBytes)
	if err != nil {
		t.Fatalf("failed to publish mock event: %v", err)
	}

	select {
	case d := <-msgs:
		var receivedEnvelope events.EventEnvelope
		if err := proto.Unmarshal(d.Body, &receivedEnvelope); err != nil {
			t.Fatalf("failed to unmarshal published event: %v", err)
		}

		payload, ok := receivedEnvelope.Payload.(*events.EventEnvelope_PromotionPublished)
		if !ok {
			t.Fatalf("expected PromotionPublishedEvent, got different type")
		}

		published := payload.PromotionPublished

		if published.PromotionId != testID {
			t.Errorf("expected PromotionId %s, got %s", testID, published.PromotionId)
		}
		if published.Category != "eletronicos" {
			t.Errorf("expected Category 'eletronicos', got %s", published.Category)
		}
		if published.Description != "Smartphone com 20% de desconto" {
			t.Errorf("expected correct Description, got %s", published.Description)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("timeout: ms promocao did not publish the validated promotion")
	}
}
