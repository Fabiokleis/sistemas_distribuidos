package notification

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

func TestNotificationRouting(t *testing.T) {
	if err := mq.Init(); err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ch, err := mq.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	testCategory := "game"
	clientRoutingKey := ex.KeyNotificationPrefix + testCategory
	msgs, err := mq.SetupConsumer(ch, clientRoutingKey)
	if err != nil {
		t.Fatalf("failed to setup test consumer: %v", err)
	}

	var rankingPrivateKey *rsa.PrivateKey
	rankingPrivateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Ranking + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	go Run()
	time.Sleep(500 * time.Millisecond)

	testID := "notif-promo-123"
	originalDesc := "Console de Última Geração"
	hotDealPromo := &events.PromotionPublishedEvent{
		PromotionId: testID,
		Category:    testCategory,
		Description: originalDesc,
	}

	outInnerBytes, _ := proto.Marshal(hotDealPromo)
	signature, err := crypto.SignPayload(rankingPrivateKey, outInnerBytes)

	envelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Ranking,
		Signature:  signature,
		Payload:    &events.EventEnvelope_PromotionPublished{PromotionPublished: hotDealPromo},
	}
	body, _ := proto.Marshal(envelope)

	err = mq.PublishEvent(ch, ex.KeyHotDeal, body)
	if err != nil {
		t.Fatalf("failed to publish mock hot deal: %v", err)
	}
	select {
	case d := <-msgs:
		var receivedEnvelope events.EventEnvelope
		if err := proto.Unmarshal(d.Body, &receivedEnvelope); err != nil {
			t.Fatalf("failed to unmarshal dispatched event: %v", err)
		}

		payload, ok := receivedEnvelope.Payload.(*events.EventEnvelope_PromotionPublished)
		if !ok {
			t.Fatalf("expected PromotionPublishedEvent for client dispatch")
		}

		receivedDesc := payload.PromotionPublished.Description

		expectedPrefix := "**HOT DEAL**: "
		if receivedDesc != expectedPrefix+originalDesc {
			t.Errorf("expected description to be '%s', got '%s'", expectedPrefix+originalDesc, receivedDesc)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("timeout: ms notification did not route the message to the client queue")
	}
}
