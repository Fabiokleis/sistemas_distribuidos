package ranking

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

func TestRankingIntegration(t *testing.T) {
	if err := mq.Init(); err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer mq.Connection.Close()

	ch, err := mq.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	msgs, err := mq.SetupConsumer(ch, ex.KeyHotDeal)
	if err != nil {
		t.Fatalf("failed to setup test consumer: %v", err)
	}

	go Run()
	time.Sleep(500 * time.Millisecond)

	testID := "ranking-promo-123"
	var gtwPrivateKey *rsa.PrivateKey
	gtwPrivateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Gateway + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	for i := 0; i < ex.HotDealThreshold; i++ {
		voteEvent := &events.VoteEvent{
			PromotionId: testID,
			VoteValue:   1,
			Category:    "game",
			Description: "The Witcher 3 - 80% Off",
		}
		outInnerBytes, _ := proto.Marshal(voteEvent)
		signature, err := crypto.SignPayload(gtwPrivateKey, outInnerBytes)

		envelopeVote := &events.EventEnvelope{
			Timestamp:  timestamppb.Now(),
			ProducerId: ex.Gateway,
			Signature:  signature,
			Payload:    &events.EventEnvelope_Vote{Vote: voteEvent},
		}
		bodyVote, _ := proto.Marshal(envelopeVote)

		err = mq.PublishEvent(ch, ex.KeyPromotionVote, bodyVote)
		if err != nil {
			t.Fatalf("failed to publish vote: %v", err)
		}
	}

	select {
	case d := <-msgs:
		var receivedEnvelope events.EventEnvelope
		if err := proto.Unmarshal(d.Body, &receivedEnvelope); err != nil {
			t.Fatalf("failed to unmarshal hot deal event: %v", err)
		}

		payload, ok := receivedEnvelope.Payload.(*events.EventEnvelope_PromotionPublished)
		if !ok {
			t.Fatalf("expected PromotionPublishedEvent for hot deal")
		}

		if payload.PromotionPublished.PromotionId != testID {
			t.Errorf("expected PromotionId %s, got %s", testID, payload.PromotionPublished.PromotionId)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("timeout: ms ranking did not publish the hot deal")
	}
}
