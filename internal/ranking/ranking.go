package ranking

import (
	"crypto/rsa"
	"log"
	"log/slog"
	"sync"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PromoState struct {
	Score     int32
	IsHotDeal bool
}

var (
	privateKey    *rsa.PrivateKey
	gatewayPubKey *rsa.PublicKey

	promos = make(map[string]*PromoState)
	mu     sync.Mutex
)

func loadKeys() {
	var err error
	privateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Ranking + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load ranking private key: %v", err)
	}

	gatewayPubKey, err = crypto.LoadPublicKey(crypto.GetKeyPath(ex.Gateway + ex.PublicKeySuffix))
	if err != nil {
		log.Fatalf("failed to load gateway public key: %v", err)
	}
}

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	loadKeys()

	log.Println("[*] ms ranking started ...")

	msgs, err := mq.SetupConsumer(ch, ex.KeyPromotionVote) // promocao.voto
	if err != nil {
		log.Fatalf("failed to setup consumer: %v", err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			processVote(ch, d)
		}
	}()

	<-forever
}

func processVote(ch *amqp.Channel, d amqp.Delivery) {
	var envelope events.EventEnvelope
	if err := proto.Unmarshal(d.Body, &envelope); err != nil {
		slog.Error("failed to unmarshal message in ranking", "error", err)
		return
	}

	payload, ok := envelope.Payload.(*events.EventEnvelope_Vote)
	if !ok {
		return
	}

	vote := payload.Vote

	innerBytes, _ := proto.Marshal(vote)
	if err := crypto.VerifySignature(gatewayPubKey, innerBytes, envelope.Signature); err != nil {
		slog.Error("INVALID VOTE SIGNATURE: ms ranking dropped vote", "promotion_id", vote.PromotionId, "err", err)
		return
	}

	promoID := vote.PromotionId
	mu.Lock()

	if promos[promoID] == nil {
		promos[promoID] = &PromoState{Score: 0, IsHotDeal: false}
	}

	state := promos[promoID]
	state.Score += vote.VoteValue
	currentScore := state.Score

	shouldPublish := false
	if currentScore >= ex.HotDealThreshold && !state.IsHotDeal {
		state.IsHotDeal = true
		shouldPublish = true
	}

	mu.Unlock()

	slog.Info("vote verified",
		"promotion_id", promoID,
		"category", vote.Category,
		"value", currentScore,
		"description", vote.Description,
	)

	if shouldPublish {
		slog.Info("vote hot deal",
			"promotion_id", promoID,
			"category", vote.Category,
			"value", currentScore,
			"description", vote.Description,
		)

		publishHotDeal(ch, vote)
	}
}

func publishHotDeal(ch *amqp.Channel, vote *events.VoteEvent) {

	promo := &events.PromotionPublishedEvent{
		PromotionId: vote.PromotionId,
		Category:    vote.Category,
		Description: vote.Description,
	}

	innerBytes, _ := proto.Marshal(promo)
	signature, err := crypto.SignPayload(privateKey, innerBytes)
	if err != nil {
		slog.Error("failed to sign hot deal event", "error", err)
		return
	}

	envelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Ranking,
		Signature:  signature,
		Payload:    &events.EventEnvelope_PromotionPublished{PromotionPublished: promo},
	}

	bodyBytes, err := proto.Marshal(envelope)
	if err != nil {
		slog.Error("failed to marshal hot deal event", "error", err)
		return
	}

	if err := mq.PublishEvent(ch, ex.KeyHotDeal, bodyBytes); err != nil {
		slog.Error("failed to publish hot deal", "error", err)
	} else {
		slog.Info("dispatched",
			"routing-key", ex.KeyHotDeal,
			"description", promo.Description,
		)
	}
}
