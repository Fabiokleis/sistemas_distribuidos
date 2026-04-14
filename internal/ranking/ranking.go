package ranking

import (
	"log"
	"log/slog"
	"sync"

	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const HotDealThreshold = 3

var (
	votes    = make(map[string]int32)
	hotDeals = make(map[string]bool)
	mu       sync.Mutex
)

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

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

	mu.Lock()
	defer mu.Unlock()

	promoID := vote.PromotionId
	votes[promoID] += vote.VoteValue

	slog.Info("new vote",
		"promotion_id", promoID,
		"category", vote.Category,
		"value", votes[promoID],
		"description", vote.Description,
	)

	if votes[promoID] >= HotDealThreshold && !hotDeals[promoID] {
		hotDeals[promoID] = true
		slog.Info("vote hot deal",
			"promotion_id", promoID,
			"category", vote.Category,
			"value", votes[promoID],
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

	envelope := &events.EventEnvelope{
		Timestamp: timestamppb.Now(),
		Payload:   &events.EventEnvelope_PromotionPublished{PromotionPublished: promo},
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
