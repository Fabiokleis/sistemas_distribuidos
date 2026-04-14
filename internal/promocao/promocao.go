package promocao

import (
	"log"
	"log/slog"

	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	"promocao/internal/rabbitmq"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	log.Println("[*] ms-promocao started ...")

	msgs, err := rabbitmq.SetupConsumer(ch, ex.KeyPromotionReceived)
	if err != nil {
		log.Fatalf("failed to setup consumer: %v", err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			processIncomingPromotion(ch, d.Body)
		}
	}()

	<-forever
}

func processIncomingPromotion(ch *amqp.Channel, body []byte) {
	var envelope events.EventEnvelope
	if err := proto.Unmarshal(body, &envelope); err != nil {
		slog.Error("failed to unmarshal incoming promotion", "error", err)
		return
	}

	payload, ok := envelope.Payload.(*events.EventEnvelope_NewPromotion)
	if !ok {
		return
	}

	newPromo := payload.NewPromotion

	// TODO: adicionar validacao chave assimetrica
	if newPromo.Category == "" || newPromo.Description == "" {
		slog.Warn("promoção rejeitada por falta de dados", "id", newPromo.PromotionId)
		return
	}

	slog.Info("promocao checked",
		"promotion_id", newPromo.PromotionId,
		"category", newPromo.Category,
		"description", newPromo.Description,
	)

	publishedEvent := &events.PromotionPublishedEvent{
		PromotionId: newPromo.PromotionId,
		Category:    newPromo.Category,
		Description: newPromo.Description,
	}

	outEnvelope := &events.EventEnvelope{
		Timestamp: timestamppb.Now(),
		Payload:   &events.EventEnvelope_PromotionPublished{PromotionPublished: publishedEvent},
	}

	outBody, err := proto.Marshal(outEnvelope)
	if err != nil {
		slog.Error("failed to marshal approved promotion", "error", err)
		return
	}

	// promocao.publicada
	if err := mq.PublishEvent(ch, ex.KeyPromotionPublished, outBody); err != nil {
		slog.Error("failed to dispatch notification to gateway",
			"category", newPromo.Category,
			"error", err,
		)
	} else {
		slog.Info("dispatched",
			"routing-key", ex.KeyPromotionPublished,
			"description", newPromo.Description,
		)
	}
}
