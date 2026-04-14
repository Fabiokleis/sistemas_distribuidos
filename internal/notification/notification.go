package notification

import (
	"log"
	"log/slog"

	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	log.Println("[*] ms notification started ...")

	msgs, err := mq.SetupConsumer(ch, ex.KeyPromotionPublished, ex.KeyHotDeal) // promocao.publicada promocao.destaque
	if err != nil {
		log.Fatalf("failed to setup consumer: %v", err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			processNotification(ch, d)
		}
	}()

	<-forever
}

func processNotification(ch *amqp.Channel, d amqp.Delivery) {
	var envelope events.EventEnvelope
	if err := proto.Unmarshal(d.Body, &envelope); err != nil {
		slog.Error("failed to unmarshal message in notification", "error", err)
		return
	}

	payload, ok := envelope.Payload.(*events.EventEnvelope_PromotionPublished)
	if !ok {
		return
	}

	promo := payload.PromotionPublished

	if d.RoutingKey == ex.KeyHotDeal {
		promo.Description = "**HOT DEAL**: " + promo.Description
	}

	outEnvelope := &events.EventEnvelope{
		Timestamp: envelope.Timestamp,
		Payload:   &events.EventEnvelope_PromotionPublished{PromotionPublished: promo},
	}

	bodyBytes, err := proto.Marshal(outEnvelope)
	if err != nil {
		slog.Error("failed to marshal outbound notification", "error", err)
		return
	}

	clientRoutingKey := ex.KeyNotificationPrefix + promo.Category

	if err := mq.PublishEvent(ch, clientRoutingKey, bodyBytes); err != nil {
		slog.Error("failed to dispatch notification to client",
			"category", promo.Category,
			"error", err,
		)
	} else {
		slog.Info("dispatched",
			"routing-key", clientRoutingKey,
			"description", promo.Description,
		)
	}
}
