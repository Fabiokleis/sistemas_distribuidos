package client

import (
	"log"
	"log/slog"

	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"google.golang.org/protobuf/proto"
)

func Run() {

	conn := mq.Connection
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel error: %v", err)
	}
	defer ch.Close()

	var keys []string
	for _, cat := range ex.Categories {
		keys = append(keys, ex.KeyNotificationPrefix+cat) // promocao.{categoria}
	}
	keys = append(keys, ex.KeyHotDeal)

	log.Println("[*] client started ...")

	msgs, err := mq.SetupConsumer(ch, keys...)
	if err != nil {
		log.Fatalf("failed to setup consumer: %v", err)
	}

	var forever chan struct{}

	go func() {

		for d := range msgs {
			var envelope events.EventEnvelope
			if err := proto.Unmarshal(d.Body, &envelope); err != nil {
				slog.Error("error unmarshaling protobuf", "err", err)
				continue
			}

			var payloadRaw []byte
			var innerMsg proto.Message

			switch p := envelope.Payload.(type) {
			case *events.EventEnvelope_PromotionPublished:
				innerMsg = p.PromotionPublished
			case *events.EventEnvelope_Vote:
				innerMsg = p.Vote
			}

			if innerMsg != nil {
				if payloadRaw, err = ex.FormatPayloadToJSON(innerMsg); err != nil {
					slog.Error("failed in formatting", "err", err)
					continue
				}
			}

			ex.PrintFormatPayload(
				d.Exchange,
				d.RoutingKey,
				envelope.Timestamp.AsTime().Format("15:04:05"),
				string(payloadRaw),
			)
		}
	}()

	log.Printf(" [*] client waiting for messages. To exit press CTRL+C")
	<-forever
}
