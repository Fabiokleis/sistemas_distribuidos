package notification

import (
	"crypto/rsa"
	"log"
	"log/slog"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var (
	promocaoPubKey *rsa.PublicKey
	rankingPubKey  *rsa.PublicKey
)

func loadKeys() {
	var err error
	promocaoPubKey, err = crypto.LoadPublicKey(crypto.GetKeyPath(ex.Promocao + ex.PublicKeySuffix))
	if err != nil {
		log.Fatalf("failed to load promocao public key: %v", err)
	}

	rankingPubKey, err = crypto.LoadPublicKey(crypto.GetKeyPath(ex.Ranking + ex.PublicKeySuffix))
	if err != nil {
		log.Fatalf("failed to load ranking public key: %v", err)
	}
}

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	loadKeys()
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

	var pubKeyToUse *rsa.PublicKey
	switch d.RoutingKey {
	case ex.KeyPromotionPublished:
		pubKeyToUse = promocaoPubKey
	case ex.KeyHotDeal:
		pubKeyToUse = rankingPubKey
	}

	innerBytes, _ := proto.Marshal(promo)
	err := crypto.VerifySignature(pubKeyToUse, innerBytes, envelope.Signature)
	if err != nil {
		slog.Error("INVALID SIGNATURE: ms-notificacao dropped untrusted message",
			"routing_key", d.RoutingKey,
			"error", err)
		return
	}

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
