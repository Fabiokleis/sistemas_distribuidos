package promocao

import (
	"crypto/rsa"
	"log"
	"log/slog"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	"promocao/internal/rabbitmq"
	mq "promocao/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	privateKey    *rsa.PrivateKey
	gatewayPubKey *rsa.PublicKey
)

func loadKeys() {
	var err error
	privateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Promocao + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	gatewayPubKey, err = crypto.LoadPublicKey(crypto.GetKeyPath(ex.Gateway + ex.PublicKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa public key: %v", err)
	}
}

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	loadKeys()

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

	innerBytes, err := proto.Marshal(newPromo)
	if err != nil {
		slog.Error("failed to marshal internal payload for verification", "error", err)
		return
	}

	err = crypto.VerifySignature(gatewayPubKey, innerBytes, envelope.Signature)
	if err != nil {
		slog.Error("INVALID SIGNATURE: ms promotion dropped untrusted message from gateway",
			"error", err,
			"promotion_id", newPromo.PromotionId)
		return
	}

	if newPromo.Category == "" || newPromo.Description == "" {
		slog.Warn("promotion dropped missing data", "id", newPromo.PromotionId)
		return
	}

	slog.Info("promocao verified and signed",
		"promotion_id", newPromo.PromotionId,
		"category", newPromo.Category,
		"description", newPromo.Description,
	)

	publishedEvent := &events.PromotionPublishedEvent{
		PromotionId: newPromo.PromotionId,
		Category:    newPromo.Category,
		Description: newPromo.Description,
	}

	outInnerBytes, _ := proto.Marshal(publishedEvent)
	signature, err := crypto.SignPayload(privateKey, outInnerBytes)
	if err != nil {
		slog.Error("failed to sign outgoing promotion", "error", err)
		return
	}

	outEnvelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Promocao,
		Signature:  signature,
		Payload:    &events.EventEnvelope_PromotionPublished{PromotionPublished: publishedEvent},
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
