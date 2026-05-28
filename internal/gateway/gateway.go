package gateway

import (
	"fmt"
	"log"
	"log/slog"
	"sync"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"github.com/google/uuid"
	"github.com/manifoldco/promptui"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func New() Gateway {
	return Gateway{
		promos: make(map[string]*events.PromotionPublishedEvent),
		mu:     sync.RWMutex{},
		ch:     nil,
	}
}

func (g *Gateway) GetPromotion(id string) Promotion {
	g.mu.RLock()
	selectedPromo := g.promos[id]
	promo := Promotion{
		Id:          selectedPromo.PromotionId,
		Category:    selectedPromo.Category,
		Description: selectedPromo.Description,
	}
	g.mu.RUnlock()
	return promo
}

func (g *Gateway) ListPromotions() []Promotion {
	promotions := make([]Promotion, 0, len(g.promos))
	for _, p := range g.promos {
		promotions = append(
			promotions,
			Promotion{
				Id:          p.PromotionId,
				Category:    p.Category,
				Description: p.Description,
			},
		)

	}
	return promotions
}

func (g *Gateway) Run() {
	channel, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	g.ch = channel
	defer channel.Close()

	g.LoadKeys()
	go g.SetupPublicationConsumer() // start receiving events

	log.Println("[*] ms-gateway started ...")

	var forever chan struct{}

	<-forever
}

func (g *Gateway) SetupPublicationConsumer() {

	msgs, err := mq.SetupConsumer(g.ch, ex.KeyPromotionPublished) // promocao.publicada
	if err != nil {
		log.Fatalf("failed to setup callback consumer %v", err)
		return
	}

	log.Println("[*] ms-gateway is waiting for notifications ...")

	for d := range msgs {
		var envelope events.EventEnvelope
		if err := proto.Unmarshal(d.Body, &envelope); err != nil {
			slog.Error("failed to unmarshal published event", "error", err)
			continue
		}

		if p, ok := envelope.Payload.(*events.EventEnvelope_PromotionPublished); ok {

			innerBytes, err := proto.Marshal(p.PromotionPublished)
			if err != nil {
				slog.Error("failed to marshal internal payload for verification", "error", err)
				continue
			}

			err = crypto.VerifySignature(promocaoPubKey, innerBytes, envelope.Signature)
			if err != nil {
				slog.Error("INVALID SIGNATURE: gateway dropped untrusted message from ms promocao",
					"error", err,
					"producer_id", envelope.ProducerId)
				continue
			}

			g.mu.Lock()
			g.promos[p.PromotionPublished.PromotionId] = p.PromotionPublished
			g.mu.Unlock()

			payloadRaw, err := ex.FormatPayloadToJSON(p.PromotionPublished)
			if err != nil {
				slog.Error("failed in formatting", "err", err)
				continue
			}

			ex.PrintFormatPayload(
				d.Exchange,
				d.RoutingKey,
				envelope.Timestamp.AsTime().Format("15:04:05"),
				string(payloadRaw),
			)
		}
	}
}

func (g *Gateway) PublishPromotion(category string, description string) {
	event := &events.NewPromotionEvent{
		PromotionId: uuid.New().String(),
		Category:    category,
		Description: description,
	}

	innerBytes, _ := proto.Marshal(event)

	signature, err := crypto.SignPayload(privateKey, innerBytes)
	if err != nil {
		slog.Error("failed to sign payload", "error", err)
		return
	}

	wrap := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Gateway,
		Signature:  signature,
		Payload:    &events.EventEnvelope_NewPromotion{NewPromotion: event},
	}

	bodyBytes, err := proto.Marshal(wrap)
	if err != nil {
		slog.Error("failed to marshal new promotion event", "error", err)
		return
	}

	if err := mq.PublishEvent(g.ch, ex.KeyPromotionReceived, bodyBytes); err != nil {
		slog.Error("failed to publish promotion", "error", err)
	}
}

func (g *Gateway) HandleVote() {
	g.mu.RLock()

	if len(g.promos) == 0 {
		fmt.Println("[!] No promotions available to vote.")
		g.mu.RUnlock()
		return
	}

	var options []string
	var ids []string
	for id, p := range g.promos {
		options = append(options, fmt.Sprintf("[%s] %s", p.Category, p.Description))
		ids = append(ids, id)
	}
	g.mu.RUnlock()

	promptSelect := promptui.Select{
		Label: "Select the Promotion to vote on",
		Items: ids,
	}

	idx, _, _ := promptSelect.Run()
	selectedID := ids[idx]

	promptVote := promptui.Select{
		Label: "What is your vote?",
		Items: []string{"Positive (+1)", "Negative (-1)"},
	}
	_, voteRes, _ := promptVote.Run()

	voteValue := int32(1)
	if voteRes == "Negative (-1)" {
		voteValue = -1
	}

	g.mu.RLock()
	selectedPromo := g.promos[selectedID]
	g.mu.RUnlock()

	voteEvent := &events.VoteEvent{
		PromotionId: selectedID,
		VoteValue:   voteValue,
		Category:    selectedPromo.Category,
		Description: selectedPromo.Description,
	}

	innerBytes, _ := proto.Marshal(voteEvent)

	signature, err := crypto.SignPayload(privateKey, innerBytes)
	if err != nil {
		slog.Error("failed to sign payload", "error", err)
		return
	}

	envelope := &events.EventEnvelope{
		Timestamp:  timestamppb.Now(),
		ProducerId: ex.Gateway,
		Signature:  signature,
		Payload:    &events.EventEnvelope_Vote{Vote: voteEvent},
	}

	body, err := proto.Marshal(envelope)
	if err != nil {
		slog.Error("failed to marshal vote event", "error", err)
		return
	}

	err = mq.PublishEvent(g.ch, ex.KeyPromotionVote, body)

	if err != nil {
		slog.Error("failed to dispatch notification vote",
			"routing-key", ex.KeyPromotionVote,
			"error", err,
		)
	} else {
		slog.Info("vote successfully sent for promotion",

			"promotion_id", selectedID,
			"rounting-key", ex.KeyPromotionVote,
			"vote-value", voteValue,
			"description", selectedPromo.Description,
		)
	}
}
