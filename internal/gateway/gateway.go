package gateway

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"github.com/google/uuid"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func New() Gateway {
	return Gateway{
		promos:     make(map[string]*events.PromotionPublishedEvent),
		mu:         sync.RWMutex{},
		listener:   nil,
		sseClients: make(map[string]*SSEClient),
		interests:  make(map[string][]string),
		sseMu:      sync.RWMutex{},
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

	g.listener = &Listener{
		ctx:           context.Background(),
		subscriptions: make(map[string]context.CancelFunc),
		mu:            sync.RWMutex{},
		ch:            channel,
	}

	defer channel.Close()

	g.LoadKeys()
	go g.SetupPublicationConsumer()
	go g.SetupHotDealConsumer()

	log.Println("[*] ms-gateway started ...")

	var forever chan struct{}

	<-forever
}

func (g *Gateway) RegisterSSEClient(clientID string) *SSEClient {
	client := &SSEClient{id: clientID, ch: make(chan SSEEvent, 16)}
	g.sseMu.Lock()
	g.sseClients[clientID] = client
	g.sseMu.Unlock()
	return client
}

func (g *Gateway) UnregisterSSEClient(clientID string) {
	g.sseMu.Lock()
	delete(g.sseClients, clientID)
	for category, clients := range g.interests {
		g.interests[category] = removeString(clients, clientID)
		if len(g.interests[category]) == 0 {
			delete(g.interests, category)
			g.listener.Unsubscribe(ex.KeyNotificationPrefix + category)
		}
	}
	g.sseMu.Unlock()
}

func (g *Gateway) SubscribeConsumer(clientID string, category string) error {
	routingKey := ex.KeyNotificationPrefix + category

	g.sseMu.Lock()
	g.interests[category] = appendUnique(g.interests[category], clientID)
	isFirst := len(g.interests[category]) == 1
	g.sseMu.Unlock()

	if !isFirst {
		return nil
	}

	// 1 consumer for all clients
	return g.listener.Consume(routingKey, func(topic Topic, event *events.EventEnvelope) {
		p, ok := event.Payload.(*events.EventEnvelope_PromotionPublished)
		if !ok {
			return
		}

		payloadRaw, err := ex.FormatPayloadToJSON(p.PromotionPublished)
		if err != nil {
			slog.Error("failed to format payload", "err", err)
			return
		}

		ex.PrintFormatPayload(topic.Exchange, topic.RoutingKey, event.Timestamp.AsTime().Format("15:04:05"), string(payloadRaw))

		g.sseMu.RLock()
		clientIDs := g.interests[category]
		g.sseMu.RUnlock()

		sseEvent := SSEEvent{EventType: "promocao.categoria", Data: payloadRaw}
		for _, cid := range clientIDs {
			g.sseMu.RLock()
			client, exists := g.sseClients[cid]
			g.sseMu.RUnlock()
			if exists {
				select {
				case client.ch <- sseEvent:
				default:
				}
			}
		}
	})
}

func (g *Gateway) UnsubscribeConsumer(clientID string, category string) error {
	g.sseMu.Lock()
	g.interests[category] = removeString(g.interests[category], clientID)
	isEmpty := len(g.interests[category]) == 0
	if isEmpty {
		delete(g.interests, category)
	}
	g.sseMu.Unlock()

	if isEmpty {
		return g.listener.Unsubscribe(ex.KeyNotificationPrefix + category)
	}
	return nil
}

func (g *Gateway) SetupHotDealConsumer() {
	g.listener.Consume(ex.KeyNotificationHotDeal, func(topic Topic, event *events.EventEnvelope) {
		p, ok := event.Payload.(*events.EventEnvelope_PromotionPublished)
		if !ok {
			return
		}

		payloadRaw, err := ex.FormatPayloadToJSON(p.PromotionPublished)
		if err != nil {
			slog.Error("failed to format hotdeal payload", "err", err)
			return
		}

		ex.PrintFormatPayload(topic.Exchange, topic.RoutingKey, event.Timestamp.AsTime().Format("15:04:05"), string(payloadRaw))

		sseEvent := SSEEvent{EventType: "promocao.destaque", Data: payloadRaw}

		g.sseMu.RLock()
		clients := make([]*SSEClient, 0, len(g.sseClients))
		for _, c := range g.sseClients {
			clients = append(clients, c)
		}
		g.sseMu.RUnlock()

		for _, c := range clients {
			select {
			case c.ch <- sseEvent:
			default:
			}
		}
	})
}

func (g *Gateway) SetupPublicationConsumer() {
	log.Println("[*] ms-gateway is waiting for notifications ...")

	g.listener.Consume(ex.KeyPromotionPublished, func(topic Topic, event *events.EventEnvelope) {

		if p, ok := event.Payload.(*events.EventEnvelope_PromotionPublished); ok {

			innerBytes, err := proto.Marshal(p.PromotionPublished)
			if err != nil {
				slog.Error("failed to marshal internal payload for verification", "error", err)
				return
			}

			err = crypto.VerifySignature(promocaoPubKey, innerBytes, event.Signature)
			if err != nil {
				slog.Error("INVALID SIGNATURE: gateway dropped untrusted message from ms promocao",
					"error", err,
					"producer_id", event.ProducerId)
				return
			}

			g.mu.Lock()
			g.promos[p.PromotionPublished.PromotionId] = p.PromotionPublished
			g.mu.Unlock()

			payloadRaw, err := ex.FormatPayloadToJSON(p.PromotionPublished)
			if err != nil {
				slog.Error("failed in formatting", "err", err)
				return
			}

			ex.PrintFormatPayload(
				topic.Exchange,
				topic.RoutingKey,
				event.Timestamp.AsTime().Format("15:04:05"),
				string(payloadRaw),
			)
		}
	})

}

func (g *Gateway) PublishPromotion(signature []byte, promo Promotion) error {
	event := &events.NewPromotionEvent{
		PromotionId: uuid.New().String(),
		Category:    promo.Category,
		Description: promo.Description,
		StoreEmail:  promo.StoreEmail,
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
		return err
	}

	if err := mq.PublishEvent(g.listener.ch, ex.KeyPromotionReceived, bodyBytes); err != nil {
		slog.Error("failed to publish promotion", "error", err)
		return err
	}
	return nil
}

func (g *Gateway) HandleVote(id string, num int32) error {
	g.mu.RLock()
	if len(g.promos) == 0 {
		g.mu.RUnlock()
		slog.Error("no promotion available to vote")
		return fmt.Errorf("no promotions available to vote")
	}

	selectedPromo := g.promos[id]
	g.mu.RUnlock()

	voteEvent := &events.VoteEvent{
		PromotionId: selectedPromo.PromotionId,
		VoteValue:   num,
		Category:    selectedPromo.Category,
		Description: selectedPromo.Description,
		StoreEmail:  selectedPromo.StoreEmail,
	}

	innerBytes, _ := proto.Marshal(voteEvent)

	signature, err := crypto.SignPayload(privateKey, innerBytes)
	if err != nil {
		slog.Error("failed to sign payload", "error", err)
		return err
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
		return err
	}

	err = mq.PublishEvent(g.listener.ch, ex.KeyPromotionVote, body)

	if err != nil {
		slog.Error("failed to dispatch notification vote",
			"routing-key", ex.KeyPromotionVote,
			"error", err,
		)
		return err
	} else {
		slog.Info("vote successfully sent for promotion",

			"promotion_id", id,
			"rounting-key", ex.KeyPromotionVote,
			"vote-value", num,
			"description", selectedPromo.Description,
		)
		return nil
	}
}

func removeString(slice []string, s string) []string {
	result := slice[:0]
	for _, v := range slice {
		if v != s {
			result = append(result, v)
		}
	}
	return result
}

func appendUnique(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}
