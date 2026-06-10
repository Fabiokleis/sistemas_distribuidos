package gateway

import (
	"context"
	"promocao/internal/models/proto/events"
	"sync"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Promotion struct {
	Id          string `json:"id"`
	Category    string `json:"category" validate:"required,min=3,max=50"`
	Description string `json:"description" validate:"required,min=1,max=255"`
	StoreEmail  string `json:"store_email,omitempty" validate:"omitempty,email"`
}

type Topic struct {
	QueueName  string
	Exchange   string
	RoutingKey string
}

type SSEEvent struct {
	EventType string
	Data      []byte
}

type SSEClient struct {
	id string
	ch chan SSEEvent
}

type HandleEvent func(topic Topic, event *events.EventEnvelope)

type Listener struct {
	ctx           context.Context
	subscriptions map[string]context.CancelFunc
	mu            sync.RWMutex
	ch            *amqp.Channel
}

type Gateway struct {
	promos     map[string]*events.PromotionPublishedEvent
	mu         sync.RWMutex
	listener   *Listener
	sseClients map[string]*SSEClient
	interests  map[string][]string
	sseMu      sync.RWMutex
}

type GatewayRouter interface {
	Serve(g GatewayService)
}

type GatewayService interface {
	GetPromotion(id string) Promotion
	ListPromotions() []Promotion
	RegisterSSEClient(clientID string) *SSEClient
	UnregisterSSEClient(clientID string)
	SubscribeConsumer(clientID string, category string) error
	UnsubscribeConsumer(clientID string, category string) error
	HandleVote(id string, num int32) error
	PublishPromotion(signature []byte, promo Promotion) error
}

type GatewayController struct {
	Validate *validator.Validate
	Service  GatewayService
}

type Router struct {
	Controller *GatewayController
}
