package gateway

import (
	"promocao/internal/models/proto/events"
	"sync"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Gateway struct {
	promos map[string]*events.PromotionPublishedEvent
	mu     sync.RWMutex
	ch     *amqp.Channel
}

type GatewayRouter interface {
	Serve(g GatewayService)
}

type GatewayService interface {
	GetPromotion(id string) Promotion
	ListPromotions() []Promotion
	SetupPublicationConsumer()
	HandleVote()
	PublishPromotion(category string, description string)
}

type Promotion struct {
	Id          string `json:"id"`
	Category    string `json:"category" validate:"required,min=3,max=50"`
	Description string `json:"description" validate:"required,min=1,max=255"`
}

type GatewayController struct {
	Validate *validator.Validate
	Service  GatewayService
}

type Router struct {
	Controller *GatewayController
}
