package gateway

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"

	"promocao/internal/exchange"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"github.com/google/uuid"
	"github.com/manifoldco/promptui"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	promos = make(map[string]*events.PromotionPublishedEvent)
	mu     sync.RWMutex
)

func Run() {
	ch, err := mq.Connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	go setupPublicationConsumer(ch)

	runMenu(ch)
}

func setupPublicationConsumer(ch *amqp.Channel) {
	log.Println("[*] ms-gateway started ...")

	msgs, err := mq.SetupConsumer(ch, ex.KeyPromotionPublished) // promocao.publicada
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
			mu.Lock()
			promos[p.PromotionPublished.PromotionId] = p.PromotionPublished
			mu.Unlock()

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

func runMenu(ch *amqp.Channel) {
	for {
		prompt := promptui.Select{
			Label: "GATEWAY - Select an action",
			Items: []string{
				"1. Register New Promotion",
				"2. List Validated Promotions",
				"3. Vote on a Promotion",
				"4. Exit",
			},
		}

		_, result, err := prompt.Run()
		if err != nil {
			return
		}

		switch result {
		case "1. Register New Promotion":
			handleNewPromotion(ch)
		case "2. List Validated Promotions":
			handleListPromotions()
		case "3. Vote on a Promotion":
			handleVote(ch)
		case "4. Exit":
			fmt.Println("Exiting...")
			os.Exit(0)
		}
	}
}

func handleNewPromotion(ch *amqp.Channel) {
	promptCat := promptui.Select{
		Label: "Category",
		Items: exchange.Categories,
	}
	_, cat, _ := promptCat.Run()

	promptDesc := promptui.Prompt{Label: "Promotion description"}
	desc, _ := promptDesc.Run()

	publishPromotion(ch, cat, desc)
	fmt.Println("Promotion sent for validation!")
}

func handleListPromotions() {
	mu.RLock()
	defer mu.RUnlock()

	if len(promos) == 0 {
		fmt.Println("\n[!] No validated promotions available at the moment.")
		return
	}

	fmt.Println("\n--- VALIDATED PROMOTIONS ---")
	for id, p := range promos {
		fmt.Printf("ID: %s | Category: %-10s | Description: %s\n", id, p.Category, p.Description)
	}
	fmt.Println("----------------------------")
}

func publishPromotion(ch *amqp.Channel, category string, description string) {
	event := &events.NewPromotionEvent{
		PromotionId: uuid.New().String(),
		Category:    category,
		Description: description,
	}

	wrap := &events.EventEnvelope{
		Timestamp: timestamppb.Now(),
		Payload:   &events.EventEnvelope_NewPromotion{NewPromotion: event},
	}

	bodyBytes, err := proto.Marshal(wrap)
	if err != nil {
		slog.Error("failed to marshal new promotion event", "error", err)
		return
	}

	if err := mq.PublishEvent(ch, ex.KeyPromotionReceived, bodyBytes); err != nil {
		slog.Error("failed to publish promotion", "error", err)
	}
}

func handleVote(ch *amqp.Channel) {
	mu.RLock()

	if len(promos) == 0 {
		fmt.Println("[!] No promotions available to vote.")
		mu.RUnlock()
		return
	}

	var options []string
	var ids []string
	for id, p := range promos {
		options = append(options, fmt.Sprintf("[%s] %s", p.Category, p.Description))
		ids = append(ids, id)
	}
	mu.RUnlock()

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

	mu.RLock()
	selectedPromo := promos[selectedID]
	mu.RUnlock()

	voteEvent := &events.VoteEvent{
		PromotionId: selectedID,
		VoteValue:   voteValue,
		Category:    selectedPromo.Category,
		Description: selectedPromo.Description,
	}

	envelope := &events.EventEnvelope{
		Timestamp: timestamppb.Now(),
		Payload:   &events.EventEnvelope_Vote{Vote: voteEvent},
	}

	body, err := proto.Marshal(envelope)
	if err != nil {
		slog.Error("failed to marshal vote event", "error", err)
		return
	}

	err = mq.PublishEvent(ch, ex.KeyPromotionVote, body)

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
