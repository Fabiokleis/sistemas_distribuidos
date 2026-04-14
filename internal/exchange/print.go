package exchange

import (
	"encoding/json"
	"errors"
	"fmt"
	"promocao/internal/models/proto/events"

	"google.golang.org/protobuf/proto"
)

const (
	Reset = "\033[0m"
	Gray  = "\033[2m"
	Blue  = "\033[1;34m"
	Cyan  = "\033[0;36m"
	Green = "\033[0;32m"
)

func FormatPayloadToJSON(payload proto.Message) ([]byte, error) {
	if payload == nil {
		return nil, errors.New("nil payload")
	}

	displayData := struct {
		PromotionId string `json:"promotion_id,omitempty"`
		Category    string `json:"category,omitempty"`
		Description string `json:"description,omitempty"`
		VoteValue   int32  `json:"vote_value,omitempty"`
	}{}

	switch v := payload.(type) {
	case *events.PromotionPublishedEvent:
		displayData.PromotionId = v.GetPromotionId()
		displayData.Category = v.GetCategory()
		displayData.Description = v.GetDescription()
	case *events.VoteEvent:
		displayData.PromotionId = v.GetPromotionId()
		displayData.VoteValue = v.GetVoteValue()
		displayData.Category = v.GetCategory()
		displayData.Description = v.GetDescription()
	case *events.NewPromotionEvent:
		displayData.PromotionId = v.GetPromotionId()
		displayData.Category = v.GetCategory()
		displayData.Description = v.GetDescription()
	}

	return json.Marshal(displayData)
}

func PrintFormatPayload(exchange string, rountingKey string, ts string, payload string) {
	fmt.Printf("%s[%s]%s %s|%s %sEXCHANGE:%s %-8s %s|%s %sKEY:%s %-18s %s|%s %sPAYLOAD:%s %s\n",
		Gray, ts, Reset, // [Timestamp]
		Gray, Reset,
		Blue, Reset, exchange,
		Gray, Reset,
		Cyan, Reset, rountingKey,
		Gray, Reset,
		Green, Reset, payload,
	)
}
