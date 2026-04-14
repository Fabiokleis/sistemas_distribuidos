package exchange

const Name = "promocao"

var Categories = []string{"livro", "jogo"}

const (
	ContentType = "application/x-protobuf"
	// published by gateway
	KeyPromotionReceived = "promocao.recebida"
	KeyPromotionVote     = "promocao.voto"

	// published by ms promotion
	KeyPromotionPublished = "promocao.publicada"

	// published by ms ranking (for notifications)
	KeyNotificationPrefix = "promocao."
	KeyHotDeal            = "promocao.destaque"
)
