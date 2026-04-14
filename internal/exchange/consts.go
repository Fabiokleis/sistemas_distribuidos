package exchange

const Name = "promocao"

var Categories = []string{"livro", "jogo"}

const (
	PrivateKeySuffix = "_private.pem"
	PublicKeySuffix  = "_public.pem"
	Gateway          = "gateway"
	Promocao         = "promocao"
	Ranking          = "ranking"

	HotDealThreshold = 3
	ContentType      = "application/x-protobuf"
	// published by gateway
	KeyPromotionReceived = "promocao.recebida"
	KeyPromotionVote     = "promocao.voto"

	// published by ms promotion
	KeyPromotionPublished = "promocao.publicada"

	// published by ms ranking (for notifications)
	KeyNotificationPrefix = "promocao."
	KeyHotDeal            = "promocao.destaque"
)
