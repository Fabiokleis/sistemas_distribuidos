package exchange

var Categories = []string{"livro", "jogo"}

const (
	Name             = "promocao"
	PrivateKeySuffix = "_private.pem"
	PublicKeySuffix  = "_public.pem"
	Gateway          = "gateway"
	Promocao         = "promocao"
	Ranking          = "ranking"
	Store            = "store"

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

	// published by ms notificacao (for gateway SSE broadcast)
	KeyNotificationHotDeal = "notificacao.hotdeal"
)

type DaemonService interface {
	Run()
	LoadKeys()
}
