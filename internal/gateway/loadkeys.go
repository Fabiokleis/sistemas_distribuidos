package gateway

import (
	"crypto/rsa"
	"log"
	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
)

var (
	privateKey     *rsa.PrivateKey
	promocaoPubKey *rsa.PublicKey
)

func (g *Gateway) LoadKeys() {
	var err error
	privateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Gateway + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}

	promocaoPubKey, err = crypto.LoadPublicKey(crypto.GetKeyPath(ex.Promocao + ex.PublicKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa public key: %v", err)
	}
}
