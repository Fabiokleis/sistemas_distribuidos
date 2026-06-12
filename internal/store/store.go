package store

import (
	"bytes"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"promocao/internal/crypto"
	ex "promocao/internal/exchange"
	"promocao/internal/models/proto/events"
	"strings"

	"github.com/google/uuid"
	"github.com/manifoldco/promptui"
	"google.golang.org/protobuf/proto"
)

const defaultGatewayURL = "http://localhost:8123/promotions"

var (
	privateKey *rsa.PrivateKey
)

func loadKeys() {
	var err error
	privateKey, err = crypto.LoadPrivateKey(crypto.GetKeyPath(ex.Store + ex.PrivateKeySuffix))
	if err != nil {
		log.Fatalf("failed to load rsa private key: %v", err)
	}
}

func Run() {
	loadKeys()
	log.Println("[*] store started ...")
	prompt()
}

func prompt() {
	gatewayURL := os.Getenv("GATEWAY_URL")
	if gatewayURL == "" {
		gatewayURL = defaultGatewayURL
	}

	for {
		category, err := (&promptui.Prompt{
			Label: "Categoria do produto",
			Validate: func(s string) error {
				s = strings.TrimSpace(s)
				if len(s) < 3 {
					return fmt.Errorf("mínimo 3 caracteres")
				}
				if len(s) > 50 {
					return fmt.Errorf("máximo 50 caracteres")
				}
				return nil
			},
		}).Run()
		if err != nil {
			handlePromptErr(err)
			return
		}

		description, err := (&promptui.Prompt{
			Label: "Descrição da promoção",
			Validate: func(s string) error {
				s = strings.TrimSpace(s)
				if len(s) < 1 {
					return fmt.Errorf("descrição obrigatória")
				}
				if len(s) > 255 {
					return fmt.Errorf("máximo 255 caracteres")
				}
				return nil
			},
		}).Run()
		if err != nil {
			handlePromptErr(err)
			return
		}

		storeEmail, err := (&promptui.Prompt{
			Label: "E-mail da loja",
			Validate: func(s string) error {
				s = strings.TrimSpace(s)
				if !strings.Contains(s, "@") || !strings.Contains(s, ".") {
					return fmt.Errorf("e-mail inválido")
				}
				return nil
			},
		}).Run()
		if err != nil {
			handlePromptErr(err)
			return
		}

		fmt.Println()
		fmt.Printf("  Categoria  : %s\n", strings.TrimSpace(category))
		fmt.Printf("  Descrição  : %s\n", strings.TrimSpace(description))
		fmt.Printf("  E-mail     : %s\n", strings.TrimSpace(storeEmail))
		fmt.Println()

		idx, _, err := (&promptui.Select{
			Label: "Confirmar envio?",
			Items: []string{"Sim, enviar", "Corrigir dados", "Cancelar"},
		}).Run()
		if err != nil {
			handlePromptErr(err)
			return
		}
		switch idx {
		case 1:
			fmt.Println()
			continue
		case 2:
			fmt.Println("Operação cancelada.")
			return
		}

		promo := &events.NewPromotionEvent{
			PromotionId: uuid.New().String(),
			Category:    strings.TrimSpace(category),
			Description: strings.TrimSpace(description),
			StoreEmail:  strings.TrimSpace(storeEmail),
		}

		innerBytes, err := proto.Marshal(promo)
		if err != nil {
			log.Fatalf("failed to marshal promotion: %v", err)
		}

		signature, err := crypto.SignPayload(privateKey, innerBytes)
		if err != nil {
			log.Fatalf("failed to sign promotion: %v", err)
		}

		req, err := http.NewRequest(http.MethodPost, gatewayURL, bytes.NewReader(innerBytes))
		if err != nil {
			log.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", ex.ContentType)
		req.Header.Set("x-store-signature", base64.StdEncoding.EncodeToString(signature))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Erro ao conectar no gateway: %v\n", err)
		} else {
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusAccepted:
				fmt.Println("Promoção enviada com sucesso! Aguardando validação.")
			case http.StatusUnauthorized:
				fmt.Println("Assinatura rejeitada pelo gateway. Verifique as chaves.")
			case http.StatusBadRequest:
				fmt.Println("Dados inválidos (400). Verifique os campos.")
			case http.StatusUnprocessableEntity:
				fmt.Println("Erro no marshal do protobuf.")
			default:
				fmt.Printf("Resposta inesperada do gateway: %d\n", resp.StatusCode)
			}
		}

		fmt.Println()
		idx, _, err = (&promptui.Select{
			Label: "O que deseja fazer?",
			Items: []string{"Cadastrar outra promoção", "Sair"},
		}).Run()
		if err != nil || idx == 1 {
			return
		}
		fmt.Println()
	}
}

func handlePromptErr(err error) {
	if err == promptui.ErrInterrupt || err == promptui.ErrEOF {
		fmt.Println("\nInterrompido.")
	} else {
		log.Printf("prompt error: %v", err)
	}
}
