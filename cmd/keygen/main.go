package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	outDir := flag.String("out", "keys", "keys directory")
	flag.Parse()

	services := []string{"gateway", "promocao", "ranking"}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("failed to create directories %s: %v", *outDir, err)
	}

	fmt.Printf("creating rsa keys in: '%s'\n\n", *outDir)

	for _, srv := range services {
		fmt.Printf("generating keys for: %s...\n", srv)

		// Gera a chave RSA
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			log.Fatalf("failed to generate keys for %s: %v", srv, err)
		}

		privPath := filepath.Join(*outDir, fmt.Sprintf("%s_private.pem", srv))
		pubPath := filepath.Join(*outDir, fmt.Sprintf("%s_public.pem", srv))

		privFile, err := os.Create(privPath)
		if err != nil {
			log.Fatalf("failed to create file %s: %v", privPath, err)
		}
		privBytes := x509.MarshalPKCS1PrivateKey(privateKey)
		pem.Encode(privFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes})
		privFile.Close()

		// Salva Chave Pública
		pubFile, err := os.Create(pubPath)
		if err != nil {
			log.Fatalf("failed to create file %s: %v", pubPath, err)
		}
		pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
		if err != nil {
			log.Fatalf("failed to marshal public key for %s: %v", srv, err)
		}
		pem.Encode(pubFile, &pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes})
		pubFile.Close()

		fmt.Printf("saved %s %s\n", privPath, pubPath)
	}
}
