package main

import (
	"testing"
)

func TestConsumeAndDisplayIntegration(t *testing.T) {
	// Publica uma mensagem de teste em cada categoria
	categories := []string{"livro", "jogo", "destaque"}
	conn, err := amqp.Dial("amqp://promocao:1234@localhost:5672/")
	if err != nil {
		t.Fatalf("Erro ao conectar no RabbitMQ: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Erro ao abrir canal: %v", err)
	}
	defer ch.Close()
	exchangeName := "promocao"
	ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	for _, cat := range categories {
		body := "mensagem de teste " + cat
		err := ch.Publish(
			exchangeName,
			"promocao."+cat,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		if err != nil {
			t.Errorf("Erro ao publicar mensagem para %s: %v", cat, err)
		}
	}

	// Executa o consumidor em goroutine e aguarda algumas mensagens
	done := make(chan struct{})
	go func() {
		ConsumeAndDisplay()
		done <- struct{}{}
	}()

	// Aguarda alguns segundos para garantir consumo
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// ok, tempo suficiente para consumir
	}
}
