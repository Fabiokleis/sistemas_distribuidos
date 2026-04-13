package rabbitmq

import "testing"

func TestInitRabbitMQ(t *testing.T) {
	err := Init()
	if err != nil {
		t.Fatalf("Erro ao conectar no RabbitMQ: %v", err)
	}
	if Connection == nil {
		t.Fatal("Connection retornou nil")
	}
	Connection.Close()
}
