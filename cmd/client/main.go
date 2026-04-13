package main

import (
	"fmt"
	"log"
	"os"

	tablewriter "github.com/jedib0t/go-pretty/v6/table"
	amqp "github.com/rabbitmq/amqp091-go"
	"promocao/internal/rabbitmq"
)

func main() {
	ConsumeAndDisplay()
}

// ConsumeAndDisplay executa o consumo das notificações e exibe no terminal
func ConsumeAndDisplay() {
	if err := rabbitmq.Init(); err != nil {
		log.Fatalf("failed to connect rabbitmq error: %v", err)
	}
	defer rabbitmq.Connection.Close()
	conn := rabbitmq.Connection

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel error: %v", err)
	}
	defer ch.Close()

	exchangeName := "promocao"
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	// Categorias de interesse hardcoded
	categories := []string{"livro", "jogo", "destaque"}
	queueName := ""
	q, err := ch.QueueDeclare(
		"", // nome vazio = fila exclusiva auto-gerada
		true,
		false,
		true, // exclusiva
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}
	queueName = q.Name

	// Bind para cada categoria
	for _, cat := range categories {
		key := "promocao." + cat
		if err := ch.QueueBind(
			queueName,
			key,
			exchangeName,
			false,
			nil,
		); err != nil {
			log.Fatalf("failed to bind queue to key %s: %v", key, err)
		}
	}

	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to register consumer: %v", err)
	}

	table := tablewriter.NewWriter()
	table.SetOutputMirror(os.Stdout)
	table.AppendHeader(tablewriter.Row{"Categoria", "Mensagem"})

	log.Printf(" [*] Aguardando notificações de promoções (%v). Para sair, CTRL+C", categories)
	for d := range msgs {
		// Extrai categoria da routing key
		cat := d.RoutingKey
		if len(cat) > 9 {
			cat = cat[9:]
		}
		table.AppendRow(tablewriter.Row{cat, string(d.Body)})
		table.Render()
	}
	return
}


	fmt.Println("Hello, World!")

	if err := rabbitmq.Init(); err != nil {
		log.Fatalf("failed to connect rabbitmq error: %v", err)
	}
	defer rabbitmq.Connection.Close()
	conn := rabbitmq.Connection

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel error: %v", err)
	}

	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		true,    // durability
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)

	if err != nil {
		log.Fatalf("failed to declare a queue error: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	if err != nil {
		log.Fatalf("failed to register consumer error: %v", err)
	}

	var forever chan struct{}

	go func() {
		table := tablewriter.NewWriter()
		table.SetOutputMirror(os.Stdout)
		table.AppendHeader(tablewriter.Row{"Mensagem"})
		for d := range msgs {
			table.AppendRow(tablewriter.Row{string(d.Body)})
			table.Render()
		}
	}()

	log.Printf(" [*] waiting for messages. to exit press CTRL+C")
	<-forever
}
