package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"promocao/internal/models/proto/events"
	mq "promocao/internal/rabbitmq"

	"google.golang.org/protobuf/proto"
)

func (l *Listener) Consume(key string, handler HandleEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.subscriptions[key]; exists {
		return nil
	}

	name, err := mq.Subscribe(l.ch, key)
	if err != nil {
		return err
	}

	msgs, err := mq.ConsumeEvents(l.ch, name)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(l.ctx)
	l.subscriptions[key] = cancel

	go func() {
		slog.Info("consumer started", "key", key)
		for {
			select {
			case <-ctx.Done():
				slog.Info("consumer stopped", "key", key)
				return
			case e, ok := <-msgs:
				if !ok {
					return
				}
				var envelope events.EventEnvelope
				if err := proto.Unmarshal(e.Body, &envelope); err != nil {
					slog.Error("failed to unmarshal event", "error", err)
					continue
				}
				go handler(Topic{Exchange: e.Exchange, RoutingKey: e.RoutingKey}, &envelope)
			}
		}
	}()

	return nil
}

func (l *Listener) Unsubscribe(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	consumer, exists := l.subscriptions[key]
	if !exists {
		return fmt.Errorf("no active consumer for key: %s", key)
	}

	consumer() // cancel ctx
	delete(l.subscriptions, key)
	return nil
}
