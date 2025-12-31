package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Usage de wrapper franz-go pour implémenter un consommateur Kafka
// Pattern adapter

func FromKafkaFranz(record *kgo.Record) *Message {
	return &Message{
		Key:       record.Key,
		Value:     record.Value,
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		// Headers:   record.Headers, // À implémenter si nécessaire
	}
}

type KafkaConsumer struct {
	client   *kgo.Client
	messages chan *Message
	errors   chan error
	// ... autres champs
}

func (kc *KafkaConsumer) Start(ctx context.Context) error {
	// Utiliser kc.client.PollFetches() et envoyer dans kc.messages
}

func (kc *KafkaConsumer) Messages() <-chan *Message {
	return kc.messages
}

func (kc *KafkaConsumer) Errors() <-chan error {
	return kc.errors
}

func (kc *KafkaConsumer) Close() error {
	// Wrapper autour de kc.client.Close()
}
