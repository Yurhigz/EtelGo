package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

//type pattern Adapter for Kafka franz-go
// The idea is to wrap franz-go's kgo.Record into our own Message type
// to decouple our application logic from the underlying Kafka library.
// The basic layout is within consumer.go, all the aspects of franz-go are wrapped here.

func FromKafkaFranz(record *kgo.Record) *Message {
	return &Message{
		Key:       record.Key,
		Value:     record.Value,
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		Headers: func() map[string]string {
			headers := make(map[string]string)
			for _, h := range record.Headers {
				headers[h.Key] = string(h.Value)
			}
			return headers
		}(),
	}
}

type KafkaConsumer struct {
	client   *kgo.Client
	messages chan *Message
	errors   chan error
	// Potentially other fields for configuration, state, etc.
}

func (kc *KafkaConsumer) Start(ctx context.Context) error {
	fetches := kc.client.PollFetches(ctx)

	errs := fetches.Errors() 

	if len(errs) > 0 {
		for _, err := range errs {
			kc.errors <- err
		}
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
