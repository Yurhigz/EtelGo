package consumer

import (
	"context"
	"log/slog"

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
	logger   *slog.Logger
	messages chan *Message
	errors   chan error
	// Potentially other fields for configuration, state, etc.
}

func NewKafkaConsumer(cfg *InputConfig, logger *slog.Logger) (*KafkaConsumer, error) {
	logger.Info("Creating new Kafka consumer", " brokers", cfg.Brokers, "topic", cfg.Topic, "group", cfg.ConsumerGroup)

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
	}

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		logger.Error("failed to create Kafka client", "error", err)
		return nil, err
	}

	return &KafkaConsumer{
		client:   client,
		logger:   logger,
		messages: make(chan *Message),
		errors:   make(chan error),
	}, nil
}

func (kc *KafkaConsumer) Start(ctx context.Context) error {
	for {
		fetches := kc.client.PollFetches(ctx)
	}

	errs := fetches.Errors()

	if len(errs) > 0 {
		for _, err := range errs {
			kc.errors <- err
		}
	}
	records := fetches.Records()
	for _, record := range records {
		kc.messages <- FromKafkaFranz(record)
	}

	return nil
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
