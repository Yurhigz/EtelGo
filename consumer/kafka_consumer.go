package consumer

import (
	"context"
	"etelgo/config"
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

func NewKafkaConsumer(cfg *config.InputConfig, logger *slog.Logger) (*KafkaConsumer, error) {
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

func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.logger.Info("Starting Kafka consumer")

	go kc.pollMessages(ctx)
}

// Poll messages from Kafka and send them to the messages channel, multiple select patterns to handle context cancellation
func (kc *KafkaConsumer) pollMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			kc.logger.Info("Kafka consumer context done, stopping polling")
			return
		default:
			fetches := kc.client.PollFetches(ctx)

			errs := fetches.Errors()
			if len(errs) > 0 {
				for _, err := range errs {
					kc.logger.Error("Error fetching messages", "error", err.Err)
					select {
					case kc.errors <- err.Err:
					case <-ctx.Done():
						return
					}
				}
			}

			fetches.EachRecord(func(record *kgo.Record) {
				msg := FromKafkaFranz(record)

				deserializer := NewDeserializer("json") // For now, hardcoded to JSON
				valueFields, err := deserializer.Deserialize(msg.Value)
				if err != nil {
					kc.logger.Error("failed to deserialize message value", "error", err)
					select {
					case kc.errors <- err:
					case <-ctx.Done():
						return
					}
				} else {
					msg.ValueFields = valueFields
				}

				select {
				case kc.messages <- msg:
				case <-ctx.Done():
					return
				}
			})
		}
	}
}

func (kc *KafkaConsumer) Messages() <-chan *Message {
	return kc.messages
}

func (kc *KafkaConsumer) Errors() <-chan error {
	return kc.errors
}

func (kc *KafkaConsumer) Close() error {
	// Wrapper autour de kc.client.Close()
	panic("unimplemented")
}
