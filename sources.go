package main

import (
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Implémentation d'un lecteur Kafka avec la librairie Franz
// Il faudra mettre en place un worker pool pour paraléliser au maximum

func (ic *InputConfig) KafkaReader(logger *slog.Logger) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(ic.Brokers...),
		kgo.ConsumerGroup(ic.ConsumerGroup),
		kgo.ConsumeTopics(ic.Topic),
	)
	if err != nil {
		logger.Error("failed to create Kafka client", "error", err)
	}

	defer cl.Close()
	panic("Not implemented")
}
