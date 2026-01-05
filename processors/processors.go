package processors

import (
	"context"
	"etelgo/config"
	"etelgo/consumer"
	"log/slog"
)

// Need to add how to handle different type of consumer
// Agnostic consumer to prevent rewriting code as soon as library or inputs are added
type Orchestrator struct {
	config   *config.Config
	consumer *consumer.KafkaConsumer
	logger   *slog.Logger
	//metrics to be added to enable telemetry and observability
}

func NewOrchestrator(configPath string, logger *slog.Logger) (*Orchestrator, error) {
	cfg, err := config.LoadConfig(configPath, logger)
	if err != nil {
		logger.Error("error loading config")
		return nil, err
	}

	cons, err := consumer.NewKafkaConsumer(&cfg.Input, logger)
	if err != nil {
		logger.Error("error creating a new Kafka Consumer")
		return nil, err
	}

	return &Orchestrator{
		cfg,
		cons,
		logger,
	}, nil
}

func (o *Orchestrator) Run(ctx context.Context, dryRun bool) error {
	//start consumer

	//Messages loop

	//Apply processors

	//Send to output

	//Metrics and Errors handling
	panic("not implemented yet")
}
