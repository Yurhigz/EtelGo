package processors

import (
	"context"
	"etelgo/config"
	"etelgo/consumer"
	"log/slog"
	"sync"
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
	o.logger.Info("Running Orchestrator")

	if dryRun {
		o.logger.Info("Dry run mode - exiting")
		return nil
	}

	//start consumer
	o.consumer.Start(ctx)
	defer o.consumer.Close()

	//Messages loop
	var wg sync.WaitGroup
	workerCount := o.config.Input.Workers
	o.logger.Info("Starting workers", "count", workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go o.worker(ctx, i, &wg)
	}

	//Apply processors

	//Send to output

	//Metrics and Errors handling
	go o.HandleErrors(ctx)

	wg.Wait()

	return nil
}

func (o *Orchestrator) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	o.logger.Info("Starting worker", "id", id)

	for {
		select {
		case msg := <-o.consumer.Messages():
			err := o.ProcessMessages(msg, ctx)
			if err != nil {
				o.logger.Error("error processing message", "error", err)
			}
		case <-ctx.Done():
			o.logger.Info("worker context done, stopping", "id", id)
			return
		}

	}
}
func (o *Orchestrator) HandleErrors(ctx context.Context) {
	for {
		select {
		case err := <-o.consumer.Errors():
			o.logger.Error("received error from consumer", "error", err)
			// o.handleErrorByType(err)
		case <-ctx.Done():
			o.logger.Info("error handling context done, stopping")
			return
		}
	}
}

func (o *Orchestrator) handleErrorByType(err error) {
}

func (o *Orchestrator) ProcessMessages(msg *consumer.Message, ctx context.Context) error {
	o.logger.Info("Starting message processing")

	return nil
}
