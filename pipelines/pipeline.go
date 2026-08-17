package pipelines

import (
	"context"
	"etelgo/config"
	"etelgo/consumer"
	"etelgo/outputs"
	"etelgo/processors"
	"fmt"
	"log/slog"
	"sync"
)

// Need to add how to handle different type of consumer
// Agnostic consumer to prevent rewriting code as soon as library or inputs are added
type Orchestrator struct {
	config     *config.Config
	consumer   consumer.Consumer
	logger     *slog.Logger
	processors []processors.Processor
	output     outputs.Output
	//metrics to be added to enable telemetry and observability
}

func NewOrchestrator(configPath string, logger *slog.Logger, dryRun bool) (*Orchestrator, error) {
	cfg, err := config.LoadConfig(configPath, logger)
	if err != nil {
		logger.Error("error loading config")
		return nil, err
	}

	var cons consumer.Consumer
	if !dryRun {
		kc, err := consumer.NewKafkaConsumer(&cfg.Input, logger)
		if err != nil {
			logger.Error("error creating a new Kafka Consumer")
			return nil, err
		}
		cons = kc
	}

	// instantiate processors
	var procList []processors.Processor
	for i, pconf := range cfg.Processors {
		conv := processors.ProcessorConfig{
			Type:   pconf.Type,
			Config: pconf.Config,
		}
		p, err := processors.NewProcessor(conv, logger)
		if err != nil {
			return nil, fmt.Errorf("processor %d initialization failed: %w", i, err)
		}
		procList = append(procList, p)
	}

	var out outputs.Output
	if dryRun {
		out = outputs.NewConsoleOutput(logger)
	} else {
		ko, err := outputs.NewKafkaOutput(&cfg.Output, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka output: %w", err)
		}
		out = ko
	}

	return &Orchestrator{
		config:     cfg,
		consumer:   cons,
		logger:     logger,
		processors: procList,
		output:     out,
	}, nil
}

func (o *Orchestrator) Run(ctx context.Context, dryRun bool) error {
	o.logger.Info("Running Orchestrator")

	if dryRun {
		o.logger.Info("Dry run mode - no consumer will be started")
		return nil
	}

	// start consumer
	if o.consumer == nil {
		o.logger.Error("no consumer available to start")
		return nil
	}
	if err := o.consumer.Start(ctx); err != nil {
		o.logger.Error("failed to start consumer", "error", err)
		return err
	}
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

	var err error
	current := msg
	for _, p := range o.processors {
		if p == nil {
			continue
		}
		current, err = p.Process(current)
		if err != nil {
			return err
		}
		if current == nil {
			// dropped by a processor
			o.logger.Info("message dropped by processor")
			return nil
		}
	}

	if o.output != nil {
		return o.output.Write(ctx, current)
	}
	return nil
}
