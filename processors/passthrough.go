package processors

import (
	"etelgo/consumer"
	"log/slog"
)

type PassthroughProcessor struct {
	logger *slog.Logger
}

func NewPassthroughProcessor(logger *slog.Logger) *PassthroughProcessor {
	return &PassthroughProcessor{
		logger: logger,
	}
}

func (p *PassthroughProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	p.logger.Info("PassthroughProcessor: passing message through unchanged")
	return msg, nil
}

func (p *PassthroughProcessor) Name() string {
	return "PassthroughProcessor"
}
