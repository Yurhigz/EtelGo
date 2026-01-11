package processors

import (
	"errors"
	"etelgo/consumer"
	"log/slog"
)

const (
	ProcessorTypeTimestampReplay = "timestamp_replay"
	ProcessorTypeDrop            = "drop"
	ProcessorTypeTransform       = "transform"
	ProcessorTypeEnrich          = "enrich"
	ProcessorTypeFilter          = "filter"
	ProcessorTypePassthrough     = "passthrough"
)

type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
	logger *slog.Logger
}

type Processor interface {
	Process(msg *consumer.Message) (*consumer.Message, error)
	Name() string
}

// Factory pattern to create processors based on type
func NewProcessor(cfg ProcessorConfig, logger *slog.Logger) (Processor, error) {
	cfg.logger = logger
	switch cfg.Type {
	case ProcessorTypeTimestampReplay:
		return NewTimestampReplayProcessor(cfg)
	case ProcessorTypeDrop:
		return NewDropProcessor(cfg)
	case ProcessorTypeTransform:
		return NewTransformProcessor(cfg)
	case ProcessorTypeEnrich:
		return NewEnrichProcessor(cfg)
	case ProcessorTypeFilter:
		return NewFilterProcessor(cfg)
	case ProcessorTypePassthrough:
		return NewPassthroughProcessor(cfg), nil
	default:
		logger.Error("unknown processor type", slog.String("type", cfg.Type))
		return nil, errors.New("unknown processor type: " + cfg.Type)
	}
}

// TimestampReplayProcessor is used to replay messages based on their original timestamps
// and a period of time defined by the user.
type TimestampReplayProcessor struct {
	logger *slog.Logger
}

func NewTimestampReplayProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// DropProcessor drops messages based on certain criteria.
func NewDropProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// TransformProcessor modifies message content by modifying mentioned fields' values.
func NewTransformProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// EnrichProcessor adds additional data to messages from external sources or predefined values.
func NewEnrichProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// FilterProcessor filters messages based on specific conditions.
func NewFilterProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// PassthroughProcessor forwards messages without any modifications.
type PassthroughProcessor struct {
	logger *slog.Logger
}

func NewPassthroughProcessor(cfg ProcessorConfig) *PassthroughProcessor {
	return &PassthroughProcessor{
		logger: cfg.logger,
	}
}

func (p *PassthroughProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	p.logger.Info("PassthroughProcessor: passing message through unchanged")
	return msg, nil
}

func (p *PassthroughProcessor) Name() string {
	return ProcessorTypePassthrough
}
