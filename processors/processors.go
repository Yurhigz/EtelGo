package processors

import (
	"errors"
	"etelgo/consumer"
	"log/slog"
	"time"
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
	// Option 1 : specific timestamps to replay at
	TargetTimestamps *string // Must respect the ISO 8601 format
	// Option 2 : an offset to replay messages
	Offset *int64
	Unit   *string // e.g "seconds", "minutes", "hours"
	logger *slog.Logger
}

func NewTimestampReplayProcessor(cfg ProcessorConfig) (Processor, error) {
	processor := &TimestampReplayProcessor{
		logger: cfg.logger,
	}
	val, ok := cfg.Config["target_timestamps"]
	if ok {
		strVal, ok := val.(string)
		if ok {
			processor.TargetTimestamps = &strVal
		}
	}

	offVal, ok := cfg.Config["offset"]
	if ok {
		intVal, ok := offVal.(int64)
		if ok {
			processor.Offset = &intVal
		}
	}

	unitVal, ok := cfg.Config["unit"]
	if ok {
		strVal, ok := unitVal.(string)
		if ok {
			processor.Unit = &strVal
		}
	}

	return processor, nil
}

func (p *TimestampReplayProcessor) Name() string {
	return ProcessorTypeTimestampReplay
}

// Process can replay messages based on the options defined in the processor.
func (p *TimestampReplayProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	p.logger.Info("TimestampReplayProcessor: processing message for timestamp replay")
	// Dual logic based on the options provided

	if p.TargetTimestamps != nil {
		newTimestamp, err := time.Parse(time.RFC3339, *p.TargetTimestamps)
		if err != nil {
			p.logger.Error("failed to parse target timestamp", "error", err)
			return nil, err
		}
		msg.Timestamp = newTimestamp
	} else {
		if p.Offset != nil && p.Unit != nil {
			var duration time.Duration
			switch *p.Unit {
			case "seconds":
				duration = time.Duration(*p.Offset) * time.Second
			case "minutes":
				duration = time.Duration(*p.Offset) * time.Minute
			case "hours":
				duration = time.Duration(*p.Offset) * time.Hour
			default:
				err := errors.New("invalid time unit for offset")
				p.logger.Error("invalid time unit", "unit", *p.Unit)
				return nil, err
			}
			msg.Timestamp = msg.Timestamp.Add(duration)
		}

	}
	return msg, nil
}

// DropProcessor drops messages based on certain criteria.
type DropProcessor struct {
	filterCriteria string
	fieldName      string
	logger         *slog.Logger
}

// I'll suppose that nil message means drop in my producer
func NewDropProcessor(cfg ProcessorConfig) (Processor, error) {
	processor := &DropProcessor{
		logger: cfg.logger,
	}

	criteria, ok := cfg.Config["filter_criteria"]
	if ok {
		strVal, ok := criteria.(string)
		if ok {
			processor.filterCriteria = strVal
		}
	}

}

func (p *DropProcessor) Name() string {
	return ProcessorTypeDrop
}

// TransformProcessor modifies message content by modifying mentioned fields' values.
func NewTransformProcessor(cfg ProcessorConfig) (Processor, error) {
	panic("Not implemented")
}

// EnrichProcessor adds additional data to messages from external sources or predefined values.
func NewEnrichProcessor(cfg ProcessorConfig) (Processor, error) {
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
