package processors

import (
	"errors"
	"etelgo/consumer"
	"log/slog"
	"strings"
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

type TransformationOperation string

const (
	OperationUppercase TransformationOperation = "uppercase"
	OperationLowercase TransformationOperation = "lowercase"
	OperationAddPrefix TransformationOperation = "add_prefix"
	OperationAddSuffix TransformationOperation = "add_suffix"
)

var ValidTransformOperations = map[TransformationOperation]bool{
	OperationUppercase: true,
	OperationLowercase: true,
	OperationAddPrefix: true,
	OperationAddSuffix: true,
}

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
// This processer basically applies to every message where there is a timestamp field correspond to the field name used in the configuration.

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

// NewDropProcessor creates a new DropProcessor with the given configuration.
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

	fieldname, ok := cfg.Config["field_name"]
	if ok {
		strVal, ok := fieldname.(string)
		if ok {
			processor.fieldName = strVal
		}
	}
	return processor, nil

}

func (p *DropProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	if p.fieldName != "" && p.filterCriteria != "" {
		val, ok := msg.ValueFields[p.fieldName]
		if ok {
			strVal, ok := val.(string)
			if ok && strVal == p.filterCriteria {
				return nil, nil
			}
		}
	}

	return msg, nil

}

func (p *DropProcessor) Name() string {
	return ProcessorTypeDrop
}

// Transform operation types function
func applyTransformation(value interface{}, operation string, params map[string]interface{}) (interface{}, error) {
	strVal, ok := value.(string)
	if !ok {
		return value, nil
	}
	switch operation {
	case "uppercase":
		return strings.ToUpper(strVal), nil
	case "lowercase":
		return strings.ToLower(strVal), nil
	case "add_prefix":
		prefix, ok := params["prefix"].(string)
		if !ok {
			return value, errors.New("missing or invalid 'prefix' parameter for add_prefix operation")
		}
		return prefix + strVal, nil
	case "add_suffix":
		suffix, ok := params["suffix"].(string)
		if !ok {
			return value, errors.New("missing or invalid 'suffix' parameter for add_suffix operation")
		}
		return strVal + suffix, nil
	default:
		return value, errors.New("unknown transformation operation: " + operation)
	}
}

// TransformProcessor modifies message content by modifying mentioned fields' values.
type TransformProcessor struct {
	logger    *slog.Logger
	fieldName string
	operation string
	params    map[string]interface{}
}

func NewTransformProcessor(cfg ProcessorConfig) (Processor, error) {
	processor := &TransformProcessor{
		logger: cfg.logger,
	}

	fieldname, ok := cfg.Config["field_name"]
	if ok {
		strVal, ok := fieldname.(string)
		if ok {
			processor.fieldName = strVal
		}
	}

	operation, ok := cfg.Config["operation"]
	if ok {
		strVal, ok := operation.(string)
		if ok {
			if !ValidTransformOperations[TransformationOperation(strVal)] {
				return nil, errors.New("invalid transformation operation: " + strVal)
			}
			processor.operation = strVal
		}
	}

	processor.params = cfg.Config["params"].(map[string]interface{})

	return processor, nil
}

func (p *TransformProcessor) Name() string {
	return ProcessorTypeTransform
}

func (p *TransformProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	if p.fieldName == "" || p.operation == "" {
		p.logger.Warn("TransformProcessor: missing field_name or operation configuration")
		return msg, nil
	}

	val, ok := msg.ValueFields[p.fieldName]
	if !ok {
		return msg, nil
	}

	newVal, err := applyTransformation(val, p.operation, p.params)
	if err != nil {
		p.logger.Error("TransformProcessor: failed to apply transformation", "error", err)
		return nil, err
	}
	msg.ValueFields[p.fieldName] = newVal

	return msg, nil
}

// EnrichProcessor adds additional data to messages from external sources or predefined values.
// add_fields and values
type EnrichProcessor struct {
	logger          *slog.Logger
	addedFieldName  string
	addedFieldValue interface{}
}

func NewEnrichProcessor(cfg ProcessorConfig) (Processor, error) {
	processor := &EnrichProcessor{
		logger: cfg.logger,
	}

	fieldname, ok := cfg.Config["added_field_name"]
	if ok {
		strVal, ok := fieldname.(string)
		if ok {
			processor.addedFieldName = strVal
		}
	}

	fieldvalue, ok := cfg.Config["added_field_value"]
	if ok {
		processor.addedFieldValue = fieldvalue
	}

	return processor, nil
}

func (p *EnrichProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	if p.addedFieldName == "" || p.addedFieldValue == nil {
		p.logger.Warn("EnrichProcessor: missing added_field_name or added_field_value configuration")
		return msg, nil
	}

	msg.ValueFields[p.addedFieldName] = p.addedFieldValue
	return msg, nil
}

func (p *EnrichProcessor) Name() string {
	return ProcessorTypeEnrich
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
