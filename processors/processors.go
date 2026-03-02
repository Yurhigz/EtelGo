package processors

import (
	"errors"
	"etelgo/config"
	"etelgo/consumer"
	"log/slog"
	"strings"
	"time"
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
	case config.ProcessorTypeTimestampReplay:
		return NewTimestampReplayProcessor(cfg)
	case config.ProcessorTypeDrop:
		return NewDropProcessor(cfg)
	case config.ProcessorTypeTransform:
		return NewTransformProcessor(cfg)
	case config.ProcessorTypeEnrich:
		return NewEnrichProcessor(cfg)
	// case config.ProcessorTypeFilter:
	// 	return NewFilterProcessor(cfg)
	case config.ProcessorTypePassthrough:
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
	ParsedTimestamps *time.Time
	// Option 2 : an offset to replay messages
	Offset *time.Duration
	logger *slog.Logger
}

func NewTimestampReplayProcessor(cfg ProcessorConfig) (Processor, error) {
	processor := &TimestampReplayProcessor{
		logger: cfg.logger,
	}
	if parsedTs, ok := cfg.Config["parsed_timestamp"].(time.Time); ok {
		processor.ParsedTimestamps = &parsedTs
	} else {
		offset := cfg.Config["parsed_offset"].(time.Duration)
		processor.Offset = &offset
	}
	return processor, nil
}

func (p *TimestampReplayProcessor) Name() string {
	return config.ProcessorTypeTimestampReplay
}

// Process can replay messages based on the options defined in the processor.
// This processer basically applies to every message where there is a timestamp field correspond to the field name used in the configuration.
func (p *TimestampReplayProcessor) Process(msg *consumer.Message) (*consumer.Message, error) {
	p.logger.Info("TimestampReplayProcessor: processing message for timestamp replay")
	// Dual logic based on the options provided

	if p.ParsedTimestamps != nil {
		msg.Timestamp = *p.ParsedTimestamps
	} else {
		newTs := msg.Timestamp.Add(*p.Offset)
		msg.Timestamp = newTs
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
	return config.ProcessorTypeDrop
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
	return config.ProcessorTypeTransform
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
	return config.ProcessorTypeEnrich
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
	return config.ProcessorTypePassthrough
}
