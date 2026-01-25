package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/goccy/go-yaml"
)

// Config struct which holds the YAML configuration
// It supports both mandatory and optional fields
// with appropriate data types.

type Config struct {
	Input      InputConfig
	Processors []ProcessorConfig
	Output     OutputConfig
}

type Format string

const (
	FormatJSON   Format = "json"
	FormatAvro   Format = "avro"
	FormatProto  Format = "protobuf"
	FormatString Format = "string"
)

const (
	ProcessorTypeTimestampReplay = "timestamp_replay"
	ProcessorTypeDrop            = "drop"
	ProcessorTypeTransform       = "transform"
	ProcessorTypeEnrich          = "enrich"
	ProcessorTypePassthrough     = "passthrough"
)

var ValidFormats = map[Format]bool{
	FormatJSON:   true,
	FormatAvro:   true,
	FormatProto:  true,
	FormatString: true,
}

// InputConfig holds Kafka consumer configuration
// Supports both mandatory and optional fields for flexible source setup
type InputConfig struct {
	// Mandatory fields
	Brokers        []string `yaml:"brokers"`             // List of Kafka broker addresses (e.g., ["localhost:9092"])
	Topic          string   `yaml:"topic"`               // Kafka topic to consume from
	ConsumerGroup  string   `yaml:"consumer_group_id"`   // Consumer group ID for offset management
	Format         string   `yaml:"format"`              // Message format: "json", "avro", "protobuf", or "string"
	SchemaRegistry string   `yaml:"schema_registry_url"` // Schema registry URL (required for avro/protobuf formats)
	Workers        int      `yaml:"workers"`             // Number of parallel workers

	// Optional fields
	Offset_reset         *string `yaml:"offset_reset,omitempty"`         // Offset reset strategy: "earliest" or "latest" (default: "latest")
	Enable_auto_commit   *bool   `yaml:"enable_auto_commit,omitempty"`   // Auto-commit consumed offsets (default: false)
	Auto_commit_interval *string `yaml:"auto_commit_interval,omitempty"` // Interval for auto-commit in seconds (default: 5s)
	Partitions           []int   `yaml:"partitions,omitempty"`           // Specific partitions to consume; if empty, consume all
	Min_bytes            *int    `yaml:"min_bytes,omitempty"`            // Minimum bytes per fetch request
	Max_bytes            *int    `yaml:"max_bytes,omitempty"`            // Maximum bytes per fetch request
	Max_wait_time        *int    `yaml:"max_wait_time,omitempty"`        // Maximum wait time in milliseconds
	Session_timeout      *string `yaml:"session_timeout,omitempty"`      // Session timeout duration (e.g., "10s", "30000ms")
	Heartbeat_interval   *string `yaml:"heartbeat_interval,omitempty"`   // Heartbeat interval duration (e.g., "3s")
}

// ProcessorConfig holds the pipeline processor configuration
// Currently no mandatory or optional fields defined
type ProcessorConfig struct {
	Type   string                 `yaml:"type,omitempty"` // Processor type : e.g., "filter", "transform"
	Config map[string]interface{} `yaml:"config,omitempty"`
}

// OutputConfig holds Kafka producer configuration
// Supports both mandatory and optional fields for flexible output setup
type OutputConfig struct {
	// Mandatory fields
	Type           string   `yaml:"type"`                          // Output type: "kafka"
	Brokers        []string `yaml:"brokers"`                       // List of Kafka broker addresses
	Topic          string   `yaml:"topic"`                         // Kafka topic to produce to
	Workers        int      `yaml:"workers"`                       // Number of parallel producer workers
	Format         string   `yaml:"format"`                        // Message format: "json", "avro", "protobuf", or "string"
	SchemaRegistry string   `yaml:"schema_registry_url,omitempty"` // Schema registry URL (required for avro/protobuf formats)

	// Optional fields
	Partitions        []int   `yaml:"partitions,omitempty"`        // Target partitions; if empty, use default partitioner
	Batch_size        *int    `yaml:"batch_size,omitempty"`        // Number of messages to batch before sending (default: 2000)
	Compression       *string `yaml:"compression,omitempty"`       // Compression algorithm: "none", "gzip", "snappy", "lz4", "zstd" (default: "none")
	Auto_create_topic *bool   `yaml:"auto_create_topic,omitempty"` // Auto-create topic if it doesn't exist (default: false)
	Request_timeout   *string `yaml:"request_timeout,omitempty"`   // Request timeout duration (e.g., "30s") (default: 30s)
	Retry_backoff     *string `yaml:"retry_backoff,omitempty"`     // Backoff duration between retries (e.g., "2s") (default: 2s)
	Max_retries       *int    `yaml:"max_retries,omitempty"`       // Maximum number of retry attempts (default: 3)
}

// Yaml Parsing function to load configuration from a YAML file
// It reads the file, parses the YAML content, and populates the Config struct

type Validator interface {
	Validate(logger *slog.Logger) error
}

func (ic *InputConfig) Validate(logger *slog.Logger) error {
	logger.Debug("Validating InputConfig", "topic", ic.Topic)

	if len(ic.Brokers) == 0 {
		logger.Error("InputConfig validation failed: Brokers is required and cannot be empty")
		return fmt.Errorf("brokers is required and cannot be empty")
	}
	if ic.Topic == "" {
		logger.Error("InputConfig validation failed: Topic is required and cannot be empty")
		return fmt.Errorf("topic is required and cannot be empty")
	}

	if ic.ConsumerGroup == "" {
		logger.Warn("ConsumerGroup has not been provided, using default 'default-group'")
		ic.ConsumerGroup = "default-group"
	}

	if !ValidFormats[Format(ic.Format)] {
		logger.Error("InputConfig validation failed: Unsupported format", "format", ic.Format)
		return fmt.Errorf("unsupported format: %s", ic.Format)
	}

	if (ic.Format == "avro" || ic.Format == "protobuf") && ic.SchemaRegistry == "" {
		logger.Error("InputConfig validation failed: schema_registry_url is required for AVRO and PROTOBUF formats")
		return fmt.Errorf("schema_registry_url is required for AVRO and PROTOBUF formats")
	}

	if ic.Workers <= 0 {
		logger.Warn("Workers not set or invalid, defaulting to 1")
		ic.Workers = 1
	}

	if ic.Offset_reset == nil {
		defaultValue := "latest"
		ic.Offset_reset = &defaultValue
		logger.Warn("Offset_reset not provided, using default", "default", "latest")
	} else {
		validOffsets := []string{"earliest", "latest"}
		valid := false
		for _, v := range validOffsets {
			if *ic.Offset_reset == v {
				valid = true
				break
			}
		}
		if !valid {
			logger.Error("Invalid offset_reset value", "value", *ic.Offset_reset)
			return fmt.Errorf("offset_reset must be 'earliest' or 'latest', got: %s", *ic.Offset_reset)
		}
	}

	if ic.Enable_auto_commit == nil {
		defaultValue := false
		ic.Enable_auto_commit = &defaultValue
		logger.Debug("Enable_auto_commit not provided, using default", "default", false)
	}

	if *ic.Enable_auto_commit {
		if ic.Auto_commit_interval == nil {
			defaultValue := "5s"
			ic.Auto_commit_interval = &defaultValue
			logger.Debug("Auto_commit_interval not provided, using default", "default", "5s")
		} else {
			_, err := time.ParseDuration(*ic.Auto_commit_interval)
			if err != nil {
				logger.Error("Invalid auto_commit_interval format", "value", *ic.Auto_commit_interval)
				return fmt.Errorf("invalid auto_commit_interval: %w", err)
			}
		}
	} else {
		if ic.Auto_commit_interval != nil {
			logger.Warn("Auto_commit_interval ignored because enable_auto_commit is false")
		}
	}

	if ic.Min_bytes == nil {
		defaultValue := 1024
		ic.Min_bytes = &defaultValue
		logger.Info("Min_bytes not set, defaulting to", "default", defaultValue)
	}

	if ic.Max_bytes == nil {
		defaultValue := 1048576
		ic.Max_bytes = &defaultValue
		logger.Info("Max_bytes not set, defaulting to", "default", defaultValue)
	}

	if ic.Max_wait_time == nil {
		defaultValue := 500
		ic.Max_wait_time = &defaultValue
		logger.Info("Max_wait_time not set, defaulting to", "default", defaultValue)
	}

	if ic.Session_timeout != nil {
		_, err := time.ParseDuration(*ic.Session_timeout)
		if err != nil {
			logger.Error("InputConfig validation failed: Invalid session_timeout format", "value", *ic.Session_timeout)
			return fmt.Errorf("invalid session_timeout format: %w", err)
		}
	} else {
		defaultValue := "10s"
		ic.Session_timeout = &defaultValue
		logger.Info("Session_timeout not set, defaulting to", "default", defaultValue)
	}

	if ic.Heartbeat_interval != nil {
		_, err := time.ParseDuration(*ic.Heartbeat_interval)
		if err != nil {
			logger.Error("InputConfig validation failed: Invalid heartbeat_interval format", "value", *ic.Heartbeat_interval)
			return fmt.Errorf("invalid heartbeat_interval format: %w", err)
		}
	} else {
		defaultValue := "3s"
		ic.Heartbeat_interval = &defaultValue
		logger.Info("Heartbeat_interval not set, defaulting to", "default", defaultValue)
	}

	logger.Info("InputConfig validation successful")
	return nil
}

func (oc *OutputConfig) Validate(logger *slog.Logger) error {
	logger.Debug("Validating OutputConfig", "topic", oc.Topic)
	if oc.Type != "kafka" {
		logger.Error("OutputConfig validation failed: Unsupported output type", "type", oc.Type)
		return fmt.Errorf("unsupported output type: %s", oc.Type)
	}

	if len(oc.Brokers) == 0 {
		logger.Error("OutputConfig validation failed: Brokers is required and cannot be empty")
		return fmt.Errorf("brokers is required and cannot be empty")
	}

	if oc.Topic == "" {
		logger.Error("OutputConfig validation failed: Topic is required and cannot be empty")
		return fmt.Errorf("topic is required and cannot be empty")
	}

	if oc.Workers <= 0 {
		logger.Warn("Workers not set or invalid, defaulting to 1")
		oc.Workers = 1
	}

	if !ValidFormats[Format(oc.Format)] {
		logger.Error("OutputConfig validation failed: Unsupported format", "format", oc.Format)
		return fmt.Errorf("unsupported format: %s", oc.Format)
	}

	if (oc.Format == "avro" || oc.Format == "protobuf") && oc.SchemaRegistry == "" {
		logger.Error("OutputConfig validation failed: schema_registry_url is required for AVRO and PROTOBUF formats")
		return fmt.Errorf("schema_registry_url is required for AVRO and PROTOBUF formats")
	}

	if oc.Batch_size == nil {
		defaultValue := 2000
		oc.Batch_size = &defaultValue
		logger.Debug("Batch_size not provided, using default", "default", 2000)
	} else if *oc.Batch_size <= 0 || *oc.Batch_size > 100000 {
		logger.Warn("Batch_size invalid, using default", "provided", *oc.Batch_size)
		defaultValue := 2000
		oc.Batch_size = &defaultValue
	}

	if oc.Compression == nil {
		defaultValue := "none"
		oc.Compression = &defaultValue
		logger.Debug("Compression not provided, using default", "default", "none")
	} else {
		// Valider les valeurs acceptées
		validCompressions := []string{"none", "gzip", "snappy", "lz4", "zstd"}
		valid := false
		for _, v := range validCompressions {
			if *oc.Compression == v {
				valid = true
				break
			}
		}
		if !valid {
			logger.Error("Invalid compression", "value", *oc.Compression)
			return fmt.Errorf("compression must be one of: none, gzip, snappy, lz4, zstd; got: %s", *oc.Compression)
		}
	}

	if oc.Auto_create_topic == nil {
		defaultValue := false
		oc.Auto_create_topic = &defaultValue
		logger.Info("Auto_create_topic not set, defaulting to", "default", defaultValue)
	}

	if oc.Retry_backoff != nil {
		_, err := time.ParseDuration(*oc.Retry_backoff)
		if err != nil {
			logger.Error("Invalid retry_backoff format", "value", *oc.Retry_backoff)
			return fmt.Errorf("invalid retry_backoff: %w", err)
		}
	} else {
		defaultValue := "2s"
		logger.Info("Retry_backoff not set, defaulting to", "default", defaultValue)
		oc.Retry_backoff = &defaultValue
	}

	if oc.Request_timeout != nil {
		_, err := time.ParseDuration(*oc.Request_timeout)
		if err != nil {
			logger.Error("OutputConfig validation failed: Invalid request_timeout format, default value of 10s was set", "value", *oc.Request_timeout)
			*oc.Request_timeout = "10s"
		}

	} else {
		defaultValue := "10s"
		logger.Info("Request_timeout not set, defaulting to", "default", defaultValue)
		oc.Request_timeout = &defaultValue
	}

	if oc.Max_retries == nil {
		defaultValue := 3
		oc.Max_retries = &defaultValue
		logger.Info("Max_retries not set, defaulting to", "default", defaultValue)
	}

	logger.Info("InputConfig validation successful")
	return nil
}

type ProcessorValidator interface {
	Validate(config map[string]interface{}, logger *slog.Logger) error
}

// Validators mapping for different processor types and to provide an easier implementation of the Validate method
var processorValidators = map[string]ProcessorValidator{
	ProcessorTypeTimestampReplay: &TimestampReplayValidator{},
	ProcessorTypeTransform:       &TransformValidator{},
	ProcessorTypeDrop:            &DropValidator{},
	ProcessorTypeEnrich:          &EnrichValidator{},
	ProcessorTypePassthrough:     &PassthroughValidator{},
}

// ====== TIMESTAMP REPLAY VALIDATOR ====== //

type TimestampReplayValidator struct{}

var availableUnits = map[string]bool{
	"microseconds": true,
	"milliseconds": true,
	"seconds":      true,
	"minutes":      true,
	"hours":        true,
	"days":         true,
	"weeks":        true,
	"months":       true,
	"years":        true,
}

func (v *TimestampReplayValidator) Validate(cfg map[string]interface{}, logger *slog.Logger) error {
	hasTargetTimestamp := cfg["target_timestamp"] != nil
	hasOffset := cfg["offset"] != nil
	hasUnit := cfg["unit"] != nil

	if !hasTargetTimestamp && !hasOffset {
		logger.Error("timestamp_replay validation failed: must provide either 'target_timestamp' or 'offset'")
		return fmt.Errorf("timestamp_replay: must provide either 'target_timestamp' or 'offset' but not both")
	}

	if hasTargetTimestamp && hasOffset {
		logger.Error("timestamp_replay validation failed: cannot provide both 'target_timestamp' and 'offset'")
		return fmt.Errorf("timestamp_replay: cannot provide both 'target_timestamp' and 'offset'")
	}

	if hasOffset && !hasUnit {
		logger.Error("timestamp_replay validation failed: 'unit' is required when using 'offset'")
		return fmt.Errorf("timestamp_replay: 'unit' is required when using 'offset'")
	}

	if hasTargetTimestamp {
		parsedtimestamp, err := time.Parse(time.RFC3339, cfg["target_timestamp"].(string))
		if err != nil {
			logger.Error("timestamp_replay validation failed: invalid target_timestamp format", "error", err)
			return fmt.Errorf("timestamp_replay: invalid target_timestamp format: %w", err)
		}

		cfg["parsed_timestamp"] = parsedtimestamp
	}

	if hasOffset {
		offset, ok := cfg["offset"]
		switch offset.(type) {
		case int, int64:
		default:
			logger.Error("timestamp_replay validation failed: 'offset' must be an integer")
			return fmt.Errorf("offset must be an integer")
		}
		unitStr, ok := cfg["unit"].(string)
		if !ok || !availableUnits[unitStr] {
			logger.Error("timestamp_replay validation failed: invalid 'unit' value", "value", cfg["unit"])
			return fmt.Errorf("timestamp_replay: invalid 'unit' value: %v", cfg["unit"])
		}
	}

	return nil
}

// ====== DROP VALIDATOR ====== //

type DropValidator struct{}

// DropValidator has two specifics fields :
// filterCriteria : string (e.g., "field_name=<filterCriteria")
// fieldName : string (e.g., "<field_name>=filterCriteria")
func (v *DropValidator) Validate(cfg map[string]interface{}, logger *slog.Logger) error {
	hasFieldName := cfg["field_name"] != nil
	hasFilterCriteria := cfg["filter_criteria"] != nil

	if !hasFieldName || !hasFilterCriteria {
		logger.Error("drop validation failed: both 'field_name' and 'filter_criteria' are required")
		return fmt.Errorf("drop: both 'field_name' and 'filter_criteria' are required")
	}

	if _, ok := cfg["filter_criteria"].(string); !ok {
		logger.Error("drop validation failed: 'filter_criteria' must be a string")
		return fmt.Errorf("drop: 'filter_criteria' must be a string")
	}

	if _, ok := cfg["field_name"].(string); !ok {
		logger.Error("drop validation failed: 'field_name' must be a string")
		return fmt.Errorf("drop: 'field_name' must be a string")
	}

	return nil
}

// ====== TRANSFORM VALIDATOR ====== //

type TransformValidator struct{}

var availableOperations = map[string]bool{
	"uppercase":  true,
	"lowercase":  true,
	"add_prefix": true,
	"add_suffix": true,
}

// TransformValidator has two specifics fields :
// fieldName : string (the field to modify/transform)
// operation : string (e.g., "uppercase", "lowercase", "add_prefix", "add_suffix")
// prefix : string (the prefix to add, required if operation is "add_prefix")
// suffix : string (the suffix to add, required if operation is "add_suffix")
func (v *TransformValidator) Validate(cfg map[string]interface{}, logger *slog.Logger) error {
	hasFieldName := cfg["field_name"] != nil
	hasOperation := cfg["operation"] != nil

	if !hasFieldName || !hasOperation {
		logger.Error("transform validation failed: both 'field_name' and 'operation' are required")
		return fmt.Errorf("transform: both 'field_name' and 'operation' are required")
	}

	if _, ok := cfg["field_name"].(string); !ok {
		logger.Error("transform validation failed: 'field_name' must be a string")
		return fmt.Errorf("transform: 'field_name' must be a string")
	}

	if _, ok := cfg["operation"].(string); !ok {
		logger.Error("transform validation failed: 'operation' must be a string")
		return fmt.Errorf("transform: 'operation' must be a string")
	}

	if availableOperations[cfg["operation"].(string)] == false {
		logger.Error("transform validation failed: invalid 'operation' value", "value", cfg["operation"])
		return fmt.Errorf("transform: invalid 'operation' value: %v", cfg["operation"])
	}

	if (cfg["operation"] == "add_prefix" && cfg["prefix"] == nil) || (cfg["operation"] == "add_suffix" && cfg["suffix"] == nil) {
		logger.Error("transform validation failed: 'prefix' or 'suffix' is required for 'add_prefix' or 'add_suffix'")
		return fmt.Errorf("transform: 'prefix' or 'suffix' is required for 'add_prefix' or 'add_suffix'")
	}

	if cfg["operation"] == "add_prefix" {
		if _, ok := cfg["prefix"].(string); !ok {
			logger.Error("transform validation failed: 'prefix' must be a string")
			return fmt.Errorf("transform: 'prefix' must be a string")
		}
	}

	if cfg["operation"] == "add_suffix" {
		if _, ok := cfg["suffix"].(string); !ok {
			logger.Error("transform validation failed: 'suffix' must be a string")
			return fmt.Errorf("transform: 'suffix' must be a string")
		}
	}

	return nil
}

// ====== ENRICH VALIDATOR ====== //

type EnrichValidator struct{}

// EnrichValidator has two specifics fields :
// fieldName : string (A new for the new field to add)
// fieldValue : interface{} (The value to set to the new field)
func (v *EnrichValidator) Validate(cfg map[string]interface{}, logger *slog.Logger) error {
	hasFieldName := cfg["field_name"] != nil
	hasFieldValue := cfg["field_value"] != nil
	if !hasFieldName || !hasFieldValue {
		logger.Error("enrich validation failed: both 'field_name' and 'field_value' are required")
		return fmt.Errorf("enrich: both 'field_name' and 'field_value' are required")
	}

	if _, ok := cfg["field_name"].(string); !ok {
		logger.Error("enrich validation failed: 'field_name' must be a string")
		return fmt.Errorf("enrich: 'field_name' must be a string")
	}

	return nil
}

// ====== PASSTHROUGH VALIDATOR ====== //

type PassthroughValidator struct{}

// PassthroughValidator has no specific fields.
// Simply passes messages without any modifications.
func (v *PassthroughValidator) Validate(cfg map[string]interface{}, logger *slog.Logger) error {
	return nil
}

// Validate method for ProcessorConfig
func (pc *ProcessorConfig) Validate(logger *slog.Logger) error {
	if pc.Type == "" {
		logger.Warn("ProcessorConfig validation skipped: Type is empty")
	}

	validator, exists := processorValidators[pc.Type]
	if !exists {
		logger.Error("Unknown processor type, skipping validation", "type", pc.Type)
		return errors.New("unknown processor type: " + pc.Type)
	}

	return validator.Validate(pc.Config, logger)
}

func LoadConfig(filePath string, logger *slog.Logger) (*Config, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}

	err = yaml.Unmarshal(content, cfg)

	if err != nil {
		logger.Error("Failed to parse YAML", "error", err)
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if err := cfg.Input.Validate(logger); err != nil {
		return nil, fmt.Errorf("input validation failed: %w", err)
	}

	if err := cfg.Output.Validate(logger); err != nil {
		return nil, fmt.Errorf("output validation failed: %w", err)
	}

	for i, processorcfg := range cfg.Processors {
		logger.Info("Validating processor", "type", processorcfg.Type)
		err := processorcfg.Validate(logger)
		if err != nil {
			return nil, fmt.Errorf("processor %d validation failed: %w", i, err)
		}
	}

	return cfg, nil
}
