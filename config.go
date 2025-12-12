package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/goccy/go-yaml"
)

// Config struct which holds the YAML configuration
// It supports both mandatory and optional fields
// with appropriate data types.

type Config struct {
	Input     InputConfig
	Processor ProcessorConfig
	Output    OutputConfig
}

type Format string

const (
	FormatJSON   Format = "json"
	FormatAvro   Format = "avro"
	FormatProto  Format = "protobuf"
	FormatString Format = "string"
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
	Workers        int      `yaml:"workers"`             // Number of parallel consumer workers

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

	if ic.Auto_commit_interval == nil {
		defaultValue := "5s"
		ic.Auto_commit_interval = &defaultValue
		logger.Debug("Auto_commit_interval not provided, using default", "default", "5s")
	}

	logger.Info("InputConfig validation successful")
	return nil
}

func (ic *InputConfig) getMinBytes() int {
	panic("Not implemented yet")
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

	if *oc.Batch_size <= 0 || *oc.Batch_size > 100000 {
		logger.Warn("Batch_size not set or invalid, defaulting to 2000")
		*oc.Batch_size = 2000
	}

	if oc.Compression == nil {
		logger.Info("Compression not set, defaulting to 'none'")
		*oc.Compression = "none"
	}

	logger.Info("InputConfig validation successful")
	return nil
}

func (pc *ProcessorConfig) Validate(logger *slog.Logger) error {
	return nil
}

func LoadConfig(filePath string, logger *slog.Logger) (*Config, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}

	err = yaml.Unmarshal(content, cfg)

	if err != nil {
		return nil, err
	}

	return cfg, nil

	// panic("Not implemented yet")
}
