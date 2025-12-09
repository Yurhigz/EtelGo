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

// There are 3 main sections: Input, Processors and Output.
// Input sections :
//
//   - Mandatory :
//     Brokers (list of string),
//     topic (string)
//     consumer_group_id (string)
//     format (string)
//     schema_registry_url (string) if AVRO or PROTOBUF
//     workers (int) - default 1
//
//   - Optional() :
//     offset_reset (string) - default "latest"
//     enable_auto_commit (bool) - default false
//     auto_commit_interval (int in s) - default 5s
//     partitions (list of int)
//     min_bytes (int)
//     max_bytes (int)
//     max_wait_time (int in ms)
//     session_timeout (int in ms)
//     heartbeat_interval (int in s)
type InputConfig struct {
	// Mandatory fields
	Brokers        []string `yaml:"brokers"`
	Topic          string   `yaml:"topic"`
	ConsumerGroup  string   `yaml:"consumer_group_id"`
	Format         string   `yaml:"format"`
	SchemaRegistry string   `yaml:"schema_registry_url,omitempty"`
	Workers        int      `yaml:"workers"`

	// Optional fields
	Offset_reset         string `yaml:"offset_reset,omitempty"`
	Enable_auto_commit   bool   `yaml:"enable_auto_commit,omitempty"`
	Auto_commit_interval string `yaml:"auto_commit_interval,omitempty"`
	Partitions           []int  `yaml:"partitions,omitempty"`
	Min_bytes            int    `yaml:"min_bytes,omitempty"`
	Max_bytes            int    `yaml:"max_bytes,omitempty"`
	Max_wait_time        int    `yaml:"max_wait_time,omitempty"`
	Session_timeout      string `yaml:"session_timeout,omitempty"`
	Heartbeat_interval   string `yaml:"heartbeat_interval,omitempty"`
}

// Processors section :
//   - Mandatory :
//   - There is no mandatory field in processors section
//   - Optional :
//   - type (string) - "filter", "transform", "enrich" (examples)
type ProcessorConfig struct {
}

// Output section :
//	- Mandatory :
//		type (string) - "kafka"
//		Brokers (list of string)
//		topic (string)
//		worker (int) - default 1
//		format (string)
//		schema_registry_url (string) if AVRO or PROTOBUF

//   - Optional :
//     partitions (list of int)
//     batch_size (int) - default 2000
//     compression (string) - "none", "gzip", "snappy", "lz4", "zstd" - default "none"
//     auto_create_topic (bool) - default false
//
//     request_timeout (int in s) - default 30s
//     retry_backoff (int in s) - default 2s
//     max_retries (int) - default 3
type OutputConfig struct {
	// Mandatory fields
	Type           string   `yaml:"type"`
	Brokers        []string `yaml:"brokers"`
	Topic          string   `yaml:"topic"`
	Workers        int      `yaml:"workers"`
	Format         string   `yaml:"format"`
	SchemaRegistry string   `yaml:"schema_registry_url,omitempty"`

	// Optional fields
	Partitions        []int  `yaml:"partitions,omitempty"`
	Batch_size        int    `yaml:"batch_size,omitempty"`
	Compression       string `yaml:"compression,omitempty"`
	Auto_create_topic bool   `yaml:"auto_create_topic,omitempty"`
	Request_timeout   string `yaml:"request_timeout,omitempty"`
	Retry_backoff     string `yaml:"retry_backoff,omitempty"`
	Max_retries       int    `yaml:"max_retries,omitempty"`
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
