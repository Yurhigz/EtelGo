package main

// Config struct which holds the YAML configuration
// It supports both mandatory and optional fields
// with appropriate data types.

type Config struct {
	Input     InputConfig
	Processor ProcessorConfig
	Output    OutputConfig
}

// There are 3 main sections: Input, Processors and Output.
// Input sections :
//	- Mandatory :
// 				Brokers (list of string),
// 				topic (string)
// 				consumer_group_id (string)
// 				format (string)
// 				schema_registry_url (string) if AVRO or PROTOBUF
// 				workers (int) - default 1
//
//  - Optional() :
//				offset_reset (string) - default "latest"
// 				enable_auto_commit (bool) - default false
// 				auto_commit_interval (int in s) - default 5s
// 				partitions (list of int)
// 				min_bytes (int)
// 				max_bytes (int)
// 				max_wait_time (int in ms)
// 				session_timeout (int in ms)
// 				heartbeat_interval (int in s)
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
	Auto_commit_interval int    `yaml:"auto_commit_interval,omitempty"`
	Partitions           []int  `yaml:"partitions,omitempty"`
	Min_bytes            int    `yaml:"min_bytes,omitempty"`
	Max_bytes            int    `yaml:"max_bytes,omitempty"`
	Max_wait_time        int    `yaml:"max_wait_time,omitempty"`
	Session_timeout      int    `yaml:"session_timeout,omitempty"`
	Heartbeat_interval   int    `yaml:"heartbeat_interval,omitempty"`
}

// Processors section :
//	- Mandatory :
//			- There is no mandatory field in processors section
//	- Optional :
// 		- type (string) - "filter", "transform", "enrich" (examples)
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

//	- Optional :
//		partitions (list of int)
//		batch_size (int) - default 2000
//		compression (string) - "none", "gzip", "snappy", "lz4", "zstd" - default "none"
//		auto_create_topic (bool) - default false
//
//		request_timeout (int in s) - default 30s
//		retry_backoff (int in s) - default 2s
//		max_retries (int) - default 3
type OutputConfig struct {
	// Mandatory fields
	Type           string   `yaml:"type"`
	Brokers        []string `yaml:"broker"`
	Topic          string   `yaml:"topic"`
	Workers        int      `yaml:"workers"`
	Format         string   `yaml:"format"`
	SchemaRegistry string   `yaml:"schema_registry_url,omitempty"`

	// Optional fields
	Partitions        []int  `yaml:"partitions,omitempty"`
	Batch_size        int    `yaml:"batch_size,omitempty"`
	Compression       string `yaml:"compression,omitempty"`
	Auto_create_topic bool   `yaml:"auto_create_topic,omitempty"`
	Request_timeout   int    `yaml:"request_timeout,omitempty"`
	Retry_backoff     int    `yaml:"retry_backoff,omitempty"`
	Max_retries       int    `yaml:"max_retries,omitempty"`
}

// Yaml Parsing function to load configuration from a YAML file
// It reads the file, parses the YAML content, and populates the Config struct

func LoadConfigFromYAML(filePath string) (Config, error) {
	panic("Not implemented yet")
}
