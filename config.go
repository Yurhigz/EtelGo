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
// 				Brokers adress (list of string),
// 				topic (string)
// 				consumer_group_id (string)
// 				format (string)
// 				schema_registry_url (string) if AVRO or PROTOBUF
// 				workers (int) - default 1
//
//  - Optional :
//
//		offset_reset (string) - default "latest"
// 		enable_auto_commit (bool) - default false
// 		auto_commit_interval (int in s) - default 5s
// 		partitions (list of int)
// 		min_bytes (int)
// 		max_bytes (int)
// 		max_wait_time (int in ms)
// 		session_timeout (int in ms)
// 		heartbeat_interval (int in s)
type InputConfig struct {
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
//		broker (list of string)
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
}
