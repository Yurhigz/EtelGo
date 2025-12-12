package main

import (
	"io"
	"log/slog"
	"testing"
)

// Boilerplate test to be implemented later, using the config loading function
// func TestConfigLoading(t *testing.T) {

// 	tests := []struct {
// 		name   string
// 		config Config
// 	}{}

// 	for _, tt := range tests {

// 	}
// 	t.Log("Config loading test placeholder")
// }

func TestValidateInput(t *testing.T) {

	// Using a discarding logger to avoid spamming test output
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name    string
		config  InputConfig
		wantErr bool
	}{
		// Valid Cases
		{"Valid InputConfig",
			InputConfig{
				Brokers:        []string{"localhost:9092"},
				Topic:          "test-topic",
				ConsumerGroup:  "test-group",
				Format:         "json",
				SchemaRegistry: "",
				Workers:        2},
			false,
		},
		// Invalid Cases
		{
			"Invalid InputConfig - Unsupported Format",
			InputConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			configCopy := tt.config

			err := configCopy.Validate(logger)

			if tt.wantErr && err == nil {
				t.Errorf("Validate() error = nil, wantErr = true")
				return
			}

			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
				return
			}
		})
	}
}

func TestValidateOutput(t *testing.T) {

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name       string
		config     OutputConfig
		wantErr    bool
		wantErrMsg string
	}{
		// Valid Cases
		{name: "Valid OutputConfig - All fields set",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "json",
				Workers: 3},
			wantErr: false,
		},
		{
			name: "Valid - With defaults",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "json",
				// Workers=0 → devrait être 1
				// Batch_size=0 → devrait être 2000
			},
			wantErr: false,
		},
		{
			name: "Valid - AVRO with Schema Registry",
			config: OutputConfig{
				Type:           "kafka",
				Brokers:        []string{"localhost:9092"},
				Topic:          "output-topic",
				Format:         "avro",
				SchemaRegistry: "http://localhost:8081",
				Workers:        1,
			},
			wantErr: false,
		},
		// Missing mandatory fields
		{
			name: "Invalid - Missing Type",
			config: OutputConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "json",
			},
			wantErr:    true,
			wantErrMsg: "unsupported output type: ",
		},
		{
			name: "Invalid - Missing Brokers",
			config: OutputConfig{
				Type:   "kafka",
				Topic:  "output-topic",
				Format: "json",
			},
			wantErr:    true,
			wantErrMsg: "brokers is required and cannot be empty",
		},
		{
			name: "Invalid - Empty Brokers",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{},
				Topic:   "output-topic",
				Format:  "json",
			},
			wantErr:    true,
			wantErrMsg: "brokers is required and cannot be empty",
		},
		{
			name: "Invalid - Missing Topic",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Format:  "json",
			},
			wantErr:    true,
			wantErrMsg: "topic is required and cannot be empty",
		},
		{
			name: "Invalid - Missing Format",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
			},
			wantErr:    true,
			wantErrMsg: "unsupported format: ",
		},
		// Validations spécifiques
		{
			name: "Invalid - Unsupported Type",
			config: OutputConfig{
				Type:    "s3",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "json",
			},
			wantErr:    true,
			wantErrMsg: "unsupported output type: s3",
		},
		{
			name: "Invalid - Unsupported Format",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "xml",
			},
			wantErr:    true,
			wantErrMsg: "unsupported format: xml",
		},
		{
			name: "Invalid - AVRO without Schema Registry",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "avro",
			},
			wantErr:    true,
			wantErrMsg: "schema_registry_url is required for AVRO and PROTOBUF formats",
		},
		{
			name: "Invalid - Protobuf without Schema Registry",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "protobuf",
			},
			wantErr:    true,
			wantErrMsg: "schema_registry_url is required for AVRO and PROTOBUF formats",
		},

		// valeurs par défault
		{
			name: "Valid - Workers zero should default to 1",
			config: OutputConfig{
				Type:    "kafka",
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
				Format:  "json",
				Workers: 0,
			},
			wantErr: false,
		},
		{
			name: "Valid - Batch_size zero should default to 2000",
			config: OutputConfig{
				Type:       "kafka",
				Brokers:    []string{"localhost:9092"},
				Topic:      "output-topic",
				Format:     "json",
				Workers:    1,
				Batch_size: new(int),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configCopy := tt.config
			err := configCopy.Validate(logger)

			if tt.wantErr && err == nil {
				t.Errorf("Validate() error = nil, wantErr = true")
				return
			}

			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
				return
			}

			if tt.wantErr && err != nil && tt.wantErrMsg != "" {
				if err.Error() != tt.wantErrMsg {
					t.Errorf("Validate() error message = %q, want %q",
						err.Error(), tt.wantErrMsg)
				}
			}

			if !tt.wantErr {
				if configCopy.Workers <= 0 {
					t.Errorf("Workers should be at least 1, got %d", configCopy.Workers)
				}
				if *configCopy.Batch_size <= 0 {
					t.Errorf("Batch_size should be at least 2000, got %d", configCopy.Batch_size)
				}
			}
		})
	}

}
