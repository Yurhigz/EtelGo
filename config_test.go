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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name    string
		config  InputConfig
		wantErr bool
	}{
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

}
