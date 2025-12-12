package main

import (
	"testing"
	"time"
)

func TestProcessTime(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    time.Duration
		expectError bool
	}{
		{"Valid duration - seconds", "10s", 10 * time.Second, false},
		{"Valid duration - milliseconds", "500ms", 500 * time.Millisecond, false},
		{"Valid duration - minutes", "2m", 2 * time.Minute, false},
		{"Invalid duration - malformed", "10seconds", 0, true},
		{"Invalid duration - empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result, err := ProcessDuration(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("processDuration(%q) expected error, got nil", tt.input)
				}
				return
			}

			if result != tt.expected {
				t.Errorf("processDuration(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})

	}
}
