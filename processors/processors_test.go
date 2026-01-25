package processors

import (
	"etelgo/consumer"
	"io"
	"log/slog"
	"testing"
	"time"
)

var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// Function to create test message to mock processing
func createTestMessage() *consumer.Message {
	return &consumer.Message{
		Key:         []byte("test-key"),
		Value:       []byte("test-value"),
		Topic:       "test-topic",
		Partition:   0,
		Offset:      0,
		Timestamp:   time.Now(),
		Headers:     map[string]string{},
		KeyFields:   map[string]interface{}{},
		ValueFields: map[string]interface{}{},
	}
}

// ==================== Processor Creation Tests ====================

func TestNewProcessors(t *testing.T) {
	tests := []struct {
		name      string
		cfg       ProcessorConfig
		expectErr bool
	}{
		{
			name: "TimestampReplay Processor",
			cfg: ProcessorConfig{
				Type:   ProcessorTypeTimestampReplay,
				Config: map[string]interface{}{"offset": 100, "unit": "seconds"},
			},
			expectErr: false,
		},
		{
			name: "TimestampReplay Processor error",
			cfg: ProcessorConfig{
				Type:   ProcessorTypeTimestampReplay,
				Config: map[string]interface{}{"invalid_param": "value"},
			},
			expectErr: true,
		},
		{
			name: "Unknown Processor Type",
			cfg: ProcessorConfig{
				Type:   "unknown_type",
				Config: map[string]interface{}{},
			},
			expectErr: true,
		},
		{
			name: "Drop Processor",
			cfg: ProcessorConfig{
				Type:   ProcessorTypeDrop,
				Config: map[string]interface{}{},
			},
			expectErr: false,
		},
		{
			name: "Drop Processor error",
			cfg: ProcessorConfig{
				Type:   ProcessorTypeDrop,
				Config: map[string]interface{}{"invalid_param": "value"},
			},
			expectErr: false,
		},
		{
			name: "Transform Processor",
			cfg: ProcessorConfig{
				Type: ProcessorTypeTransform,
				Config: map[string]interface{}{
					"field_name": "test_field",
					"operation":  "uppercase",
					"params":     map[string]interface{}{},
				},
			},
			expectErr: false,
		},
		{
			name: "Transform Processor error",
			cfg: ProcessorConfig{
				Type: ProcessorTypeTransform,
				Config: map[string]interface{}{
					"field_name": "test_field",
					"operation":  "invalid_op",
					"params":     map[string]interface{}{},
				},
			},
			expectErr: true,
		},
		{
			name: "Passthrough Processor",
			cfg: ProcessorConfig{
				Type:   ProcessorTypePassthrough,
				Config: map[string]interface{}{},
			},
			expectErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewProcessor(tt.cfg, testLogger)
			if tt.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectErr && processor == nil {
				t.Errorf("expected processor, got nil")
			}

		})
	}
}

// ==================== TimestampReplayProcessor Tests ====================

func TestTimestampReplayProcessor_Name(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	if processor.Name() != ProcessorTypeTimestampReplay {
		t.Errorf("expected name %s, got %s", ProcessorTypeTimestampReplay, processor.Name())
	}
}

func TestTimestampReplayProcessor_WithTargetTimestamp(t *testing.T) {
	targetTime := "2026-01-23T10:00:00Z"
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"target_timestamps": targetTime,
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()
	originalTimestamp := msg.Timestamp

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result == nil {
		t.Errorf("expected message, got nil")
	}

	expectedTime, _ := time.Parse(time.RFC3339, targetTime)
	if !result.Timestamp.Equal(expectedTime) {
		t.Errorf("expected timestamp %v, got %v", expectedTime, result.Timestamp)
	}
	if result.Timestamp.Equal(originalTimestamp) {
		t.Errorf("timestamp should have changed")
	}
}

func TestTimestampReplayProcessor_WithInvalidTargetTimestamp(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"target_timestamps": "invalid-date",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()

	_, err := processor.Process(msg)
	if err == nil {
		t.Errorf("expected error for invalid timestamp format, got nil")
	}
}

func TestTimestampReplayProcessor_WithOffsetSeconds(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"offset": int64(60),
			"unit":   "seconds",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()
	originalTimestamp := msg.Timestamp

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}

	expectedTimestamp := originalTimestamp.Add(60 * time.Second)
	if !result.Timestamp.Equal(expectedTimestamp) {
		t.Errorf("expected timestamp %v, got %v", expectedTimestamp, result.Timestamp)
	}
}

func TestTimestampReplayProcessor_WithOffsetMinutes(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"offset": int64(5),
			"unit":   "minutes",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()
	originalTimestamp := msg.Timestamp

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}

	expectedTimestamp := originalTimestamp.Add(5 * time.Minute)
	if !result.Timestamp.Equal(expectedTimestamp) {
		t.Errorf("expected timestamp %v, got %v", expectedTimestamp, result.Timestamp)
	}
}

func TestTimestampReplayProcessor_WithOffsetHours(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"offset": int64(2),
			"unit":   "hours",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()
	originalTimestamp := msg.Timestamp

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}

	expectedTimestamp := originalTimestamp.Add(2 * time.Hour)
	if !result.Timestamp.Equal(expectedTimestamp) {
		t.Errorf("expected timestamp %v, got %v", expectedTimestamp, result.Timestamp)
	}
}

func TestTimestampReplayProcessor_WithInvalidUnit(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"offset": int64(5),
			"unit":   "invalid_unit",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()

	_, err := processor.Process(msg)
	if err == nil {
		t.Errorf("expected error for invalid time unit, got nil")
	}
}

func TestTimestampReplayProcessor_NegativeOffset(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTimestampReplay,
		Config: map[string]interface{}{
			"offset": int64(-30),
			"unit":   "seconds",
		},
		logger: testLogger,
	}

	processor, _ := NewTimestampReplayProcessor(cfg)
	msg := createTestMessage()
	originalTimestamp := msg.Timestamp

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}

	expectedTimestamp := originalTimestamp.Add(-30 * time.Second)
	if !result.Timestamp.Equal(expectedTimestamp) {
		t.Errorf("expected timestamp %v, got %v", expectedTimestamp, result.Timestamp)
	}
}

// ==================== DropProcessor Tests ====================

func TestDropProcessor_Name(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypeDrop,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	if processor.Name() != ProcessorTypeDrop {
		t.Errorf("expected name %s, got %s", ProcessorTypeDrop, processor.Name())
	}
}

func TestDropProcessor_NoConfiguration(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypeDrop,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["test_field"] = "test_value"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result == nil {
		t.Errorf("expected message, got nil")
	}
}

func TestDropProcessor_DropMessage(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeDrop,
		Config: map[string]interface{}{
			"field_name":      "status",
			"filter_criteria": "inactive",
		},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["status"] = "inactive"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil (dropped message), got %v", result)
	}
}

func TestDropProcessor_KeepMessage_CriteriaNotMatched(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeDrop,
		Config: map[string]interface{}{
			"field_name":      "status",
			"filter_criteria": "inactive",
		},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["status"] = "active"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result == nil {
		t.Errorf("expected message, got nil")
	}
}

func TestDropProcessor_FieldDoesNotExist(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeDrop,
		Config: map[string]interface{}{
			"field_name":      "nonexistent",
			"filter_criteria": "value",
		},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["status"] = "active"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result == nil {
		t.Errorf("expected message to be kept, got nil")
	}
}

func TestDropProcessor_FieldValueNotString(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeDrop,
		Config: map[string]interface{}{
			"field_name":      "count",
			"filter_criteria": "100",
		},
		logger: testLogger,
	}

	processor, _ := NewDropProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["count"] = 100 // int, not string

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result == nil {
		t.Errorf("expected message to be kept, got nil")
	}
}

// ==================== applyTransformation Tests ====================

func TestApplyTransformation_Uppercase(t *testing.T) {
	result, err := applyTransformation("hello", "uppercase", map[string]interface{}{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "HELLO" {
		t.Errorf("expected HELLO, got %v", result)
	}
}

func TestApplyTransformation_Lowercase(t *testing.T) {
	result, err := applyTransformation("HELLO", "lowercase", map[string]interface{}{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "hello" {
		t.Errorf("expected hello, got %v", result)
	}
}

func TestApplyTransformation_AddPrefix(t *testing.T) {
	params := map[string]interface{}{"prefix": "PREFIX_"}
	result, err := applyTransformation("value", "add_prefix", params)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "PREFIX_value" {
		t.Errorf("expected PREFIX_value, got %v", result)
	}
}

func TestApplyTransformation_AddSuffix(t *testing.T) {
	params := map[string]interface{}{"suffix": "_SUFFIX"}
	result, err := applyTransformation("value", "add_suffix", params)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "value_SUFFIX" {
		t.Errorf("expected value_SUFFIX, got %v", result)
	}
}

func TestApplyTransformation_AddPrefix_MissingParameter(t *testing.T) {
	_, err := applyTransformation("value", "add_prefix", map[string]interface{}{})
	if err == nil {
		t.Errorf("expected error for missing prefix parameter, got nil")
	}
}

func TestApplyTransformation_AddSuffix_MissingParameter(t *testing.T) {
	_, err := applyTransformation("value", "add_suffix", map[string]interface{}{})
	if err == nil {
		t.Errorf("expected error for missing suffix parameter, got nil")
	}
}

func TestApplyTransformation_NonStringValue(t *testing.T) {
	result, err := applyTransformation(123, "uppercase", map[string]interface{}{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != 123 {
		t.Errorf("expected non-string value to remain unchanged, got %v", result)
	}
}

func TestApplyTransformation_UnknownOperation(t *testing.T) {
	_, err := applyTransformation("value", "unknown_op", map[string]interface{}{})
	if err == nil {
		t.Errorf("expected error for unknown operation, got nil")
	}
}

// ==================== TransformProcessor Tests ====================

func TestTransformProcessor_Name(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "test_field",
			"operation":  "uppercase",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	if processor.Name() != ProcessorTypeTransform {
		t.Errorf("expected name %s, got %s", ProcessorTypeTransform, processor.Name())
	}
}

func TestTransformProcessor_InvalidOperation(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "test_field",
			"operation":  "invalid_op",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	_, err := NewTransformProcessor(cfg)
	if err == nil {
		t.Errorf("expected error for invalid operation, got nil")
	}
}

func TestTransformProcessor_UppercaseTransform(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "message",
			"operation":  "uppercase",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "hello world"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result.ValueFields["message"] != "HELLO WORLD" {
		t.Errorf("expected HELLO WORLD, got %v", result.ValueFields["message"])
	}
}

func TestTransformProcessor_LowercaseTransform(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "message",
			"operation":  "lowercase",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "HELLO WORLD"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result.ValueFields["message"] != "hello world" {
		t.Errorf("expected hello world, got %v", result.ValueFields["message"])
	}
}

func TestTransformProcessor_AddPrefixTransform(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "message",
			"operation":  "add_prefix",
			"params": map[string]interface{}{
				"prefix": "[LOG] ",
			},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "error occurred"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result.ValueFields["message"] != "[LOG] error occurred" {
		t.Errorf("expected [LOG] error occurred, got %v", result.ValueFields["message"])
	}
}

func TestTransformProcessor_AddSuffixTransform(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "message",
			"operation":  "add_suffix",
			"params": map[string]interface{}{
				"suffix": " [END]",
			},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "processing"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result.ValueFields["message"] != "processing [END]" {
		t.Errorf("expected processing [END], got %v", result.ValueFields["message"])
	}
}

func TestTransformProcessor_FieldNotFound(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "nonexistent",
			"operation":  "uppercase",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "hello"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result.ValueFields["message"] != "hello" {
		t.Errorf("expected message to remain unchanged")
	}
}

func TestTransformProcessor_MissingFieldName(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"operation": "uppercase",
			"params":    map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "hello"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result != msg {
		t.Errorf("expected original message to be returned")
	}
}

func TestTransformProcessor_MissingOperation(t *testing.T) {
	cfg := ProcessorConfig{
		Type: ProcessorTypeTransform,
		Config: map[string]interface{}{
			"field_name": "message",
			"params":     map[string]interface{}{},
		},
		logger: testLogger,
	}

	processor, _ := NewTransformProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["message"] = "hello"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result != msg {
		t.Errorf("expected original message to be returned")
	}
}

// ==================== PassthroughProcessor Tests ====================

func TestPassthroughProcessor_Name(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypePassthrough,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor := NewPassthroughProcessor(cfg)
	if processor.Name() != ProcessorTypePassthrough {
		t.Errorf("expected name %s, got %s", ProcessorTypePassthrough, processor.Name())
	}
}

func TestPassthroughProcessor_MessageUnchanged(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypePassthrough,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor := NewPassthroughProcessor(cfg)
	msg := createTestMessage()
	msg.ValueFields["test"] = "value"
	msg.Topic = "my-topic"

	result, err := processor.Process(msg)
	if err != nil {
		t.Errorf("unexpected error processing message: %v", err)
	}
	if result != msg {
		t.Errorf("expected same message to be returned")
	}
	if result.Topic != "my-topic" {
		t.Errorf("expected topic to remain unchanged")
	}
	if result.ValueFields["test"] != "value" {
		t.Errorf("expected value fields to remain unchanged")
	}
}

func TestPassthroughProcessor_MultipleMessages(t *testing.T) {
	cfg := ProcessorConfig{
		Type:   ProcessorTypePassthrough,
		Config: map[string]interface{}{},
		logger: testLogger,
	}

	processor := NewPassthroughProcessor(cfg)

	for i := 0; i < 5; i++ {
		msg := createTestMessage()
		result, err := processor.Process(msg)
		if err != nil {
			t.Errorf("unexpected error processing message %d: %v", i, err)
		}
		if result != msg {
			t.Errorf("expected same message for iteration %d", i)
		}
	}
}
