package outputs

import (
	"context"
	"encoding/json"
	"etelgo/consumer"
	"fmt"
	"log/slog"
)

// Output is the interface for sink implementations
type Output interface {
	Write(ctx context.Context, msg *consumer.Message) error
	Close() error
}

// ConsoleOutput writes messages to stdout (used for dry-run)
type ConsoleOutput struct {
	logger *slog.Logger
}

func NewConsoleOutput(logger *slog.Logger) *ConsoleOutput {
	return &ConsoleOutput{logger: logger}
}

func (c *ConsoleOutput) Write(ctx context.Context, msg *consumer.Message) error {
	// try to pretty-print ValueFields as JSON
	b, err := json.Marshal(msg.ValueFields)
	if err != nil {
		c.logger.Error("ConsoleOutput: failed to marshal message", "error", err)
		return err
	}
	fmt.Println("[dry-run] topic=", msg.Topic, "offset=", msg.Offset, "value=", string(b))
	return nil
}

func (c *ConsoleOutput) Close() error {
	c.logger.Info("ConsoleOutput closed")
	return nil
}
