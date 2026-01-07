package processors

import (
	"etelgo/consumer"
	"log/slog"
)

type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
	logger *slog.Logger
}

type Processor interface {
	Process(msg *consumer.Message) (*consumer.Message, error)
	Name() string
}
