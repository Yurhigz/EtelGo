package consumer

import (
	"context"
	"encoding/json"
	"time"
)

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   map[string]string

	// Deserialized fields
	KeyFields   map[string]interface{}
	ValueFields map[string]interface{}
}

type Consumer interface {
	Start(ctx context.Context) error

	Messages() <-chan *Message

	Errors() <-chan error

	Close() error
}

type Deserializer interface {
	Deserialize(data []byte) (map[string]interface{}, error)
}

type JSONDeserializer struct{}

func (d *JSONDeserializer) Deserialize(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(data, &result)
	return result, err
}

func NewDeserializer(format string) Deserializer {
	switch format {
	case "json":
		return &JSONDeserializer{}
	// case "avro":
	//	return &AvroDeserializer{}
	// case "protobuf":
	//	return &ProtobufDeserializer{}
	default:
		return &JSONDeserializer{}
	}
}
