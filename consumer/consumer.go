package consumer

import (
	"context"
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
}

type Consumer interface {
	Start(ctx context.Context) error

	Messages() <-chan *Message

	Errors() <-chan error

	Close() error
}
