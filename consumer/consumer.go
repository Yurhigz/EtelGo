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

// Commentaire temporaire : Ici l'interface consumer me permet de définir des méthodes globales pour tous les consommateurs
type Consumer interface {
	Start(ctx context.Context) error

	Messages() <-chan *Message

	Errors() <-chan error

	Close() error
}
