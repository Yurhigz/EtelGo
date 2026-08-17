package outputs

import (
    "context"
    "encoding/json"
    "fmt"
    "log/slog"
    "strings"
    "time"

    "etelgo/config"
    "etelgo/consumer"

    "github.com/twmb/franz-go/pkg/kgo"
)

// KafkaOutput publishes messages to Kafka using franz-go (kgo)
type KafkaOutput struct {
    client       *kgo.Client
    cfg          *config.OutputConfig
    logger       *slog.Logger
    timeout      time.Duration
    retryBackoff time.Duration
    maxRetries   int
}

func NewKafkaOutput(cfg *config.OutputConfig, logger *slog.Logger) (*KafkaOutput, error) {
    logger.Info("Creating Kafka output", "brokers", cfg.Brokers, "topic", cfg.Topic)

    client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
    if err != nil {
        logger.Error("failed to create kafka client for producer", "error", err)
        return nil, err
    }

    ko := &KafkaOutput{
        client: client,
        cfg:    cfg,
        logger: logger,
    }

    // parse request timeout
    if cfg.Request_timeout != nil {
        if d, err := time.ParseDuration(*cfg.Request_timeout); err == nil {
            ko.timeout = d
        } else {
            ko.timeout = 10 * time.Second
        }
    } else {
        ko.timeout = 10 * time.Second
    }

    if cfg.Retry_backoff != nil {
        if d, err := time.ParseDuration(*cfg.Retry_backoff); err == nil {
            ko.retryBackoff = d
        } else {
            ko.retryBackoff = 2 * time.Second
        }
    } else {
        ko.retryBackoff = 2 * time.Second
    }

    if cfg.Max_retries != nil {
        ko.maxRetries = *cfg.Max_retries
    } else {
        ko.maxRetries = 3
    }

    // compression support: currently only log if unsupported
    if cfg.Compression != nil {
        c := strings.ToLower(*cfg.Compression)
        switch c {
        case "none":
        default:
            logger.Info("compression requested but not explicitly configured in producer; using broker defaults", "compression", c)
        }
    }

    return ko, nil
}

func (k *KafkaOutput) Write(ctx context.Context, msg *consumer.Message) error {
    var value []byte
    // prefer ValueFields for structured data
    if msg.ValueFields != nil {
        b, err := json.Marshal(msg.ValueFields)
        if err != nil {
            k.logger.Error("KafkaOutput: failed to marshal ValueFields", "error", err)
            return err
        }
        value = b
    } else if msg.Value != nil {
        value = msg.Value
    } else {
        value = []byte{}
    }

    rec := &kgo.Record{
        Topic: k.cfg.Topic,
        Key:   msg.Key,
        Value: value,
    }

    var lastErr error
    for attempt := 0; attempt <= k.maxRetries; attempt++ {
        ctx2, cancel := context.WithTimeout(ctx, k.timeout)
        _, err := k.client.ProduceSync(ctx2, rec)
        cancel()
        if err == nil {
            return nil
        }
        lastErr = err
        k.logger.Warn("KafkaOutput: produce failed, will retry", "attempt", attempt, "error", err)
        time.Sleep(k.retryBackoff)
    }

    return fmt.Errorf("failed to produce after %d attempts: %w", k.maxRetries+1, lastErr)
}

func (k *KafkaOutput) Close() error {
    k.logger.Info("Closing Kafka producer")
    if k.client != nil {
        k.client.Close()
    }
    return nil
}