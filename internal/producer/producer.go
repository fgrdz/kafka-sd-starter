package producer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/message"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Runner struct {
	client    producerClient
	cfg       config.Config
	generator *message.Generator
	writer    *output.JSONLWriter
	metrics   *appmetrics.Producer
	logger    *slog.Logger
	acked     atomic.Uint64
	closeOnce sync.Once
}

type producerClient interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Flush(context.Context) error
	Close()
}

func New(cfg config.Config, runID string, writer *output.JSONLWriter, metrics *appmetrics.Producer, logger *slog.Logger) (*Runner, error) {
	generator, err := message.NewGenerator(runID, cfg.Partitions, cfg.Message.PayloadBytes)
	if err != nil {
		return nil, fmt.Errorf("create message generator: %w", err)
	}
	acks := kgo.LeaderAck()
	if cfg.Producer.Acks == "all" {
		acks = kgo.AllISRAcks()
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...), kgo.RequiredAcks(acks), kgo.RecordRetries(cfg.Producer.Retries),
		kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	if !cfg.Producer.Idempotent {
		options = append(options, kgo.DisableIdempotentWrite())
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Runner{client: client, cfg: cfg, generator: generator, writer: writer, metrics: metrics, logger: logger}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second / time.Duration(r.cfg.Producer.RatePerSecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := r.client.Flush(context.Background()); err != nil {
				return fmt.Errorf("flush pending messages: %w", err)
			}
			return nil
		case <-ticker.C:
			r.produce()
		}
	}
}

func (r *Runner) produce() {
	event := r.generator.Next()
	value, err := message.Encode(event)
	if err != nil {
		r.logger.Error("encode message", "error", err)
		return
	}
	attemptedAt := time.Now()
	r.metrics.Attempted.Inc()
	if err := r.writer.Write(map[string]any{"type": "attempted", "timestamp": attemptedAt, "message": event}); err != nil {
		r.logger.Error("write attempt", "error", err)
	}
	record := &kgo.Record{Topic: r.cfg.Topic, Partition: event.Partition, Key: []byte(event.MessageID), Value: value}
	r.client.Produce(context.Background(), record, func(delivered *kgo.Record, deliveryErr error) {
		entry := map[string]any{"timestamp": time.Now(), "message_id": event.MessageID, "partition": event.Partition}
		if deliveryErr != nil {
			r.metrics.Failed.Inc()
			entry["type"], entry["error"] = "failed", deliveryErr.Error()
			r.logger.Error("delivery failed", "message_id", event.MessageID, "partition", event.Partition, "error", deliveryErr)
		} else {
			r.acked.Add(1)
			r.metrics.Acknowledged.Inc()
			r.metrics.Latency.Observe(time.Since(attemptedAt).Seconds())
			r.metrics.LastAck.SetToCurrentTime()
			entry["type"], entry["offset"] = "acknowledged", delivered.Offset
		}
		if err := r.writer.Write(entry); err != nil {
			r.logger.Error("write producer result", "error", err)
		}
	})
}

func (r *Runner) Acknowledged() uint64 {
	return r.acked.Load()
}

func (r *Runner) Close() {
	r.closeOnce.Do(r.client.Close)
}
