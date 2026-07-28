package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/integrity"
	"github.com/fgrdz/kafka-sd-starter/internal/message"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Runner struct {
	client    *kgo.Client
	tracker   *integrity.Tracker
	writer    *output.JSONLWriter
	metrics   *appmetrics.Consumer
	logger    *slog.Logger
	count     atomic.Uint64
	closeOnce sync.Once
}

func New(cfg config.Config, writer *output.JSONLWriter, metrics *appmetrics.Consumer, logger *slog.Logger) (*Runner, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(cfg.Topic), kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &Runner{client: client, tracker: integrity.NewTracker(), writer: writer, metrics: metrics, logger: logger}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	for {
		fetches := r.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				r.logger.Error("fetch", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
			}
			continue
		}
		var records []*kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			if r.process(record) {
				records = append(records, record)
			}
		})
		if len(records) > 0 {
			if err := r.client.CommitRecords(ctx, records...); err != nil {
				r.logger.Error("commit offsets", "error", err)
			}
		}
	}
}

func (r *Runner) process(record *kgo.Record) bool {
	event, err := message.Decode(record.Value)
	if err != nil {
		r.logger.Error("decode consumed message", "partition", record.Partition, "offset", record.Offset, "error", err)
		return false
	}
	result := r.tracker.Observe(record.Topic, record.Partition, record.Offset, event.MessageID, event.PartitionSequence)
	r.metrics.Messages.Inc()
	r.count.Add(1)
	if result.Duplicate {
		r.metrics.Duplicates.Inc()
	}
	if result.OutOfOrder {
		r.metrics.OutOfOrder.Inc()
	}
	latency := time.Since(time.Unix(0, event.ProducedAtUnixNS))
	if latency >= 0 {
		r.metrics.Latency.Observe(latency.Seconds())
	}
	r.metrics.Last.SetToCurrentTime()
	if err := r.writer.Write(map[string]any{
		"type": "consumed", "timestamp": time.Now(), "run_id": event.RunID, "message_id": event.MessageID,
		"topic": record.Topic, "partition": record.Partition, "declared_partition": event.Partition,
		"partition_sequence": event.PartitionSequence, "offset": record.Offset, "duplicate": result.Duplicate,
		"sequence_regression": result.SequenceRegression, "sequence_gap": result.SequenceGap, "out_of_order": result.OutOfOrder,
	}); err != nil {
		r.logger.Error("write consumer event", "error", err)
	}
	return true
}

func (r *Runner) Processed() uint64 {
	return r.count.Load()
}

func (r *Runner) Close() {
	r.closeOnce.Do(r.client.Close)
}
