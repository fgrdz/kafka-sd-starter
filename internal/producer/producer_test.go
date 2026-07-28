package producer

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/message"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type pendingClient struct {
	mu        sync.Mutex
	callbacks []func()
	flushed   bool
	submitted chan struct{}
}

func (c *pendingClient) Produce(_ context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callbacks = append(c.callbacks, func() {
		record.Offset = 1
		callback(record, nil)
	})
	select {
	case c.submitted <- struct{}{}:
	default:
	}
}

func (c *pendingClient) Flush(context.Context) error {
	c.mu.Lock()
	callbacks := append([]func(){}, c.callbacks...)
	c.callbacks = nil
	c.flushed = true
	c.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
	return nil
}

func (*pendingClient) Close() {}

func TestShutdownFlushesPendingDeliveryCallbacks(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "producer.jsonl")
	writer, err := output.NewJSONLWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Topic: "topic", Partitions: 1, Message: config.Message{PayloadBytes: 1}, Producer: config.Producer{RatePerSecond: 1000}}
	generator, err := messageGenerator("run", cfg)
	if err != nil {
		t.Fatal(err)
	}
	client := &pendingClient{submitted: make(chan struct{}, 1)}
	runner := &Runner{
		client: client, cfg: cfg, generator: generator, writer: writer,
		metrics: appmetrics.NewProducer(prometheus.NewRegistry()), logger: slog.Default(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-client.submitted
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	if !client.flushed || len(client.callbacks) != 0 {
		client.mu.Unlock()
		t.Fatalf("pending callbacks were not flushed: %+v", client)
	}
	client.mu.Unlock()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	lines := 0
	types := make(map[string]int)
	for scanner.Scan() {
		var entry struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("persisted line %d is invalid JSON: %v", lines+1, err)
		}
		types[entry.Type]++
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines == 0 || types["attempted"] == 0 || types["attempted"] != types["acknowledged"] || lines != types["attempted"]+types["acknowledged"] {
		t.Fatalf("pending shutdown records not fully persisted: lines=%d types=%v", lines, types)
	}
}

func messageGenerator(runID string, cfg config.Config) (*message.Generator, error) {
	return message.NewGenerator(runID, cfg.Partitions, cfg.Message.PayloadBytes)
}
