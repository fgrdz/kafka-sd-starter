package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndValidate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`profile: A
brokers: [localhost:9092]
topic: test-a
consumer_group: test
partitions: 3
replication_factor: 1
min_insync_replicas: 1
producer: {rate_per_second: 10, acks: leader, idempotent: false, retries: 3}
message: {payload_bytes: 32}
metrics: {producer_address: ":8080", consumer_address: ":8081"}
experiment: {warmup: 1m, baseline: 2m, timeout: 10m, application_stable: 10s, performance_window: 60s}
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profile != "A" || cfg.Partitions != 3 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestRejectsMixedProfile(t *testing.T) {
	t.Parallel()
	cfg := Config{Profile: "B", Brokers: []string{"broker"}, Topic: "t", ConsumerGroup: "g", Partitions: 1, ReplicationFactor: 1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
