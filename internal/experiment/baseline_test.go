package experiment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSummarizeBaseline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	producerPath := filepath.Join(dir, "producer.jsonl")
	consumerPath := filepath.Join(dir, "consumer.jsonl")
	producer := "{\"type\":\"attempted\"}\n" +
		"{\"type\":\"acknowledged\",\"message_id\":\"m1\"}\n" +
		"{\"type\":\"attempted\"}\n" +
		"{\"type\":\"acknowledged\",\"message_id\":\"m2\"}\n"
	consumer := "{\"type\":\"consumed\",\"message_id\":\"m1\",\"duplicate\":false,\"out_of_order\":false}\n" +
		"{\"type\":\"consumed\",\"message_id\":\"m2\",\"duplicate\":false,\"out_of_order\":false}\n"
	if err := os.WriteFile(producerPath, []byte(producer), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(consumerPath, []byte(consumer), 0o600); err != nil {
		t.Fatal(err)
	}
	summary, err := summarize(producerPath, consumerPath)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Acknowledged != 2 || summary.Consumed != 2 || summary.FinalLag != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if err := validateSummary(summary); err != nil {
		t.Fatalf("valid baseline rejected: %v", err)
	}
}

func TestValidateSummaryRejectsIntegrityFailure(t *testing.T) {
	t.Parallel()
	summary := BaselineSummary{Acknowledged: 2, Consumed: 1, Missing: 1, FinalLag: 1, Duplicates: 1}
	if err := validateSummary(summary); err == nil {
		t.Fatal("expected smoke criteria failure")
	}
}

func TestSummarizePreservesPartitionIntegrityDetails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	producerPath := filepath.Join(dir, "producer.jsonl")
	consumerPath := filepath.Join(dir, "consumer.jsonl")
	if err := os.WriteFile(producerPath, []byte("{\"type\":\"acknowledged\",\"message_id\":\"run-p2-00000892\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	consumer := "{\"type\":\"consumed\",\"topic\":\"topic\",\"partition\":2,\"offset\":17," +
		"\"message_id\":\"run-p2-00000892\",\"duplicate\":false,\"sequence_regression\":true," +
		"\"sequence_gap\":false,\"out_of_order\":true}\n"
	if err := os.WriteFile(consumerPath, []byte(consumer), 0o600); err != nil {
		t.Fatal(err)
	}
	summary, err := summarize(producerPath, consumerPath)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Missing != 0 || summary.Regressions != 1 || len(summary.Partitions) != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	detail := summary.Partitions[0]
	if detail.Topic != "topic" || detail.Partition != 2 || detail.Regressions != 1 {
		t.Fatalf("unexpected partition detail: %+v", detail)
	}
}

func TestLoadVersions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "versions.env")
	content := "STRIMZI_VERSION=1.1.0\nKAFKA_VERSION=4.3.0\nKIND_VERSION=0.32.0\n" +
		"KIND_NODE_IMAGE=kindest/node:v1.35.5@sha256:x\nKUBE_PROMETHEUS_STACK_VERSION=87.19.1\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	versions, err := loadVersions(path)
	if err != nil {
		t.Fatal(err)
	}
	if versions["KAFKA_VERSION"] != "4.3.0" {
		t.Fatalf("unexpected versions: %#v", versions)
	}
}
