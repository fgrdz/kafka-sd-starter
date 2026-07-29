package experiment

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/consumer"
	"github.com/fgrdz/kafka-sd-starter/internal/kafkaadmin"
	"github.com/fgrdz/kafka-sd-starter/internal/message"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/fgrdz/kafka-sd-starter/internal/producer"
	"github.com/prometheus/client_golang/prometheus"
)

type RunOptions struct {
	Profile       string
	Scenario      string
	Repetition    int
	DryRun        bool
	ConfirmDelete bool
	OutputRoot    string
	RunPrefix     string
	RunID         string
}

type Metadata struct {
	RunID      string            `json:"run_id"`
	Profile    string            `json:"profile"`
	Scenario   string            `json:"scenario"`
	Repetition int               `json:"repetition"`
	DryRun     bool              `json:"dry_run"`
	CreatedAt  time.Time         `json:"created_at"`
	Config     config.Config     `json:"config"`
	Versions   map[string]string `json:"versions"`
	Status     string            `json:"status"`
	Summary    *BaselineSummary  `json:"summary,omitempty"`
}

type BaselineSummary struct {
	Attempted    int                  `json:"attempted"`
	Acknowledged int                  `json:"acknowledged"`
	Failed       int                  `json:"failed"`
	Consumed     int                  `json:"consumed"`
	Missing      int                  `json:"acknowledged_missing"`
	Duplicates   int                  `json:"duplicates"`
	Regressions  int                  `json:"sequence_regressions"`
	Gaps         int                  `json:"sequence_gaps"`
	OutOfOrder   int                  `json:"out_of_order"`
	FinalLag     int                  `json:"final_lag"`
	Partitions   []PartitionIntegrity `json:"partitions"`
}

type PartitionIntegrity struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Consumed    int    `json:"consumed"`
	Duplicates  int    `json:"duplicates"`
	Regressions int    `json:"sequence_regressions"`
	Gaps        int    `json:"sequence_gaps"`
}

func Plan(cfg config.Config, options RunOptions) ([]string, error) {
	if options.Profile != cfg.Profile {
		return nil, errors.New("requested profile does not match configuration")
	}
	if options.Scenario != "baseline" && options.Scenario != "fault" {
		return nil, errors.New("scenario must be baseline or fault")
	}
	if options.Repetition < 1 {
		return nil, errors.New("repetition must be at least 1")
	}
	steps := []string{"validate configuration and environment", "create unique run directory", "verify Kafka stability", "start producer and consumer", "collect warmup", "collect baseline"}
	if options.Scenario == "fault" {
		steps = append(steps, "discover partition leader", "map broker ID to Strimzi pod", "record failure instant", "force-delete leader pod")
	}
	if options.Scenario == "baseline" {
		return append(steps, "stop producer", "drain and stop consumer", "calculate integrity", "export raw evidence and metadata"), nil
	}
	return append(steps, "observe four recovery milestones", "export raw evidence and metadata"), nil
}

func DryRun(cfg config.Config, options RunOptions) (string, error) {
	if !options.DryRun {
		if options.Scenario == "fault" && !options.ConfirmDelete {
			return "", errors.New("real fault requires --confirm-delete")
		}
		return "", errors.New("real experiment integration is not complete; no action was performed")
	}
	steps, err := Plan(cfg, options)
	if err != nil {
		return "", err
	}
	runID, err := newRunID(options)
	if err != nil {
		return "", err
	}
	root := options.OutputRoot
	if root == "" {
		root = os.TempDir()
	}
	runDir := filepath.Join(root, runID)
	for _, dir := range []string{"prometheus", "kubernetes", "kafka", "logs"} {
		if err := os.MkdirAll(filepath.Join(runDir, dir), 0o750); err != nil {
			return "", fmt.Errorf("create run directory: %w", err)
		}
	}
	metadata := Metadata{RunID: runID, Profile: options.Profile, Scenario: options.Scenario, Repetition: options.Repetition, DryRun: true, CreatedAt: time.Now().UTC(), Config: cfg, Status: "dry_run"}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode metadata: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "metadata.json"), data, 0o600); err != nil {
		return "", fmt.Errorf("write metadata: %w", err)
	}
	timeline, err := output.NewJSONLWriter(filepath.Join(runDir, "timeline.jsonl"))
	if err != nil {
		return "", err
	}
	for index, step := range steps {
		if err := timeline.Write(map[string]any{"type": "planned_step", "sequence": index + 1, "description": step, "dry_run": true}); err != nil {
			timeline.Close()
			return "", err
		}
	}
	if err := timeline.Close(); err != nil {
		return "", err
	}
	for _, name := range []string{"producer.jsonl", "consumer.jsonl"} {
		file, err := os.OpenFile(filepath.Join(runDir, name), os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return "", fmt.Errorf("create %s: %w", name, err)
		}
		if err := file.Close(); err != nil {
			return "", fmt.Errorf("close %s: %w", name, err)
		}
	}
	return runDir, nil
}

func RunBaseline(ctx context.Context, cfg config.Config, options RunOptions, versionsPath string) (string, error) {
	if options.DryRun {
		return DryRun(cfg, options)
	}
	if options.Scenario == "fault" {
		return RunFault(ctx, cfg, options, versionsPath)
	}
	if _, err := Plan(cfg, options); err != nil {
		return "", err
	}
	versions, err := loadVersions(versionsPath)
	if err != nil {
		return "", err
	}
	runID, err := newRunID(options)
	if err != nil {
		return "", err
	}
	cfg.Topic = runID
	cfg.ConsumerGroup = runID
	root := options.OutputRoot
	if root == "" {
		root = filepath.Join("data", "raw")
	}
	runDir := filepath.Join(root, runID)
	if err := createRunDirectories(runDir); err != nil {
		return "", err
	}
	metadata := Metadata{
		RunID: runID, Profile: cfg.Profile, Scenario: "baseline", Repetition: options.Repetition,
		CreatedAt: time.Now().UTC(), Config: cfg, Versions: versions, Status: "running",
	}
	if err := writeMetadata(runDir, metadata); err != nil {
		return "", err
	}
	timeline, err := output.NewJSONLWriter(filepath.Join(runDir, "timeline.jsonl"))
	if err != nil {
		return "", err
	}
	defer timeline.Close()
	mark := func(phase string) error {
		return timeline.Write(map[string]any{"type": "phase", "phase": phase, "timestamp": time.Now().UTC()})
	}
	if err := mark("environment_validated"); err != nil {
		return "", err
	}
	admin, err := kafkaadmin.New(cfg.Brokers)
	if err != nil {
		return "", err
	}
	defer admin.Close()
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readyCancel()
	if err := admin.Ping(readyCtx); err != nil {
		return "", err
	}
	if err := admin.CreateTopic(readyCtx, cfg.Topic, cfg.Partitions, cfg.ReplicationFactor, cfg.MinISR); err != nil {
		return "", err
	}
	if err := mark("topic_created"); err != nil {
		return "", err
	}
	producerWriter, err := output.NewJSONLWriter(filepath.Join(runDir, "producer.jsonl"))
	if err != nil {
		return "", err
	}
	defer func() { _ = producerWriter.Close() }()
	consumerWriter, err := output.NewJSONLWriter(filepath.Join(runDir, "consumer.jsonl"))
	if err != nil {
		return "", err
	}
	defer func() { _ = consumerWriter.Close() }()
	producerRunner, err := producer.New(cfg, runID, producerWriter, appmetrics.NewProducer(prometheus.NewRegistry()), slog.Default())
	if err != nil {
		return "", err
	}
	defer producerRunner.Close()
	consumerRunner, err := consumer.New(cfg, consumerWriter, appmetrics.NewConsumer(prometheus.NewRegistry()), slog.Default())
	if err != nil {
		return "", err
	}
	defer consumerRunner.Close()
	runCtx, cancelRun := context.WithTimeout(ctx, time.Duration(cfg.Experiment.Timeout))
	defer cancelRun()
	consumerCtx, stopConsumer := context.WithCancel(runCtx)
	defer stopConsumer()
	producerCtx, stopProducer := context.WithCancel(runCtx)
	defer stopProducer()
	consumerDone := runComponent(consumerCtx, consumerRunner.Run)
	producerDone := runComponent(producerCtx, producerRunner.Run)
	if err := mark("applications_started"); err != nil {
		return "", err
	}
	availabilityDeadline := time.Now().Add(30 * time.Second)
	for {
		if producerRunner.Acknowledged() > 0 && consumerRunner.Processed() > 0 {
			break
		}
		if time.Now().After(availabilityDeadline) {
			return "", errors.New("producer and consumer did not become available within 30s")
		}
		if err := waitPhase(runCtx, 250*time.Millisecond); err != nil {
			return "", err
		}
	}
	if err := mark("applications_available"); err != nil {
		return "", err
	}
	if err := mark("warmup_started"); err != nil {
		return "", err
	}
	if err := waitPhase(runCtx, time.Duration(cfg.Experiment.Warmup)); err != nil {
		return "", err
	}
	if err := mark("warmup_finished"); err != nil {
		return "", err
	}
	if err := mark("baseline_started"); err != nil {
		return "", err
	}
	if err := waitPhase(runCtx, time.Duration(cfg.Experiment.Baseline)); err != nil {
		return "", err
	}
	if err := mark("baseline_finished"); err != nil {
		return "", err
	}
	stopProducer()
	if err := <-producerDone; err != nil {
		return "", fmt.Errorf("producer: %w", err)
	}
	if err := mark("producer_stopped"); err != nil {
		return "", err
	}
	drainDeadline := time.Now().Add(30 * time.Second)
	var summary BaselineSummary
	for {
		if consumerRunner.Processed() >= producerRunner.Acknowledged() || time.Now().After(drainDeadline) {
			break
		}
		if err := waitPhase(runCtx, 500*time.Millisecond); err != nil {
			return "", err
		}
	}
	stopConsumer()
	if err := <-consumerDone; err != nil {
		return "", fmt.Errorf("consumer: %w", err)
	}
	if err := mark("consumer_drained_and_stopped"); err != nil {
		return "", err
	}
	producerRunner.Close()
	consumerRunner.Close()
	if err := producerWriter.Close(); err != nil {
		return "", fmt.Errorf("close producer JSONL before analysis: %w", err)
	}
	if err := consumerWriter.Close(); err != nil {
		return "", fmt.Errorf("close consumer JSONL before analysis: %w", err)
	}
	summary, err = summarize(filepath.Join(runDir, "producer.jsonl"), filepath.Join(runDir, "consumer.jsonl"))
	if err != nil {
		return "", err
	}
	if err := writeJSON(filepath.Join(runDir, "summary.json"), summary); err != nil {
		return "", err
	}
	integrity := struct {
		AcknowledgedMissing int                  `json:"acknowledged_missing"`
		Duplicates          int                  `json:"duplicates"`
		SequenceRegressions int                  `json:"sequence_regressions"`
		SequenceGaps        int                  `json:"sequence_gaps"`
		OutOfOrder          int                  `json:"out_of_order"`
		FinalLag            int                  `json:"final_lag"`
		Partitions          []PartitionIntegrity `json:"partitions"`
	}{
		AcknowledgedMissing: summary.Missing, Duplicates: summary.Duplicates,
		SequenceRegressions: summary.Regressions, SequenceGaps: summary.Gaps,
		OutOfOrder: summary.OutOfOrder, FinalLag: summary.FinalLag, Partitions: summary.Partitions,
	}
	if err := writeJSON(filepath.Join(runDir, "integrity.json"), integrity); err != nil {
		return "", err
	}
	if err := mark("integrity_calculated"); err != nil {
		return "", err
	}
	metadata.Summary = &summary
	metadata.Status = "passed"
	if err := validateSummary(summary); err != nil {
		metadata.Status = "failed"
		if writeErr := writeMetadata(runDir, metadata); writeErr != nil {
			return "", errors.Join(err, writeErr)
		}
		return "", err
	}
	if err := mark("experiment_completed"); err != nil {
		return "", err
	}
	if err := writeMetadata(runDir, metadata); err != nil {
		return "", err
	}
	return runDir, nil
}

func newRunID(options RunOptions) (string, error) {
	if options.RunID != "" {
		if options.RunPrefix != "" {
			return "", errors.New("--run-id and --run-prefix are mutually exclusive")
		}
		if filepath.Base(options.RunID) != options.RunID || options.RunID == "." || options.RunID == ".." {
			return "", errors.New("--run-id must be a single path-safe component")
		}
		return options.RunID, nil
	}
	prefix := options.RunPrefix
	if prefix == "" {
		prefix = fmt.Sprintf("%s-%s-%02d", options.Profile, options.Scenario, options.Repetition)
	}
	return message.NewRunID(prefix)
}

func createRunDirectories(runDir string) error {
	for _, dir := range []string{"prometheus", "kubernetes", "kafka", "logs"} {
		if err := os.MkdirAll(filepath.Join(runDir, dir), 0o750); err != nil {
			return fmt.Errorf("create run directory: %w", err)
		}
	}
	return nil
}

func loadVersions(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open versions %q: %w", path, err)
	}
	defer file.Close()
	versions := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("invalid versions line %q", line)
		}
		versions[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	for _, key := range []string{"STRIMZI_VERSION", "KAFKA_VERSION", "KIND_VERSION", "KIND_NODE_IMAGE", "KUBE_PROMETHEUS_STACK_VERSION"} {
		if versions[key] == "" {
			return nil, fmt.Errorf("missing version %s", key)
		}
	}
	return versions, nil
}

func writeMetadata(runDir string, metadata Metadata) error {
	return writeJSON(filepath.Join(runDir, "metadata.json"), metadata)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode JSON %q: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write JSON %q: %w", path, err)
	}
	return nil
}

func runComponent(ctx context.Context, run func(context.Context) error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	return done
}

func waitPhase(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("baseline interrupted: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func summarize(producerPath, consumerPath string) (BaselineSummary, error) {
	var summary BaselineSummary
	acknowledged := make(map[string]struct{})
	consumed := make(map[string]struct{})
	type partitionKey struct {
		topic     string
		partition int32
	}
	partitions := make(map[partitionKey]*PartitionIntegrity)
	if err := scanJSONL(producerPath, func(entry map[string]any) {
		switch entry["type"] {
		case "attempted":
			summary.Attempted++
		case "acknowledged":
			summary.Acknowledged++
			if id, ok := entry["message_id"].(string); ok {
				acknowledged[id] = struct{}{}
			}
		case "failed":
			summary.Failed++
		}
	}); err != nil {
		return summary, err
	}
	if err := scanJSONL(consumerPath, func(entry map[string]any) {
		if entry["type"] != "consumed" {
			return
		}
		summary.Consumed++
		if id, ok := entry["message_id"].(string); ok {
			consumed[id] = struct{}{}
		}
		if duplicate, _ := entry["duplicate"].(bool); duplicate {
			summary.Duplicates++
		}
		topic, _ := entry["topic"].(string)
		partition := int32(-1)
		if value, ok := entry["partition"].(float64); ok {
			partition = int32(value)
		}
		key := partitionKey{topic: topic, partition: partition}
		detail := partitions[key]
		if detail == nil {
			detail = &PartitionIntegrity{Topic: topic, Partition: partition}
			partitions[key] = detail
		}
		detail.Consumed++
		if duplicate, _ := entry["duplicate"].(bool); duplicate {
			detail.Duplicates++
		}
		if regression, _ := entry["sequence_regression"].(bool); regression {
			summary.Regressions++
			detail.Regressions++
		}
		if gap, _ := entry["sequence_gap"].(bool); gap {
			summary.Gaps++
			detail.Gaps++
		}
		if outOfOrder, _ := entry["out_of_order"].(bool); outOfOrder {
			summary.OutOfOrder++
		}
	}); err != nil {
		return summary, err
	}
	for id := range acknowledged {
		if _, ok := consumed[id]; !ok {
			summary.Missing++
		}
	}
	summary.FinalLag = summary.Missing
	for _, detail := range partitions {
		summary.Partitions = append(summary.Partitions, *detail)
	}
	sort.Slice(summary.Partitions, func(i, j int) bool {
		if summary.Partitions[i].Topic != summary.Partitions[j].Topic {
			return summary.Partitions[i].Topic < summary.Partitions[j].Topic
		}
		return summary.Partitions[i].Partition < summary.Partitions[j].Partition
	})
	return summary, nil
}

func scanJSONL(path string, observe func(map[string]any)) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open JSONL %q: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		var entry map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("decode JSONL %q: %w", path, err)
		}
		observe(entry)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan JSONL %q: %w", path, err)
	}
	return nil
}

func validateSummary(summary BaselineSummary) error {
	var failures []string
	if summary.Acknowledged == 0 {
		failures = append(failures, "no production acknowledgements")
	}
	if summary.Consumed == 0 {
		failures = append(failures, "no consumed messages")
	}
	if summary.Failed != 0 {
		failures = append(failures, "unexpected final production errors="+strconv.Itoa(summary.Failed))
	}
	if summary.Duplicates != 0 {
		failures = append(failures, "duplicates="+strconv.Itoa(summary.Duplicates))
	}
	if summary.Missing != 0 {
		failures = append(failures, "acknowledged_missing="+strconv.Itoa(summary.Missing))
	}
	if summary.Regressions != 0 {
		failures = append(failures, "sequence_regressions="+strconv.Itoa(summary.Regressions))
	}
	if summary.OutOfOrder != 0 {
		failures = append(failures, "out_of_order="+strconv.Itoa(summary.OutOfOrder))
	}
	if summary.Gaps != 0 {
		failures = append(failures, "sequence_gaps="+strconv.Itoa(summary.Gaps))
	}
	if summary.FinalLag != 0 {
		failures = append(failures, "final_lag="+strconv.Itoa(summary.FinalLag))
	}
	if len(failures) > 0 {
		return fmt.Errorf("baseline smoke criteria failed: %s", strings.Join(failures, ", "))
	}
	return nil
}
