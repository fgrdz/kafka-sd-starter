package experiment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/consumer"
	"github.com/fgrdz/kafka-sd-starter/internal/kafkaadmin"
	kube "github.com/fgrdz/kafka-sd-starter/internal/kubernetes"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/fgrdz/kafka-sd-starter/internal/producer"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	kafkaNamespace = "kafka"
	kafkaSelector  = "strimzi.io/cluster=experiment,strimzi.io/component-type=kafka"
)

type FaultPlan struct {
	RunID              string    `json:"run_id"`
	Profile            string    `json:"profile"`
	Topic              string    `json:"topic"`
	Partition          int32     `json:"partition"`
	BrokerID           int32     `json:"broker_id"`
	BrokerPod          string    `json:"broker_pod"`
	BrokerNode         string    `json:"broker_node"`
	Namespace          string    `json:"namespace"`
	BootstrapServers   []string  `json:"bootstrap_servers"`
	PlannedFailureTime time.Time `json:"planned_failure_time"`
	DryRun             bool      `json:"dry_run"`
	DeletionAuthorized bool      `json:"deletion_authorized"`
	plannedPodUID      string
}

type Recovery struct {
	InfrastructureSeconds float64 `json:"infrastructure_seconds"`
	KafkaSeconds          float64 `json:"kafka_seconds"`
	ApplicationSeconds    float64 `json:"application_seconds"`
	PerformanceSeconds    float64 `json:"performance_seconds"`
}

type faultKafka interface {
	Ping(context.Context) error
	CreateTopic(context.Context, string, int32, int16, int) error
	PartitionLeader(context.Context, string, int32) (kafkaadmin.Leader, error)
	TopicState(context.Context, string) (kafkaadmin.TopicState, error)
	Close()
}

type faultKubernetes interface {
	GetPod(context.Context, string, string) (*corev1.Pod, error)
	ListPods(context.Context, string, string) ([]corev1.Pod, error)
	DeletePod(context.Context, string, string) error
	ListEvents(context.Context, string) ([]corev1.Event, error)
}

var (
	newKafkaClient = func(brokers []string) (faultKafka, error) { return kafkaadmin.New(brokers) }
	newKubeClient  = func() (faultKubernetes, error) { return kube.NewInCluster() }
	nowUTC         = func() time.Time { return time.Now().UTC() }
)

type faultSession struct {
	cfg            config.Config
	runID, runDir  string
	timeline       *output.JSONLWriter
	producerWriter *output.JSONLWriter
	consumerWriter *output.JSONLWriter
	producer       *producer.Runner
	consumer       *consumer.Runner
	stopProducer   context.CancelFunc
	stopConsumer   context.CancelFunc
	producerDone   <-chan error
	consumerDone   <-chan error
	baselineRate   float64
	cancelRun      context.CancelFunc
}

func DryRunFault(ctx context.Context, cfg config.Config, options RunOptions, versionsPath string) (string, error) {
	options.Scenario, options.DryRun, options.ConfirmDelete = "fault", true, false
	return runFault(ctx, cfg, options, versionsPath)
}

func RunFault(ctx context.Context, cfg config.Config, options RunOptions, versionsPath string) (string, error) {
	options.Scenario, options.DryRun = "fault", false
	if !options.ConfirmDelete {
		return "", errors.New("fault scenario requires explicit --confirm-delete before any experiment mutation")
	}
	return runFault(ctx, cfg, options, versionsPath)
}

func runFault(ctx context.Context, cfg config.Config, options RunOptions, versionsPath string) (runDir string, resultErr error) {
	if _, err := Plan(cfg, options); err != nil {
		return "", err
	}
	versions, err := loadVersions(versionsPath)
	if err != nil {
		return "", err
	}
	kafka, err := newKafkaClient(cfg.Brokers)
	if err != nil {
		return "", err
	}
	defer kafka.Close()
	k8s, err := newKubeClient()
	if err != nil {
		return "", err
	}
	if err := validateKafkaReady(ctx, k8s); err != nil {
		return "", err
	}
	session, metadata, err := prepareFaultSession(ctx, cfg, options, versions, kafka)
	if err != nil {
		return "", err
	}
	defer func() {
		if resultErr != nil {
			metadata.Status = "failed"
			_ = writeMetadata(session.runDir, metadata)
		}
	}()
	runDir = session.runDir
	defer session.timeline.Close()
	defer session.cancelRun()
	if err := snapshotPods(ctx, k8s, filepath.Join(runDir, "kubernetes/pods-before.json")); err != nil {
		return runDir, err
	}
	before, err := kafka.TopicState(ctx, session.cfg.Topic)
	if err != nil {
		return runDir, err
	}
	if before.LeaderlessPartitions != 0 {
		return runDir, fmt.Errorf("topic has %d partition(s) without leader", before.LeaderlessPartitions)
	}
	if err := writeJSON(filepath.Join(runDir, "kafka/topic-before.json"), before); err != nil {
		return runDir, err
	}
	leader, err := kafka.PartitionLeader(ctx, session.cfg.Topic, 0)
	if err != nil {
		return runDir, err
	}
	pods, err := k8s.ListPods(ctx, kafkaNamespace, kafkaSelector)
	if err != nil {
		return runDir, err
	}
	pod, err := kube.MapBrokerPod(leader.BrokerID, pods)
	if err != nil {
		return runDir, err
	}
	if err := session.mark("leader_discovered", map[string]any{"partition": leader.Partition, "broker_id": leader.BrokerID, "broker_pod": pod.Name}); err != nil {
		return runDir, err
	}
	plan := FaultPlan{
		RunID: session.runID, Profile: cfg.Profile, Topic: session.cfg.Topic, Partition: leader.Partition,
		BrokerID: leader.BrokerID, BrokerPod: pod.Name, BrokerNode: pod.Spec.NodeName,
		Namespace: kafkaNamespace, BootstrapServers: append([]string(nil), cfg.Brokers...),
		PlannedFailureTime: nowUTC(), DryRun: options.DryRun, DeletionAuthorized: options.ConfirmDelete && !options.DryRun,
		plannedPodUID: string(pod.UID),
	}
	if err := writeJSON(filepath.Join(runDir, "fault-plan.json"), plan); err != nil {
		return runDir, err
	}
	if options.DryRun {
		if err := session.stopAndAnalyze(ctx, &metadata); err != nil {
			return runDir, err
		}
		if err := writeFaultPlaceholders(ctx, session, k8s, before); err != nil {
			return runDir, err
		}
		metadata.Status = "dry_run"
		if err := session.mark("experiment_completed", map[string]any{"dry_run": true}); err != nil {
			return runDir, err
		}
		return runDir, writeMetadata(runDir, metadata)
	}
	t0 := nowUTC()
	if err := injectFault(ctx, k8s, kafka, session, plan, func() error {
		if err := session.markAt("fault_plan_confirmed", t0, nil); err != nil {
			return err
		}
		return session.markAt("broker_pod_delete_requested", t0, map[string]any{"pod": plan.BrokerPod})
	}); err != nil {
		return runDir, err
	}
	if err := session.markAt("fault_injected", t0, map[string]any{"pod": plan.BrokerPod}); err != nil {
		return runDir, err
	}
	recovery, during, after, err := observeRecovery(ctx, session, k8s, kafka, plan, t0)
	if err != nil {
		return runDir, err
	}
	if err := writeJSON(filepath.Join(runDir, "recovery.json"), recovery); err != nil {
		return runDir, err
	}
	if err := writeJSON(filepath.Join(runDir, "kafka/topic-during.json"), during); err != nil {
		return runDir, err
	}
	if err := writeJSON(filepath.Join(runDir, "kafka/topic-after.json"), after); err != nil {
		return runDir, err
	}
	if err := session.stopAndAnalyze(ctx, &metadata); err != nil {
		return runDir, err
	}
	if err := snapshotPods(ctx, k8s, filepath.Join(runDir, "kubernetes/pods-after.json")); err != nil {
		return runDir, err
	}
	if err := snapshotEvents(ctx, k8s, filepath.Join(runDir, "kubernetes/events.jsonl")); err != nil {
		return runDir, err
	}
	metadata.Status = "passed"
	if err := session.mark("experiment_completed", nil); err != nil {
		return runDir, err
	}
	return runDir, writeMetadata(runDir, metadata)
}

func injectFault(ctx context.Context, k8s faultKubernetes, kafka faultKafka, session *faultSession, plan FaultPlan, confirmed func() error) error {
	if err := revalidateDeletion(ctx, k8s, kafka, session, plan); err != nil {
		return err
	}
	if confirmed != nil {
		if err := confirmed(); err != nil {
			return err
		}
	}
	return k8s.DeletePod(ctx, kafkaNamespace, plan.BrokerPod)
}

func prepareFaultSession(ctx context.Context, cfg config.Config, options RunOptions, versions map[string]string, kafka faultKafka) (*faultSession, Metadata, error) {
	runID, err := newRunID(options)
	if err != nil {
		return nil, Metadata{}, err
	}
	cfg.Topic, cfg.ConsumerGroup = runID, runID
	root := options.OutputRoot
	if root == "" {
		root = filepath.Join("data", "raw")
	}
	runDir := filepath.Join(root, runID)
	if err := createRunDirectories(runDir); err != nil {
		return nil, Metadata{}, err
	}
	metadata := Metadata{RunID: runID, Profile: cfg.Profile, Scenario: "fault", Repetition: options.Repetition, DryRun: options.DryRun, CreatedAt: nowUTC(), Config: cfg, Versions: versions, Status: "running"}
	if err := writeMetadata(runDir, metadata); err != nil {
		return nil, metadata, err
	}
	timeline, err := output.NewJSONLWriter(filepath.Join(runDir, "timeline.jsonl"))
	if err != nil {
		return nil, metadata, err
	}
	s := &faultSession{cfg: cfg, runID: runID, runDir: runDir, timeline: timeline}
	if err := kafka.Ping(ctx); err != nil {
		return nil, metadata, err
	}
	if err := s.mark("environment_validated", nil); err != nil {
		return nil, metadata, err
	}
	if err := kafka.CreateTopic(ctx, cfg.Topic, cfg.Partitions, cfg.ReplicationFactor, cfg.MinISR); err != nil {
		return nil, metadata, err
	}
	if err := s.mark("topic_created", nil); err != nil {
		return nil, metadata, err
	}
	s.producerWriter, err = output.NewJSONLWriter(filepath.Join(runDir, "producer.jsonl"))
	if err != nil {
		return nil, metadata, err
	}
	s.consumerWriter, err = output.NewJSONLWriter(filepath.Join(runDir, "consumer.jsonl"))
	if err != nil {
		return nil, metadata, err
	}
	s.producer, err = producer.New(cfg, runID, s.producerWriter, appmetrics.NewProducer(prometheus.NewRegistry()), slog.Default())
	if err != nil {
		return nil, metadata, err
	}
	s.consumer, err = consumer.New(cfg, s.consumerWriter, appmetrics.NewConsumer(prometheus.NewRegistry()), slog.Default())
	if err != nil {
		return nil, metadata, err
	}
	runCtx, cancelRun := context.WithTimeout(ctx, time.Duration(cfg.Experiment.Timeout))
	s.cancelRun = cancelRun
	producerCtx, stopProducer := context.WithCancel(runCtx)
	consumerCtx, stopConsumer := context.WithCancel(runCtx)
	s.stopProducer, s.stopConsumer = stopProducer, stopConsumer
	s.producerDone = runComponent(producerCtx, s.producer.Run)
	s.consumerDone = runComponent(consumerCtx, s.consumer.Run)
	if err := s.mark("applications_started", nil); err != nil {
		return nil, metadata, err
	}
	if err := waitCounters(runCtx, 30*time.Second, func() bool { return s.producer.Acknowledged() > 0 && s.consumer.Processed() > 0 }); err != nil {
		return nil, metadata, fmt.Errorf("applications unavailable: %w", err)
	}
	if err := s.mark("applications_available", nil); err != nil {
		return nil, metadata, err
	}
	if err := s.mark("warmup_started", nil); err != nil {
		return nil, metadata, err
	}
	if err := waitPhase(runCtx, time.Duration(cfg.Experiment.Warmup)); err != nil {
		return nil, metadata, err
	}
	if err := s.mark("warmup_finished", nil); err != nil {
		return nil, metadata, err
	}
	if err := s.mark("baseline_started", nil); err != nil {
		return nil, metadata, err
	}
	baselineAckStart := s.producer.Acknowledged()
	baselineStarted := nowUTC()
	if err := waitPhase(runCtx, time.Duration(cfg.Experiment.Baseline)); err != nil {
		return nil, metadata, err
	}
	baselineDuration := nowUTC().Sub(baselineStarted).Seconds()
	if baselineDuration > 0 {
		s.baselineRate = float64(s.producer.Acknowledged()-baselineAckStart) / baselineDuration
	}
	if err := s.mark("baseline_finished", nil); err != nil {
		return nil, metadata, err
	}
	return s, metadata, nil
}

func revalidateDeletion(ctx context.Context, k8s faultKubernetes, kafka faultKafka, s *faultSession, plan FaultPlan) error {
	if plan.RunID != s.runID || plan.Topic != s.cfg.Topic || plan.Namespace != kafkaNamespace || !plan.DeletionAuthorized || plan.DryRun {
		return errors.New("fault plan identity or authorization changed")
	}
	leader, err := kafka.PartitionLeader(ctx, plan.Topic, plan.Partition)
	if err != nil {
		return err
	}
	if leader.BrokerID != plan.BrokerID {
		return fmt.Errorf("leader changed after planning: planned broker %d, current broker %d", plan.BrokerID, leader.BrokerID)
	}
	pod, err := k8s.GetPod(ctx, kafkaNamespace, plan.BrokerPod)
	if err != nil {
		return err
	}
	if pod.Name != plan.BrokerPod || string(pod.UID) != plan.plannedPodUID || !kube.PodReady(pod) ||
		pod.Labels["strimzi.io/pool-name"] != "brokers" ||
		pod.Labels["strimzi.io/broker-role"] != "true" ||
		pod.Labels["strimzi.io/controller-role"] == "true" {
		return errors.New("planned pod no longer satisfies broker identity and readiness constraints")
	}
	pods, err := k8s.ListPods(ctx, kafkaNamespace, "app.kubernetes.io/name=experiment-runner,experiment/scenario=fault")
	if err != nil {
		return err
	}
	hostname := os.Getenv("HOSTNAME")
	for _, candidate := range pods {
		if candidate.Name != hostname && candidate.Status.Phase == corev1.PodRunning {
			return fmt.Errorf("another active fault experiment exists: pod/%s", candidate.Name)
		}
	}
	return nil
}

func validateKafkaReady(ctx context.Context, client faultKubernetes) error {
	pods, err := client.ListPods(ctx, kafkaNamespace, kafkaSelector)
	if err != nil {
		return err
	}
	brokers := 0
	for index := range pods {
		pod := &pods[index]
		if pod.Labels["strimzi.io/pool-name"] == "brokers" {
			brokers++
		}
		if !kube.PodReady(pod) {
			return fmt.Errorf("Kafka is not Ready: pod %s is not Running and Ready", pod.Name)
		}
	}
	if brokers == 0 {
		return errors.New("Kafka is not Ready: no Strimzi broker pods found")
	}
	return nil
}

func observeRecovery(ctx context.Context, s *faultSession, k8s faultKubernetes, kafka faultKafka, plan FaultPlan, t0 time.Time) (Recovery, kafkaadmin.TopicState, kafkaadmin.TopicState, error) {
	var result Recovery
	var during, after kafkaadmin.TopicState
	if err := ctx.Err(); err != nil {
		return result, during, after, fmt.Errorf("recovery timeout or cancellation: %w", err)
	}
	ackAtFault, consumedAtFault := s.producer.Acknowledged(), s.consumer.Processed()
	lastAck, lastConsumed := ackAtFault, consumedAtFault
	productionMarked, consumptionMarked := false, false
	var podRecreated, podReady, newLeader, kafkaReady, appReady time.Time
	var stableSince time.Time
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return result, during, after, fmt.Errorf("recovery timeout or cancellation: %w", ctx.Err())
		case at := <-ticker.C:
			state, err := kafka.TopicState(ctx, plan.Topic)
			if err == nil {
				if during.Topic == "" {
					during = state
				}
				leader, leaderErr := kafka.PartitionLeader(ctx, plan.Topic, plan.Partition)
				if leaderErr == nil && (newLeaderDetected(plan.BrokerID, leader.BrokerID) || !podRecreated.IsZero()) && newLeader.IsZero() {
					newLeader = at
					_ = s.markAt("new_leader_observed", at, map[string]any{"broker_id": leader.BrokerID})
				}
				if state.LeaderlessPartitions == 0 && state.UnderReplicated == 0 && allISRRecovered(state) && kafkaReady.IsZero() {
					kafkaReady, after = at, state
					result.KafkaSeconds = at.Sub(t0).Seconds()
					_ = s.markAt("isr_recovered", at, nil)
				}
			}
			pod, podErr := k8s.GetPod(ctx, kafkaNamespace, plan.BrokerPod)
			if podErr == nil && podRecreatedDetected(plan.plannedPodUID, pod) {
				if podRecreated.IsZero() {
					podRecreated = at
					_ = s.markAt("broker_pod_recreated", at, nil)
				}
				if kube.PodReady(pod) && podReady.IsZero() {
					podReady = at
					result.InfrastructureSeconds = at.Sub(t0).Seconds()
					_ = s.markAt("broker_pod_ready", at, nil)
				}
			}
			acks, consumed := s.producer.Acknowledged(), s.consumer.Processed()
			ackRate := float64(acks - lastAck)
			consumeRate := float64(consumed - lastConsumed)
			progressed := ackRate >= .9*s.baselineRate && consumeRate >= .9*s.baselineRate
			if acks > ackAtFault && !productionMarked {
				_ = s.markAt("application_production_resumed", at, nil)
				productionMarked = true
			}
			if consumed > consumedAtFault && !consumptionMarked {
				_ = s.markAt("application_consumption_resumed", at, nil)
				consumptionMarked = true
			}
			if productionMarked && consumptionMarked && appReady.IsZero() {
				appReady = at
				result.ApplicationSeconds = at.Sub(t0).Seconds()
			}
			if !appReady.IsZero() && progressed {
				if stableSince.IsZero() {
					stableSince = at
				}
			} else {
				stableSince = time.Time{}
			}
			lastAck, lastConsumed = acks, consumed
			if !stableSince.IsZero() && at.Sub(stableSince) >= time.Duration(s.cfg.Experiment.PerformanceWindow) {
				result.PerformanceSeconds = at.Sub(t0).Seconds()
				_ = s.markAt("performance_recovered", at, nil)
			}
			if !podReady.IsZero() && !newLeader.IsZero() && !kafkaReady.IsZero() && !appReady.IsZero() && result.PerformanceSeconds > 0 {
				return result, during, after, nil
			}
		}
	}
}

func newLeaderDetected(planned, current int32) bool {
	return current >= 0 && current != planned
}

func podRecreatedDetected(plannedUID string, pod *corev1.Pod) bool {
	return pod != nil && string(pod.UID) != "" && string(pod.UID) != plannedUID
}

func allISRRecovered(state kafkaadmin.TopicState) bool {
	for _, partition := range state.Partitions {
		if partition.Leader < 0 || len(partition.ISR) != len(partition.Replicas) {
			return false
		}
	}
	return len(state.Partitions) > 0
}

func (s *faultSession) stopAndAnalyze(ctx context.Context, metadata *Metadata) error {
	s.stopProducer()
	if err := <-s.producerDone; err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	if err := s.mark("producer_stopped", nil); err != nil {
		return err
	}
	if err := waitCounters(ctx, 30*time.Second, func() bool { return s.consumer.Processed() >= s.producer.Acknowledged() }); err != nil {
		return fmt.Errorf("consumer drain: %w", err)
	}
	s.stopConsumer()
	if err := <-s.consumerDone; err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	if err := s.mark("consumer_drained_and_stopped", nil); err != nil {
		return err
	}
	s.producer.Close()
	s.consumer.Close()
	if err := s.producerWriter.Close(); err != nil {
		return err
	}
	if err := s.consumerWriter.Close(); err != nil {
		return err
	}
	summary, err := summarize(filepath.Join(s.runDir, "producer.jsonl"), filepath.Join(s.runDir, "consumer.jsonl"))
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(s.runDir, "summary.json"), summary); err != nil {
		return err
	}
	integrity := map[string]any{
		"production_errors": summary.Failed, "acknowledged_missing": summary.Missing,
		"duplicates": summary.Duplicates, "sequence_regressions": summary.Regressions,
		"sequence_gaps": summary.Gaps, "out_of_order": summary.OutOfOrder,
		"final_lag": summary.FinalLag, "partitions": summary.Partitions,
	}
	if err := writeJSON(filepath.Join(s.runDir, "integrity.json"), integrity); err != nil {
		return err
	}
	metadata.Summary = &summary
	if err := s.mark("integrity_calculated", nil); err != nil {
		return err
	}
	return validateFaultSummary(summary)
}

func validateFaultSummary(summary BaselineSummary) error {
	if summary.Attempted == 0 {
		return errors.New("fault experiment produced no attempts")
	}
	if summary.Acknowledged == 0 {
		return errors.New("fault experiment produced no acknowledgements")
	}
	if summary.Consumed == 0 {
		return errors.New("fault experiment consumed no messages")
	}
	if summary.Acknowledged+summary.Failed != summary.Attempted {
		return fmt.Errorf(
			"fault experiment has incomplete producer outcomes: attempted=%d acknowledged=%d failed=%d",
			summary.Attempted,
			summary.Acknowledged,
			summary.Failed,
		)
	}
	return nil
}

func (s *faultSession) mark(phase string, fields map[string]any) error {
	return s.markAt(phase, nowUTC(), fields)
}

func (s *faultSession) markAt(phase string, at time.Time, fields map[string]any) error {
	entry := map[string]any{"type": "phase", "phase": phase, "timestamp": at.UTC()}
	for key, value := range fields {
		entry[key] = value
	}
	return s.timeline.Write(entry)
}

func waitCounters(ctx context.Context, timeout time.Duration, condition func() bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func snapshotPods(ctx context.Context, client faultKubernetes, path string) error {
	pods, err := client.ListPods(ctx, kafkaNamespace, kafkaSelector)
	if err != nil {
		return err
	}
	return writeJSON(path, pods)
}

func snapshotEvents(ctx context.Context, client faultKubernetes, path string) error {
	events, err := client.ListEvents(ctx, kafkaNamespace)
	if err != nil {
		return err
	}
	writer, err := output.NewJSONLWriter(path)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := writer.Write(event); err != nil {
			_ = writer.Close()
			return err
		}
	}
	return writer.Close()
}

func writeFaultPlaceholders(ctx context.Context, s *faultSession, client faultKubernetes, state kafkaadmin.TopicState) error {
	if err := writeJSON(filepath.Join(s.runDir, "kafka/topic-during.json"), state); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(s.runDir, "kafka/topic-after.json"), state); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(s.runDir, "recovery.json"), Recovery{}); err != nil {
		return err
	}
	if err := snapshotPods(ctx, client, filepath.Join(s.runDir, "kubernetes/pods-after.json")); err != nil {
		return err
	}
	return snapshotEvents(ctx, client, filepath.Join(s.runDir, "kubernetes/events.jsonl"))
}
