package experiment

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/kafkaadmin"
)

type fakeFaultKube struct {
	pod         *corev1.Pod
	pods        []corev1.Pod
	deleteCalls []string
}

func (f *fakeFaultKube) GetPod(context.Context, string, string) (*corev1.Pod, error) {
	if f.pod == nil {
		return nil, errors.New("not found")
	}
	return f.pod.DeepCopy(), nil
}
func (f *fakeFaultKube) ListPods(_ context.Context, _ string, selector string) ([]corev1.Pod, error) {
	if selector == "app.kubernetes.io/name=experiment-runner,experiment/scenario=fault" {
		return f.pods, nil
	}
	return nil, nil
}
func (f *fakeFaultKube) DeletePod(_ context.Context, namespace, name string) error {
	f.deleteCalls = append(f.deleteCalls, namespace+"/"+name)
	return nil
}
func (f *fakeFaultKube) ListEvents(context.Context, string) ([]corev1.Event, error) {
	return nil, nil
}

type fakeFaultKafka struct{ leader kafkaadmin.Leader }

func (f *fakeFaultKafka) Ping(context.Context) error { return nil }
func (f *fakeFaultKafka) CreateTopic(context.Context, string, int32, int16, int) error {
	return nil
}
func (f *fakeFaultKafka) PartitionLeader(context.Context, string, int32) (kafkaadmin.Leader, error) {
	return f.leader, nil
}
func (f *fakeFaultKafka) TopicState(context.Context, string) (kafkaadmin.TopicState, error) {
	return kafkaadmin.TopicState{}, nil
}
func (f *fakeFaultKafka) Close() {}

func TestInjectFaultDeletesExactlyPlannedPodOnce(t *testing.T) {
	t.Setenv("HOSTNAME", "runner")
	pod := readyBrokerPod("experiment-brokers-1", "uid-1")
	k8s := &fakeFaultKube{pod: pod}
	kafka := &fakeFaultKafka{leader: kafkaadmin.Leader{Topic: "run", Partition: 0, BrokerID: 1}}
	session := &faultSession{runID: "run", cfg: config.Config{Topic: "run"}}
	plan := authorizedPlan(pod)
	if err := injectFault(context.Background(), k8s, kafka, session, plan, nil); err != nil {
		t.Fatal(err)
	}
	if len(k8s.deleteCalls) != 1 || k8s.deleteCalls[0] != "kafka/experiment-brokers-1" {
		t.Fatalf("unexpected deletes: %#v", k8s.deleteCalls)
	}
}

func TestInjectFaultWithoutConfirmationNeverDeletes(t *testing.T) {
	pod := readyBrokerPod("experiment-brokers-1", "uid-1")
	k8s := &fakeFaultKube{pod: pod}
	kafka := &fakeFaultKafka{leader: kafkaadmin.Leader{Topic: "run", Partition: 0, BrokerID: 1}}
	session := &faultSession{runID: "run", cfg: config.Config{Topic: "run"}}
	plan := authorizedPlan(pod)
	plan.DeletionAuthorized = false
	if err := injectFault(context.Background(), k8s, kafka, session, plan, nil); err == nil {
		t.Fatal("expected authorization error")
	}
	if len(k8s.deleteCalls) != 0 {
		t.Fatalf("delete called without confirmation: %#v", k8s.deleteCalls)
	}
}

func TestInjectFaultDryRunNeverDeletes(t *testing.T) {
	pod := readyBrokerPod("experiment-brokers-1", "uid-1")
	k8s := &fakeFaultKube{pod: pod}
	kafka := &fakeFaultKafka{leader: kafkaadmin.Leader{Topic: "run", Partition: 0, BrokerID: 1}}
	session := &faultSession{runID: "run", cfg: config.Config{Topic: "run"}}
	plan := authorizedPlan(pod)
	plan.DryRun = true
	if err := injectFault(context.Background(), k8s, kafka, session, plan, nil); err == nil {
		t.Fatal("expected dry-run safety error")
	}
	if len(k8s.deleteCalls) != 0 {
		t.Fatalf("dry-run called DeletePod: %#v", k8s.deleteCalls)
	}
}

func TestInjectFaultNeverDeletesDifferentPod(t *testing.T) {
	planned := readyBrokerPod("experiment-brokers-1", "uid-1")
	current := readyBrokerPod("experiment-brokers-2", "uid-2")
	k8s := &fakeFaultKube{pod: current}
	kafka := &fakeFaultKafka{leader: kafkaadmin.Leader{Topic: "run", Partition: 0, BrokerID: 1}}
	session := &faultSession{runID: "run", cfg: config.Config{Topic: "run"}}
	if err := injectFault(context.Background(), k8s, kafka, session, authorizedPlan(planned), nil); err == nil {
		t.Fatal("expected pod identity error")
	}
	if len(k8s.deleteCalls) != 0 {
		t.Fatal("different pod was deleted")
	}
}

func TestRecoveryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err := observeRecovery(ctx, &faultSession{}, &fakeFaultKube{}, &fakeFaultKafka{}, FaultPlan{}, metav1.Now().Time)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestNewLeaderDetection(t *testing.T) {
	t.Parallel()
	if !newLeaderDetected(1, 2) || newLeaderDetected(1, 1) || newLeaderDetected(1, -1) {
		t.Fatal("new leader detector returned an invalid result")
	}
}

func TestPodRecreatedDetection(t *testing.T) {
	t.Parallel()
	if !podRecreatedDetected("old", readyBrokerPod("experiment-brokers-1", "new")) {
		t.Fatal("replacement UID was not detected")
	}
	if podRecreatedDetected("same", readyBrokerPod("experiment-brokers-1", "same")) {
		t.Fatal("same UID must not be considered recreated")
	}
}

func TestRecoveryTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	_, _, _, err := observeRecovery(ctx, &faultSession{}, &fakeFaultKube{}, &fakeFaultKafka{}, FaultPlan{}, metav1.Now().Time)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func readyBrokerPod(name, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: typesUID(uid), Labels: map[string]string{
			"strimzi.io/pod-name": name, "strimzi.io/pool-name": "brokers",
			"strimzi.io/broker-role": "true", "strimzi.io/controller-role": "false",
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func authorizedPlan(pod *corev1.Pod) FaultPlan {
	return FaultPlan{
		RunID: "run", Topic: "run", Partition: 0, BrokerID: 1, BrokerPod: pod.Name,
		Namespace: kafkaNamespace, DeletionAuthorized: true, plannedPodUID: string(pod.UID),
	}
}

func typesUID(value string) types.UID { return types.UID(value) }
