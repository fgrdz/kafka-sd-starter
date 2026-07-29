package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBrokerPod(t *testing.T) {
	t.Parallel()
	pods := []Pod{{Name: "experiment-brokers-2", Labels: brokerLabels("experiment-brokers-2")}}
	got, err := BrokerPod(2, pods)
	if err != nil || got != "experiment-brokers-2" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestBrokerPodRejectsController(t *testing.T) {
	t.Parallel()
	labels := brokerLabels("experiment-controllers-2")
	labels["strimzi.io/pool-name"] = "controllers"
	labels["strimzi.io/broker-role"] = "false"
	labels["strimzi.io/controller-role"] = "true"
	if _, err := BrokerPod(2, []Pod{{Name: "experiment-controllers-2", Labels: labels}}); err == nil {
		t.Fatal("controller must never be selected")
	}
}

func TestBrokerPodRequiresUniqueMapping(t *testing.T) {
	t.Parallel()
	for name, pods := range map[string][]Pod{
		"zero": nil,
		"multiple": {
			{Name: "experiment-brokers-2", Labels: brokerLabels("experiment-brokers-2")},
			{Name: "other-brokers-2", Labels: brokerLabels("other-brokers-2")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BrokerPod(2, pods); err == nil {
				t.Fatal("expected non-unique mapping error")
			}
		})
	}
}

func TestMapBrokerPodRequiresReady(t *testing.T) {
	t.Parallel()
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "experiment-brokers-2", Labels: brokerLabels("experiment-brokers-2")}}
	if _, err := MapBrokerPod(2, []corev1.Pod{pod}); err == nil {
		t.Fatal("non-ready broker pod must be rejected")
	}
}

func brokerLabels(name string) map[string]string {
	return map[string]string{
		"strimzi.io/pod-name":        name,
		"strimzi.io/pool-name":       "brokers",
		"strimzi.io/broker-role":     "true",
		"strimzi.io/controller-role": "false",
	}
}
