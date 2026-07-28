package kubernetes

import "testing"

func TestBrokerPod(t *testing.T) {
	t.Parallel()
	pods := []Pod{{Name: "experiment-kafka-2", Labels: map[string]string{"strimzi.io/broker-id": "2"}}}
	got, err := BrokerPod(2, pods)
	if err != nil || got != "experiment-kafka-2" {
		t.Fatalf("got %q, %v", got, err)
	}
}
