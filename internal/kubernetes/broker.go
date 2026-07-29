package kubernetes

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type Pod struct {
	Name   string
	Labels map[string]string
}

func BrokerPod(brokerID int32, pods []Pod) (string, error) {
	id := strconv.FormatInt(int64(brokerID), 10)
	var matches []string
	for _, pod := range pods {
		if pod.Labels["strimzi.io/pool-name"] != "brokers" ||
			pod.Labels["strimzi.io/broker-role"] != "true" ||
			pod.Labels["strimzi.io/controller-role"] == "true" {
			continue
		}
		if pod.Labels["strimzi.io/broker-id"] == id ||
			pod.Labels["strimzi.io/pod-name"] == pod.Name && strings.HasSuffix(pod.Name, "-"+id) {
			matches = append(matches, pod.Name)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("broker %d must map to exactly one ready Strimzi broker pod, found %d", brokerID, len(matches))
	}
	return matches[0], nil
}

func MapBrokerPod(brokerID int32, pods []corev1.Pod) (*corev1.Pod, error) {
	compact := make([]Pod, 0, len(pods))
	byName := make(map[string]*corev1.Pod, len(pods))
	for index := range pods {
		pod := &pods[index]
		compact = append(compact, Pod{Name: pod.Name, Labels: pod.Labels})
		byName[pod.Name] = pod
	}
	name, err := BrokerPod(brokerID, compact)
	if err != nil {
		return nil, err
	}
	pod := byName[name]
	if !PodReady(pod) {
		return nil, fmt.Errorf("broker pod %s is not Running and Ready", name)
	}
	return pod.DeepCopy(), nil
}
