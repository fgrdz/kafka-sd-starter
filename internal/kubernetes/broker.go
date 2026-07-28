package kubernetes

import (
	"fmt"
	"strconv"
	"strings"
)

type Pod struct {
	Name   string
	Labels map[string]string
}

func BrokerPod(brokerID int32, pods []Pod) (string, error) {
	id := strconv.FormatInt(int64(brokerID), 10)
	for _, pod := range pods {
		if pod.Labels["strimzi.io/broker-id"] == id || pod.Labels["strimzi.io/pod-name"] == pod.Name && strings.HasSuffix(pod.Name, "-"+id) {
			return pod.Name, nil
		}
	}
	for _, pod := range pods {
		if strings.HasSuffix(pod.Name, "-"+id) {
			return pod.Name, nil
		}
	}
	return "", fmt.Errorf("no Strimzi pod found for broker %d", brokerID)
}
