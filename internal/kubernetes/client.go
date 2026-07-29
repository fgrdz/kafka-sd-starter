package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type PodController interface {
	DeletePod(context.Context, string, string) error
}

type PodClient interface {
	GetPod(context.Context, string, string) (*corev1.Pod, error)
	ListPods(context.Context, string, string) ([]corev1.Pod, error)
	DeletePod(context.Context, string, string) error
	WatchPod(context.Context, string, string) (watch.Interface, error)
	ListEvents(context.Context, string) ([]corev1.Event, error)
}

type Client struct {
	client kubernetes.Interface
}

func New(client kubernetes.Interface) *Client {
	return &Client{client: client}
}

func NewInCluster() (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create in-cluster Kubernetes client: %w", err)
	}
	return New(client), nil
}

func (c *Client) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pod, err := c.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod %s/%s: %w", namespace, name, err)
	}
	return pod, nil
}

func (c *Client) ListPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error) {
	list, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list pods in %s: %w", namespace, err)
	}
	return list.Items, nil
}

func (c *Client) DeletePod(ctx context.Context, namespace, name string) error {
	grace := int64(0)
	if err := c.client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		return fmt.Errorf("force delete pod %s/%s: %w", namespace, name, err)
	}
	return nil
}

func (c *Client) WatchPod(ctx context.Context, namespace, name string) (watch.Interface, error) {
	result, err := c.client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + name,
	})
	if err != nil {
		return nil, fmt.Errorf("watch pod %s/%s: %w", namespace, name, err)
	}
	return result, nil
}

func (c *Client) ListEvents(ctx context.Context, namespace string) ([]corev1.Event, error) {
	list, err := c.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list events in %s: %w", namespace, err)
	}
	return list.Items, nil
}

func PodReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
