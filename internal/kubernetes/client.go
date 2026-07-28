package kubernetes

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type PodController interface {
	DeletePod(context.Context, string, string) error
}

type Client struct {
	client kubernetes.Interface
}

func New(client kubernetes.Interface) *Client {
	return &Client{client: client}
}

func (c *Client) DeletePod(ctx context.Context, namespace, name string) error {
	grace := int64(0)
	if err := c.client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		return fmt.Errorf("force delete pod %s/%s: %w", namespace, name, err)
	}
	return nil
}
