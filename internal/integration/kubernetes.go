package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// K8sClient provides helpers for interacting with a Kubernetes API server.
type K8sClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// K8sNode represents a Kubernetes node.
type K8sNode struct {
	Metadata K8sMetadata `json:"metadata"`
	Status   struct {
		Capacity    map[string]string    `json:"capacity"`
		Allocatable map[string]string    `json:"allocatable"`
		Conditions  []K8sNodeCondition   `json:"conditions"`
	} `json:"status"`
}

// K8sMetadata holds common Kubernetes object metadata.
type K8sMetadata struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// K8sNodeCondition represents a Kubernetes node condition.
type K8sNodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// K8sPod represents a Kubernetes pod.
type K8sPod struct {
	Metadata K8sMetadata `json:"metadata"`
	Spec     struct {
		NodeName   string `json:"nodeName"`
		Containers []struct {
			Name      string `json:"name"`
			Image     string `json:"image"`
			Resources struct {
				Requests map[string]string `json:"requests"`
				Limits   map[string]string `json:"limits"`
			} `json:"resources"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// NewK8sClient creates a Kubernetes API client.
func NewK8sClient(baseURL, token string) *K8sClient {
	return &K8sClient{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ListNodes returns all nodes in the cluster.
func (c *K8sClient) ListNodes(ctx context.Context) ([]K8sNode, error) {
	var result struct {
		Items []K8sNode `json:"items"`
	}
	if err := c.get(ctx, "/api/v1/nodes", &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// GetNode returns a specific node by name.
func (c *K8sClient) GetNode(ctx context.Context, name string) (*K8sNode, error) {
	var node K8sNode
	if err := c.get(ctx, "/api/v1/nodes/"+name, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// ListPods returns pods in a namespace (empty namespace = all namespaces).
func (c *K8sClient) ListPods(ctx context.Context, namespace string) ([]K8sPod, error) {
	path := "/api/v1/pods"
	if namespace != "" {
		path = fmt.Sprintf("/api/v1/namespaces/%s/pods", namespace)
	}

	var result struct {
		Items []K8sPod `json:"items"`
	}
	if err := c.get(ctx, path, &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// IsNodeReady checks if a Kubernetes node is in Ready condition.
func IsNodeReady(node *K8sNode) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == "Ready" {
			return cond.Status == "True"
		}
	}
	return false
}

func (c *K8sClient) get(ctx context.Context, path string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
