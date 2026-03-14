package plugin

import (
	"context"
	"log/slog"
	"strings"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// KubernetesPlugin extends scheduling with Kubernetes-style constraints
// including pod topology spread, resource limits, and namespace awareness.
type KubernetesPlugin struct {
	logger *slog.Logger
}

// NewKubernetesPlugin creates a new KubernetesPlugin.
func NewKubernetesPlugin(logger *slog.Logger) *KubernetesPlugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &KubernetesPlugin{logger: logger}
}

// Name returns the plugin name.
func (p *KubernetesPlugin) Name() string {
	return "kubernetes"
}

// Filter applies Kubernetes-style filtering.
func (p *KubernetesPlugin) Filter(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) ([]*scheduler.NodeInfo, error) {
	if !isK8sJob(job) {
		return nodes, nil
	}

	eligible := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		// Check node selector.
		if !matchK8sNodeSelector(job, node) {
			continue
		}
		// Check taint tolerance (simplified).
		if hasUntolerated(job, node) {
			continue
		}
		eligible = append(eligible, node)
	}

	if len(eligible) == 0 {
		p.logger.Warn("no nodes match Kubernetes constraints", "job_id", job.ID)
		return nodes, nil
	}
	return eligible, nil
}

// Score implements Kubernetes-style scoring including topology spread and
// balanced resource allocation.
func (p *KubernetesPlugin) Score(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) (map[string]int, error) {
	scores := make(map[string]int, len(nodes))
	if !isK8sJob(job) {
		return scores, nil
	}

	namespace := job.Labels["k8s.namespace"]

	for _, node := range nodes {
		score := 0

		// Topology spread: prefer nodes with fewer jobs in the same namespace.
		if namespace != "" {
			sameNS := 0
			for _, running := range node.RunningJobs {
				if running.Labels != nil && running.Labels["k8s.namespace"] == namespace {
					sameNS++
				}
			}
			// Fewer co-located = higher score.
			if sameNS == 0 {
				score += 40
			} else if sameNS < 3 {
				score += 20
			}
		}

		// Balanced resource allocation: prefer nodes where CPU and memory
		// utilization are close to each other (avoid skew).
		if node.Total.CPU > 0 && node.Total.Memory > 0 {
			cpuRatio := float64(node.Used.CPU) / float64(node.Total.CPU)
			memRatio := float64(node.Used.Memory) / float64(node.Total.Memory)
			skew := cpuRatio - memRatio
			if skew < 0 {
				skew = -skew
			}
			// Lower skew = better balance = higher score.
			balanceScore := int((1 - skew) * 40)
			score += balanceScore
		}

		// Prefer same-zone nodes for low-latency communication.
		jobZone := job.Labels["topology.zone"]
		nodeZone := node.Labels["topology.zone"]
		if jobZone != "" && jobZone == nodeZone {
			score += 20
		}

		scores[node.ID] = min(score, 100)
	}
	return scores, nil
}

// Reserve is a no-op for Kubernetes.
func (p *KubernetesPlugin) Reserve(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

// Bind is a no-op for Kubernetes.
func (p *KubernetesPlugin) Bind(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

func isK8sJob(job *scheduler.Job) bool {
	if job.Labels == nil {
		return false
	}
	for k := range job.Labels {
		if strings.HasPrefix(k, "k8s.") {
			return true
		}
	}
	return false
}

func matchK8sNodeSelector(job *scheduler.Job, node *scheduler.NodeInfo) bool {
	for k, v := range job.NodeSelector {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}

func hasUntolerated(job *scheduler.Job, node *scheduler.NodeInfo) bool {
	for _, taint := range node.Taints {
		if taint.Effect != scheduler.TaintEffectNoSchedule {
			continue
		}
		tolerated := false
		// Check if job tolerates this taint via labels.
		tolKey := "toleration." + taint.Key
		if job.Labels[tolKey] == taint.Value || job.Labels[tolKey] == "*" {
			tolerated = true
		}
		if !tolerated {
			return true
		}
	}
	return false
}
