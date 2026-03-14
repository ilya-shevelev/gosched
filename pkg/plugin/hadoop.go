package plugin

import (
	"context"
	"log/slog"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// HadoopPlugin is a scheduler plugin optimized for Hadoop MapReduce workloads.
// It prioritizes data locality and rack awareness.
type HadoopPlugin struct {
	logger *slog.Logger
}

// NewHadoopPlugin creates a new HadoopPlugin.
func NewHadoopPlugin(logger *slog.Logger) *HadoopPlugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &HadoopPlugin{logger: logger}
}

// Name returns the plugin name.
func (p *HadoopPlugin) Name() string {
	return "hadoop"
}

// Filter removes nodes unsuitable for Hadoop workloads.
func (p *HadoopPlugin) Filter(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) ([]*scheduler.NodeInfo, error) {
	if !isHadoopJob(job) {
		return nodes, nil
	}

	eligible := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		// Hadoop tasks need at least 1 core and 1GiB memory.
		if node.Available.CPU < 1000 || node.Available.Memory < 1024*1024*1024 {
			continue
		}
		eligible = append(eligible, node)
	}

	if len(eligible) == 0 {
		p.logger.Warn("no nodes eligible for Hadoop job", "job_id", job.ID)
		return nodes, nil
	}
	return eligible, nil
}

// Score ranks nodes for Hadoop workloads prioritizing data locality and rack awareness.
func (p *HadoopPlugin) Score(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) (map[string]int, error) {
	scores := make(map[string]int, len(nodes))
	if !isHadoopJob(job) {
		return scores, nil
	}

	preferredRack := job.Labels["hadoop.preferred.rack"]
	preferredNode := job.Labels["hadoop.preferred.node"]

	for _, node := range nodes {
		score := 0

		// Node-level data locality (highest priority).
		if preferredNode != "" && node.Hostname == preferredNode {
			score += 50
		}

		// Rack-level data locality.
		if preferredRack != "" && node.Labels["topology.rack"] == preferredRack {
			score += 30
		}

		// Prefer nodes with HDFS data.
		if node.Labels["storage.type"] == "hdfs" {
			score += 10
		}

		// Prefer nodes with more local storage.
		if node.Available.Storage > 100*1024*1024*1024 { // >100GiB
			score += 10
		}

		scores[node.ID] = min(score, 100)
	}
	return scores, nil
}

// Reserve is a no-op for Hadoop.
func (p *HadoopPlugin) Reserve(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

// Bind is a no-op for Hadoop.
func (p *HadoopPlugin) Bind(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

func isHadoopJob(job *scheduler.Job) bool {
	if job.Labels == nil {
		return false
	}
	return job.Labels["workload.type"] == "hadoop" || job.Labels["hadoop.app.id"] != ""
}
