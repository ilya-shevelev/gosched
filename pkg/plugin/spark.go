package plugin

import (
	"context"
	"log/slog"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// SparkPlugin is a scheduler plugin optimized for Apache Spark workloads.
// It prefers nodes with data locality, co-locates executors with drivers,
// and respects Spark-specific scheduling constraints.
type SparkPlugin struct {
	logger *slog.Logger
}

// NewSparkPlugin creates a new SparkPlugin.
func NewSparkPlugin(logger *slog.Logger) *SparkPlugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &SparkPlugin{logger: logger}
}

// Name returns the plugin name.
func (p *SparkPlugin) Name() string {
	return "spark"
}

// Filter removes nodes that are not suitable for Spark workloads.
func (p *SparkPlugin) Filter(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) ([]*scheduler.NodeInfo, error) {
	if !isSparkJob(job) {
		return nodes, nil
	}

	eligible := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		// Spark executor needs at least 1 core and 512MiB memory.
		if node.Available.CPU < 1000 {
			continue
		}
		if node.Available.Memory < 512*1024*1024 {
			continue
		}
		eligible = append(eligible, node)
	}

	if len(eligible) == 0 {
		p.logger.Warn("no nodes eligible for Spark job", "job_id", job.ID)
		return nodes, nil // Fall back to all nodes rather than failing.
	}
	return eligible, nil
}

// Score ranks nodes for Spark workloads. Prefers nodes with:
// - Data locality (hdfs-data label)
// - Existing Spark executors for the same application (co-location)
// - More available memory (Spark is memory-intensive).
func (p *SparkPlugin) Score(_ context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) (map[string]int, error) {
	scores := make(map[string]int, len(nodes))
	if !isSparkJob(job) {
		return scores, nil
	}

	appID := job.Labels["spark.app.id"]

	for _, node := range nodes {
		score := 0

		// Prefer data nodes for data locality.
		if node.Labels["storage.type"] == "hdfs" || node.Labels["data.local"] == "true" {
			score += 30
		}

		// Prefer co-locating executors of the same Spark application.
		if appID != "" {
			for _, running := range node.RunningJobs {
				if running.Labels["spark.app.id"] == appID {
					score += 20
					break
				}
			}
		}

		// Prefer nodes with more available memory (Spark is memory-hungry).
		memGiB := float64(node.Available.Memory) / float64(1024*1024*1024)
		if memGiB > 16 {
			score += 30
		} else if memGiB > 8 {
			score += 20
		} else if memGiB > 4 {
			score += 10
		}

		// Bonus for GPU nodes when the job requests GPUs (Spark Rapids).
		if job.Resources.GPU.Count > 0 && len(node.GPUs) > 0 {
			score += 20
		}

		scores[node.ID] = min(score, 100)
	}
	return scores, nil
}

// Reserve is a no-op for Spark -- reservation is handled by the scheduler.
func (p *SparkPlugin) Reserve(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

// Bind is a no-op for Spark -- binding is handled by the scheduler.
func (p *SparkPlugin) Bind(_ context.Context, _ *scheduler.Job, _ *scheduler.NodeInfo) error {
	return nil
}

func isSparkJob(job *scheduler.Job) bool {
	if job.Labels == nil {
		return false
	}
	return job.Labels["workload.type"] == "spark" || job.Labels["spark.app.id"] != ""
}
