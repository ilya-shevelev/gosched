package scheduler

import (
	"context"
	"sort"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

// BinPackScheduler implements a bin-packing strategy that minimizes resource
// fragmentation by preferring nodes with the least available resources that
// still fit the job's requirements.
type BinPackScheduler struct {
	plugins []SchedulerPlugin
}

// NewBinPackScheduler creates a new BinPackScheduler with optional plugins.
func NewBinPackScheduler(plugins ...SchedulerPlugin) *BinPackScheduler {
	return &BinPackScheduler{plugins: plugins}
}

// Schedule assigns jobs to nodes using bin-packing heuristic.
func (s *BinPackScheduler) Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error) {
	if len(pending) == 0 || len(nodes) == 0 {
		return nil, nil
	}

	// Sort jobs by resource demand descending (largest first fit).
	sortedJobs := make([]*Job, len(pending))
	copy(sortedJobs, pending)
	sort.Slice(sortedJobs, func(i, j int) bool {
		ri := sortedJobs[i].Resources
		rj := sortedJobs[j].Resources
		return ri.CPU+MilliCPUFromMemory(ri.Memory) > rj.CPU+MilliCPUFromMemory(rj.Memory)
	})

	// Track remaining capacity per node.
	available := make(map[string]*NodeInfo, len(nodes))
	for _, n := range nodes {
		if !n.Healthy {
			continue
		}
		nodeCopy := *n
		available[n.ID] = &nodeCopy
	}

	var assignments []*Assignment

	for _, job := range sortedJobs {
		select {
		case <-ctx.Done():
			return assignments, ctx.Err()
		default:
		}

		// Apply plugin filters.
		eligible, err := s.filterNodes(ctx, job, available)
		if err != nil {
			continue
		}
		if len(eligible) == 0 {
			continue
		}

		// Score nodes: prefer those with least remaining resources (most packed).
		scores, err := s.scoreNodes(ctx, job, eligible)
		if err != nil {
			continue
		}

		bestNode := selectBestBinPack(job, eligible, scores)
		if bestNode == nil {
			continue
		}

		// Reserve resources.
		if err := s.reserveNode(ctx, job, bestNode); err != nil {
			continue
		}

		assignments = append(assignments, &Assignment{
			JobID:     job.ID,
			TaskIndex: 0,
			NodeID:    bestNode.ID,
			Resources: job.Resources,
		})

		// Update available resources on the node.
		bestNode.Available = bestNode.Available.Sub(job.Resources)
		bestNode.Used = bestNode.Used.Add(job.Resources)
	}

	return assignments, nil
}

// Preempt computes preemption plans for a job using bin-pack strategy.
func (s *BinPackScheduler) Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error) {
	if job.PreemptionPolicy == PreemptionPolicyNever {
		return nil, nil
	}

	var plans []*PreemptionPlan

	for _, node := range nodes {
		if !node.Healthy {
			continue
		}

		// Find preemptable jobs on this node (lower priority only).
		var victims []*Job
		freed := node.Available
		for _, running := range node.RunningJobs {
			if running.Priority >= job.Priority {
				continue
			}
			if job.PreemptionPolicy == PreemptionPolicyLowerOnly && running.Priority >= job.Priority {
				continue
			}
			victims = append(victims, running)
			freed = freed.Add(running.Resources)
			if freed.Fits(job.Resources) {
				break
			}
		}

		if freed.Fits(job.Resources) && len(victims) > 0 {
			victimIDs := make([]string, len(victims))
			for i, v := range victims {
				victimIDs[i] = v.ID
			}
			plans = append(plans, &PreemptionPlan{
				VictimJobIDs:   victimIDs,
				NodeID:         node.ID,
				FreedResources: freed,
			})
		}
	}

	// Prefer plans that evict fewer jobs.
	sort.Slice(plans, func(i, j int) bool {
		return len(plans[i].VictimJobIDs) < len(plans[j].VictimJobIDs)
	})

	return plans, nil
}

func (s *BinPackScheduler) filterNodes(ctx context.Context, job *Job, available map[string]*NodeInfo) ([]*NodeInfo, error) {
	eligible := make([]*NodeInfo, 0)
	for _, n := range available {
		if n.Available.Fits(job.Resources) {
			eligible = append(eligible, n)
		}
	}

	for _, plugin := range s.plugins {
		var err error
		eligible, err = plugin.Filter(ctx, job, eligible)
		if err != nil {
			return nil, err
		}
	}
	return eligible, nil
}

func (s *BinPackScheduler) scoreNodes(ctx context.Context, job *Job, nodes []*NodeInfo) (map[string]int, error) {
	scores := make(map[string]int, len(nodes))
	for _, n := range nodes {
		scores[n.ID] = 0
	}

	for _, plugin := range s.plugins {
		pluginScores, err := plugin.Score(ctx, job, nodes)
		if err != nil {
			return nil, err
		}
		for id, score := range pluginScores {
			scores[id] += score
		}
	}
	return scores, nil
}

func (s *BinPackScheduler) reserveNode(ctx context.Context, job *Job, node *NodeInfo) error {
	for _, plugin := range s.plugins {
		if err := plugin.Reserve(ctx, job, node); err != nil {
			return err
		}
	}
	return nil
}

// selectBestBinPack selects the node with least remaining resources after placement.
func selectBestBinPack(job *Job, nodes []*NodeInfo, scores map[string]int) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	type candidate struct {
		node      *NodeInfo
		remaining float64
		score     int
	}

	candidates := make([]candidate, 0, len(nodes))
	for _, n := range nodes {
		after := n.Available.Sub(job.Resources)
		remaining := float64(after.CPU) + float64(after.Memory)/float64(1024*1024*1024)
		candidates = append(candidates, candidate{
			node:      n,
			remaining: remaining,
			score:     scores[n.ID],
		})
	}

	// Sort by plugin score descending, then by remaining ascending (bin-pack).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].remaining < candidates[j].remaining
	})

	return candidates[0].node
}

// MilliCPUFromMemory converts memory bytes to a comparable milliCPU-scale value
// for combined sorting. This uses a rough equivalence of 1 GiB = 1000 milliCPU.
func MilliCPUFromMemory(mem resource.Bytes) resource.MilliCPU {
	return resource.MilliCPU(int64(mem) / int64(resource.GiB) * int64(resource.MilliCPUPerCore))
}
