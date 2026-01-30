package scheduler

import (
	"context"
	"sort"
)

// SpreadScheduler distributes jobs evenly across nodes to maximize
// fault tolerance and avoid hotspots.
type SpreadScheduler struct {
	plugins []SchedulerPlugin
}

// NewSpreadScheduler creates a new SpreadScheduler with optional plugins.
func NewSpreadScheduler(plugins ...SchedulerPlugin) *SpreadScheduler {
	return &SpreadScheduler{plugins: plugins}
}

// Schedule assigns jobs to nodes using an even-distribution strategy.
func (s *SpreadScheduler) Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error) {
	if len(pending) == 0 || len(nodes) == 0 {
		return nil, nil
	}

	// Track allocation counts per node for spreading.
	allocationCount := make(map[string]int, len(nodes))
	for _, n := range nodes {
		allocationCount[n.ID] = len(n.RunningJobs)
	}

	// Build mutable node state.
	available := make(map[string]*NodeInfo, len(nodes))
	for _, n := range nodes {
		if !n.Healthy {
			continue
		}
		nodeCopy := *n
		available[n.ID] = &nodeCopy
	}

	var assignments []*Assignment

	for _, job := range pending {
		select {
		case <-ctx.Done():
			return assignments, ctx.Err()
		default:
		}

		// Filter nodes that can fit this job.
		eligible := make([]*NodeInfo, 0)
		for _, n := range available {
			if n.Available.Fits(job.Resources) {
				eligible = append(eligible, n)
			}
		}

		// Apply plugin filters.
		for _, plugin := range s.plugins {
			var err error
			eligible, err = plugin.Filter(ctx, job, eligible)
			if err != nil {
				break
			}
		}

		if len(eligible) == 0 {
			continue
		}

		// Get plugin scores.
		pluginScores := make(map[string]int, len(eligible))
		for _, plugin := range s.plugins {
			scores, err := plugin.Score(ctx, job, eligible)
			if err != nil {
				continue
			}
			for id, score := range scores {
				pluginScores[id] += score
			}
		}

		// Sort by least loaded (spread), then by plugin score.
		sort.Slice(eligible, func(i, j int) bool {
			ci := allocationCount[eligible[i].ID]
			cj := allocationCount[eligible[j].ID]
			if ci != cj {
				return ci < cj
			}
			si := pluginScores[eligible[i].ID]
			sj := pluginScores[eligible[j].ID]
			if si != sj {
				return si > sj
			}
			// Tie-break: prefer node with more available resources.
			ri := eligible[i].Available
			rj := eligible[j].Available
			return ri.CPU > rj.CPU
		})

		bestNode := eligible[0]

		// Reserve through plugins.
		reserved := true
		for _, plugin := range s.plugins {
			if err := plugin.Reserve(ctx, job, bestNode); err != nil {
				reserved = false
				break
			}
		}
		if !reserved {
			continue
		}

		assignments = append(assignments, &Assignment{
			JobID:     job.ID,
			TaskIndex: 0,
			NodeID:    bestNode.ID,
			Resources: job.Resources,
		})

		// Update tracking state.
		bestNode.Available = bestNode.Available.Sub(job.Resources)
		bestNode.Used = bestNode.Used.Add(job.Resources)
		allocationCount[bestNode.ID]++
	}

	return assignments, nil
}

// Preempt computes preemption plans preferring nodes with the most running jobs
// (to maintain spread after preemption).
func (s *SpreadScheduler) Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error) {
	if job.PreemptionPolicy == PreemptionPolicyNever {
		return nil, nil
	}

	var plans []*PreemptionPlan

	// Sort nodes by most running jobs (prefer preempting from busiest nodes).
	sortedNodes := make([]*NodeInfo, len(nodes))
	copy(sortedNodes, nodes)
	sort.Slice(sortedNodes, func(i, j int) bool {
		return len(sortedNodes[i].RunningJobs) > len(sortedNodes[j].RunningJobs)
	})

	for _, node := range sortedNodes {
		if !node.Healthy {
			continue
		}

		var victims []*Job
		freed := node.Available
		// Sort running jobs by priority ascending (evict lowest first).
		running := make([]*Job, len(node.RunningJobs))
		copy(running, node.RunningJobs)
		sort.Slice(running, func(i, j int) bool {
			return running[i].Priority < running[j].Priority
		})

		for _, candidate := range running {
			if candidate.Priority >= job.Priority {
				continue
			}
			victims = append(victims, candidate)
			freed = freed.Add(candidate.Resources)
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

	return plans, nil
}
