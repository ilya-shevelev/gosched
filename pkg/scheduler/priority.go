package scheduler

import (
	"context"
	"sort"
)

// PriorityScheduler schedules higher-priority jobs first, with preemption
// support for evicting lower-priority work when needed.
type PriorityScheduler struct {
	inner   Scheduler
	plugins []SchedulerPlugin
}

// NewPriorityScheduler wraps an inner scheduler with priority ordering and
// preemption capabilities.
func NewPriorityScheduler(inner Scheduler, plugins ...SchedulerPlugin) *PriorityScheduler {
	if inner == nil {
		inner = NewBinPackScheduler(plugins...)
	}
	return &PriorityScheduler{
		inner:   inner,
		plugins: plugins,
	}
}

// Schedule orders pending jobs by priority (descending) before delegating
// to the inner scheduler.
func (s *PriorityScheduler) Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error) {
	if len(pending) == 0 {
		return nil, nil
	}

	// Sort by priority descending, then by creation time ascending (FIFO within same priority).
	sorted := make([]*Job, len(pending))
	copy(sorted, pending)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Priority != sorted[j].Priority {
			return sorted[i].Priority > sorted[j].Priority
		}
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	return s.inner.Schedule(ctx, sorted, nodes)
}

// Preempt finds preemption plans to make room for a high-priority job.
// It evaluates all nodes and returns plans sorted by cost (fewest evictions).
func (s *PriorityScheduler) Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error) {
	if job.PreemptionPolicy == PreemptionPolicyNever {
		return nil, nil
	}

	var plans []*PreemptionPlan

	for _, node := range nodes {
		if !node.Healthy {
			continue
		}

		plan := computePreemptionPlan(job, node)
		if plan != nil {
			plans = append(plans, plan)
		}
	}

	// Sort plans by number of victims (prefer fewer evictions).
	sort.Slice(plans, func(i, j int) bool {
		li, lj := len(plans[i].VictimJobIDs), len(plans[j].VictimJobIDs)
		if li != lj {
			return li < lj
		}
		// Tie-break: prefer more freed resources.
		return plans[i].FreedResources.CPU > plans[j].FreedResources.CPU
	})

	return plans, nil
}

// computePreemptionPlan determines which jobs on a node should be preempted
// to make room for the given job.
func computePreemptionPlan(job *Job, node *NodeInfo) *PreemptionPlan {
	// Collect preemptable victims (lower priority).
	type victim struct {
		job *Job
	}
	var candidates []victim
	for _, running := range node.RunningJobs {
		if running.Priority < job.Priority {
			candidates = append(candidates, victim{job: running})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort candidates by priority ascending (evict lowest first),
	// then by resource usage descending (evict biggest first).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].job.Priority != candidates[j].job.Priority {
			return candidates[i].job.Priority < candidates[j].job.Priority
		}
		return candidates[i].job.Resources.CPU > candidates[j].job.Resources.CPU
	})

	// Greedily select victims until we have enough resources.
	freed := node.Available
	var victimIDs []string
	for _, c := range candidates {
		freed = freed.Add(c.job.Resources)
		victimIDs = append(victimIDs, c.job.ID)
		if freed.Fits(job.Resources) {
			return &PreemptionPlan{
				VictimJobIDs:   victimIDs,
				NodeID:         node.ID,
				FreedResources: freed,
			}
		}
	}

	// Even after evicting all candidates, not enough resources.
	return nil
}
