package scheduler

import (
	"context"
	"fmt"
	"sort"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

// GangScheduler implements all-or-nothing scheduling where all tasks of a
// multi-task job must be placed simultaneously, or none are placed.
type GangScheduler struct {
	// Fallback is used for single-task jobs.
	Fallback Scheduler
}

// NewGangScheduler creates a GangScheduler with a fallback for non-gang jobs.
func NewGangScheduler(fallback Scheduler) *GangScheduler {
	if fallback == nil {
		fallback = NewBinPackScheduler()
	}
	return &GangScheduler{Fallback: fallback}
}

// Schedule assigns all tasks of gang jobs atomically, falling back to the
// inner scheduler for single-task jobs.
func (s *GangScheduler) Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error) {
	if len(pending) == 0 || len(nodes) == 0 {
		return nil, nil
	}

	// Separate gang jobs from regular jobs.
	var gangJobs, regularJobs []*Job
	for _, j := range pending {
		if j.IsGangJob() {
			gangJobs = append(gangJobs, j)
		} else {
			regularJobs = append(regularJobs, j)
		}
	}

	// Build mutable node state.
	nodeMap := make(map[string]*NodeInfo, len(nodes))
	for _, n := range nodes {
		nodeCopy := *n
		nodeMap[n.ID] = &nodeCopy
	}

	var allAssignments []*Assignment

	// Schedule gang jobs first (they are harder to fit).
	sort.Slice(gangJobs, func(i, j int) bool {
		return gangJobs[i].Priority > gangJobs[j].Priority
	})

	for _, job := range gangJobs {
		select {
		case <-ctx.Done():
			return allAssignments, ctx.Err()
		default:
		}

		assignments, err := s.scheduleGangJob(ctx, job, nodeMap)
		if err != nil {
			continue
		}
		if assignments == nil {
			continue
		}

		// All tasks placed: commit the assignments.
		for _, a := range assignments {
			node := nodeMap[a.NodeID]
			node.Available = node.Available.Sub(a.Resources)
			node.Used = node.Used.Add(a.Resources)
		}
		allAssignments = append(allAssignments, assignments...)
	}

	// Schedule remaining single-task jobs.
	if len(regularJobs) > 0 {
		currentNodes := make([]*NodeInfo, 0, len(nodeMap))
		for _, n := range nodeMap {
			currentNodes = append(currentNodes, n)
		}

		regularAssignments, err := s.Fallback.Schedule(ctx, regularJobs, currentNodes)
		if err != nil {
			return allAssignments, fmt.Errorf("fallback scheduler: %w", err)
		}
		allAssignments = append(allAssignments, regularAssignments...)
	}

	return allAssignments, nil
}

// scheduleGangJob attempts to place all tasks of a gang job.
// Returns nil if not all tasks can be placed.
func (s *GangScheduler) scheduleGangJob(_ context.Context, job *Job, nodeMap map[string]*NodeInfo) ([]*Assignment, error) {
	taskRes := job.TaskResources
	taskCount := job.TaskCount

	// Collect eligible healthy nodes sorted by available capacity descending.
	eligible := make([]*NodeInfo, 0, len(nodeMap))
	for _, n := range nodeMap {
		if n.Healthy && n.Available.Fits(taskRes) {
			eligible = append(eligible, n)
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].Available.CPU > eligible[j].Available.CPU
	})

	// Greedily assign tasks to nodes (allowing multiple tasks per node).
	assignments := make([]*Assignment, 0, taskCount)
	placed := 0

	for _, node := range eligible {
		// How many tasks can this node handle?
		tasksHere := maxTasksFit(node, taskRes)
		for t := 0; t < tasksHere && placed < taskCount; t++ {
			assignments = append(assignments, &Assignment{
				JobID:     job.ID,
				TaskIndex: placed,
				NodeID:    node.ID,
				Resources: taskRes,
			})
			placed++
		}
		if placed >= taskCount {
			break
		}
	}

	// All-or-nothing: if we cannot place all tasks, place none.
	if placed < taskCount {
		return nil, nil
	}

	return assignments, nil
}

// maxTasksFit returns how many instances of taskRes can fit in the node's
// available resources.
func maxTasksFit(node *NodeInfo, taskRes resource.Resources) int {
	avail := node.Available
	count := 0
	remaining := avail
	for remaining.Fits(taskRes) {
		remaining = remaining.Sub(taskRes)
		count++
	}
	return count
}

// Preempt delegates to the fallback scheduler's preemption logic.
func (s *GangScheduler) Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error) {
	return s.Fallback.Preempt(ctx, job, nodes)
}
