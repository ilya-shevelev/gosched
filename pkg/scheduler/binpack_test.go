package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

func TestBinPackScheduler_Schedule(t *testing.T) {
	tests := []struct {
		name            string
		jobs            []*Job
		nodes           []*NodeInfo
		wantAssignments int
		wantNodeIDs     map[string]string // jobID -> expected nodeID (if deterministic)
	}{
		{
			name:            "empty inputs",
			jobs:            nil,
			nodes:           nil,
			wantAssignments: 0,
		},
		{
			name: "single job single node",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
			},
			wantAssignments: 1,
			wantNodeIDs:     map[string]string{"j1": "n1"},
		},
		{
			name: "job does not fit any node",
			jobs: []*Job{
				makeJob("j1", 8000, 16*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 0,
		},
		{
			name: "bin-pack prefers tighter fit",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNodeWithUsed("n-big", 8000, 16*resource.GiB, 0, 0),
				makeNodeWithUsed("n-small", 4000, 4*resource.GiB, 2000, 2*resource.GiB),
			},
			wantAssignments: 1,
			wantNodeIDs:     map[string]string{"j1": "n-small"},
		},
		{
			name: "multiple jobs fill nodes",
			jobs: []*Job{
				makeJob("j1", 3000, 3*resource.GiB),
				makeJob("j2", 3000, 3*resource.GiB),
				makeJob("j3", 3000, 3*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 4*resource.GiB),
				makeNode("n2", 4000, 4*resource.GiB),
			},
			wantAssignments: 2, // Each node fits only 1 job (3000m < 4000m, second won't fit).
		},
		{
			name: "unhealthy nodes are skipped",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				{
					ID:        "n-bad",
					Total:     resource.NewResources(8000, 8*resource.GiB),
					Available: resource.NewResources(8000, 8*resource.GiB),
					Healthy:   false,
				},
				makeNode("n-good", 4000, 4*resource.GiB),
			},
			wantAssignments: 1,
			wantNodeIDs:     map[string]string{"j1": "n-good"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched := NewBinPackScheduler()
			ctx := context.Background()

			assignments, err := sched.Schedule(ctx, tt.jobs, tt.nodes)
			if err != nil {
				t.Fatalf("Schedule() error = %v", err)
			}

			if len(assignments) != tt.wantAssignments {
				t.Errorf("Schedule() got %d assignments, want %d", len(assignments), tt.wantAssignments)
			}

			if tt.wantNodeIDs != nil {
				for _, a := range assignments {
					wantNode, ok := tt.wantNodeIDs[a.JobID]
					if ok && a.NodeID != wantNode {
						t.Errorf("job %s assigned to %s, want %s", a.JobID, a.NodeID, wantNode)
					}
				}
			}
		})
	}
}

func TestBinPackScheduler_Preempt(t *testing.T) {
	tests := []struct {
		name      string
		job       *Job
		nodes     []*NodeInfo
		wantPlans int
	}{
		{
			name: "preemption policy never",
			job: &Job{
				ID:               "high",
				Priority:         PriorityCritical,
				Resources:        resource.NewResources(2000, 2*resource.GiB),
				PreemptionPolicy: PreemptionPolicyNever,
			},
			nodes: []*NodeInfo{
				makeNodeWithRunning("n1", 4000, 4*resource.GiB,
					makeJob("low", 2000, 2*resource.GiB)),
			},
			wantPlans: 0,
		},
		{
			name: "preempt lower priority job",
			job: &Job{
				ID:               "high",
				Priority:         PriorityCritical,
				Resources:        resource.NewResources(2000, 2*resource.GiB),
				PreemptionPolicy: PreemptionPolicyLowerOnly,
			},
			nodes: []*NodeInfo{
				makeNodeWithRunning("n1", 4000, 4*resource.GiB,
					&Job{
						ID:        "low",
						Priority:  PriorityLow,
						Resources: resource.NewResources(2000, 2*resource.GiB),
					}),
			},
			wantPlans: 1,
		},
		{
			name: "cannot preempt equal priority",
			job: &Job{
				ID:               "same",
				Priority:         PriorityNormal,
				Resources:        resource.NewResources(4000, 4*resource.GiB),
				PreemptionPolicy: PreemptionPolicyLowerOnly,
			},
			nodes: []*NodeInfo{
				makeNodeWithRunning("n1", 4000, 4*resource.GiB,
					&Job{
						ID:        "other",
						Priority:  PriorityNormal,
						Resources: resource.NewResources(2000, 2*resource.GiB),
					}),
			},
			wantPlans: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched := NewBinPackScheduler()
			ctx := context.Background()

			plans, err := sched.Preempt(ctx, tt.job, tt.nodes)
			if err != nil {
				t.Fatalf("Preempt() error = %v", err)
			}

			if len(plans) != tt.wantPlans {
				t.Errorf("Preempt() got %d plans, want %d", len(plans), tt.wantPlans)
			}
		})
	}
}

// Helper functions for creating test data.

func makeJob(id string, cpuMillis resource.MilliCPU, memory resource.Bytes) *Job {
	return &Job{
		ID:        id,
		Name:      id,
		Priority:  PriorityNormal,
		State:     JobStatePending,
		Resources: resource.NewResources(cpuMillis, memory),
		TaskCount: 1,
		Labels:    make(map[string]string),
		CreatedAt: time.Now(),
	}
}

func makeNode(id string, cpuMillis resource.MilliCPU, memory resource.Bytes) *NodeInfo {
	total := resource.NewResources(cpuMillis, memory)
	return &NodeInfo{
		ID:        id,
		Hostname:  id,
		Labels:    make(map[string]string),
		Total:     total,
		Used:      resource.Zero(),
		Available: total,
		Healthy:   true,
		LastHeartbeat: time.Now(),
	}
}

func makeNodeWithUsed(id string, totalCPU resource.MilliCPU, totalMem resource.Bytes, usedCPU resource.MilliCPU, usedMem resource.Bytes) *NodeInfo {
	total := resource.NewResources(totalCPU, totalMem)
	used := resource.NewResources(usedCPU, usedMem)
	return &NodeInfo{
		ID:        id,
		Hostname:  id,
		Labels:    make(map[string]string),
		Total:     total,
		Used:      used,
		Available: total.Sub(used),
		Healthy:   true,
		LastHeartbeat: time.Now(),
	}
}

func makeNodeWithRunning(id string, totalCPU resource.MilliCPU, totalMem resource.Bytes, jobs ...*Job) *NodeInfo {
	total := resource.NewResources(totalCPU, totalMem)
	used := resource.Zero()
	for _, j := range jobs {
		used = used.Add(j.Resources)
	}
	return &NodeInfo{
		ID:          id,
		Hostname:    id,
		Labels:      make(map[string]string),
		Total:       total,
		Used:        used,
		Available:   total.Sub(used),
		RunningJobs: jobs,
		Healthy:     true,
		LastHeartbeat: time.Now(),
	}
}
