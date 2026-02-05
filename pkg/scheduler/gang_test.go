package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

func TestGangScheduler_Schedule(t *testing.T) {
	tests := []struct {
		name            string
		jobs            []*Job
		nodes           []*NodeInfo
		wantAssignments int
	}{
		{
			name:            "empty inputs",
			jobs:            nil,
			nodes:           nil,
			wantAssignments: 0,
		},
		{
			name: "single-task jobs delegated to fallback",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
				makeJob("j2", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
			},
			wantAssignments: 2,
		},
		{
			name: "gang job all tasks placed",
			jobs: []*Job{
				{
					ID:            "gang1",
					Name:          "gang1",
					Priority:      PriorityNormal,
					State:         JobStatePending,
					TaskCount:     3,
					TaskResources: resource.NewResources(1000, 1*resource.GiB),
					Labels:        make(map[string]string),
					CreatedAt:     time.Now(),
				},
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 3, // All 3 tasks placed.
		},
		{
			name: "gang job cannot fit - all or nothing",
			jobs: []*Job{
				{
					ID:            "gang1",
					Name:          "gang1",
					Priority:      PriorityNormal,
					State:         JobStatePending,
					TaskCount:     5,
					TaskResources: resource.NewResources(2000, 2*resource.GiB),
					Labels:        make(map[string]string),
					CreatedAt:     time.Now(),
				},
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 4*resource.GiB),
				makeNode("n2", 4000, 4*resource.GiB),
			},
			wantAssignments: 0, // 5 tasks * 2000m = 10000m, only 8000m total.
		},
		{
			name: "gang job with mixed regular jobs",
			jobs: []*Job{
				{
					ID:            "gang1",
					Name:          "gang1",
					Priority:      PriorityHigh,
					State:         JobStatePending,
					TaskCount:     2,
					TaskResources: resource.NewResources(1000, 1*resource.GiB),
					Labels:        make(map[string]string),
					CreatedAt:     time.Now(),
				},
				makeJob("regular1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 3, // 2 gang tasks + 1 regular.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched := NewGangScheduler(NewBinPackScheduler())
			ctx := context.Background()

			assignments, err := sched.Schedule(ctx, tt.jobs, tt.nodes)
			if err != nil {
				t.Fatalf("Schedule() error = %v", err)
			}

			if len(assignments) != tt.wantAssignments {
				t.Errorf("Schedule() got %d assignments, want %d", len(assignments), tt.wantAssignments)
			}

			// Verify gang constraint: all tasks of a gang job should be assigned.
			if tt.wantAssignments > 0 {
				tasksByJob := make(map[string]int)
				for _, a := range assignments {
					tasksByJob[a.JobID]++
				}
				for _, job := range tt.jobs {
					if job.IsGangJob() {
						placed := tasksByJob[job.ID]
						if placed != 0 && placed != job.TaskCount {
							t.Errorf("gang job %s: placed %d tasks, expected 0 or %d",
								job.ID, placed, job.TaskCount)
						}
					}
				}
			}
		})
	}
}
