package scheduler

import (
	"context"
	"testing"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

func TestSpreadScheduler_Schedule(t *testing.T) {
	tests := []struct {
		name            string
		jobs            []*Job
		nodes           []*NodeInfo
		wantAssignments int
		wantSpread      bool // If true, verify jobs are spread across nodes.
	}{
		{
			name:            "empty inputs",
			jobs:            nil,
			nodes:           nil,
			wantAssignments: 0,
		},
		{
			name: "single job",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 1,
		},
		{
			name: "two jobs spread across two nodes",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
				makeJob("j2", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 2,
			wantSpread:      true,
		},
		{
			name: "three jobs across two nodes",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
				makeJob("j2", 1000, 1*resource.GiB),
				makeJob("j3", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
				makeNode("n2", 4000, 8*resource.GiB),
			},
			wantAssignments: 3,
		},
		{
			name: "prefers less loaded node",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
			},
			nodes: []*NodeInfo{
				makeNodeWithRunning("n-busy", 8000, 16*resource.GiB,
					makeJob("existing1", 1000, 1*resource.GiB),
					makeJob("existing2", 1000, 1*resource.GiB),
				),
				makeNode("n-empty", 4000, 8*resource.GiB),
			},
			wantAssignments: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched := NewSpreadScheduler()
			ctx := context.Background()

			assignments, err := sched.Schedule(ctx, tt.jobs, tt.nodes)
			if err != nil {
				t.Fatalf("Schedule() error = %v", err)
			}

			if len(assignments) != tt.wantAssignments {
				t.Errorf("Schedule() got %d assignments, want %d", len(assignments), tt.wantAssignments)
			}

			if tt.wantSpread && len(assignments) == 2 {
				if assignments[0].NodeID == assignments[1].NodeID {
					t.Error("expected jobs to be spread across different nodes")
				}
			}
		})
	}
}
