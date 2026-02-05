package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

func TestDeadlineScheduler_Schedule(t *testing.T) {
	now := time.Now()
	soon := now.Add(2 * time.Minute)
	later := now.Add(30 * time.Minute)
	expired := now.Add(-1 * time.Minute)

	tests := []struct {
		name            string
		jobs            []*Job
		nodes           []*NodeInfo
		wantAssignments int
		wantFirstJobID  string // The first assigned job (if deterministic).
	}{
		{
			name: "expired jobs are skipped",
			jobs: []*Job{
				{
					ID:        "expired",
					Priority:  PriorityCritical,
					State:     JobStatePending,
					Resources: resource.NewResources(1000, 1*resource.GiB),
					Deadline:  &expired,
					Labels:    make(map[string]string),
					CreatedAt: now,
				},
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
			},
			wantAssignments: 0,
		},
		{
			name: "urgent deadline job goes first",
			jobs: []*Job{
				makeJob("no-deadline", 1000, 1*resource.GiB),
				{
					ID:        "urgent",
					Priority:  PriorityNormal,
					State:     JobStatePending,
					Resources: resource.NewResources(1000, 1*resource.GiB),
					Deadline:  &soon,
					Labels:    make(map[string]string),
					CreatedAt: now,
				},
			},
			nodes: []*NodeInfo{
				// Only one slot available to make ordering deterministic.
				makeNode("n1", 1000, 1*resource.GiB),
			},
			wantAssignments: 1,
			wantFirstJobID:  "urgent",
		},
		{
			name: "nearest deadline prioritized",
			jobs: []*Job{
				{
					ID:        "later",
					Priority:  PriorityNormal,
					State:     JobStatePending,
					Resources: resource.NewResources(1000, 1*resource.GiB),
					Deadline:  &later,
					Labels:    make(map[string]string),
					CreatedAt: now,
				},
				{
					ID:        "sooner",
					Priority:  PriorityNormal,
					State:     JobStatePending,
					Resources: resource.NewResources(1000, 1*resource.GiB),
					Deadline:  &soon,
					Labels:    make(map[string]string),
					CreatedAt: now,
				},
			},
			nodes: []*NodeInfo{
				makeNode("n1", 1000, 1*resource.GiB), // Only fits one.
			},
			wantAssignments: 1,
			wantFirstJobID:  "sooner",
		},
		{
			name: "all jobs fit",
			jobs: []*Job{
				makeJob("j1", 1000, 1*resource.GiB),
				{
					ID:        "j2",
					Priority:  PriorityNormal,
					State:     JobStatePending,
					Resources: resource.NewResources(1000, 1*resource.GiB),
					Deadline:  &later,
					Labels:    make(map[string]string),
					CreatedAt: now,
				},
			},
			nodes: []*NodeInfo{
				makeNode("n1", 4000, 8*resource.GiB),
			},
			wantAssignments: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched := NewDeadlineScheduler(NewBinPackScheduler(), 5*time.Minute)
			ctx := context.Background()

			assignments, err := sched.Schedule(ctx, tt.jobs, tt.nodes)
			if err != nil {
				t.Fatalf("Schedule() error = %v", err)
			}

			if len(assignments) != tt.wantAssignments {
				t.Errorf("Schedule() got %d assignments, want %d",
					len(assignments), tt.wantAssignments)
			}

			if tt.wantFirstJobID != "" && len(assignments) > 0 {
				if assignments[0].JobID != tt.wantFirstJobID {
					t.Errorf("first assignment is %s, want %s",
						assignments[0].JobID, tt.wantFirstJobID)
				}
			}
		})
	}
}

func TestDeadlineUrgency(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		deadline *time.Time
		want     int
	}{
		{
			name:     "no deadline",
			deadline: nil,
			want:     0,
		},
		{
			name:     "far future",
			deadline: timePtr(now.Add(2 * time.Hour)),
			want:     0,
		},
		{
			name:     "30 minutes out",
			deadline: timePtr(now.Add(30 * time.Minute)),
			want:     50,
		},
		{
			name:     "past deadline",
			deadline: timePtr(now.Add(-10 * time.Minute)),
			want:     100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{Deadline: tt.deadline}
			got := DeadlineUrgency(job, now)
			if got != tt.want {
				t.Errorf("DeadlineUrgency() = %d, want %d", got, tt.want)
			}
		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
