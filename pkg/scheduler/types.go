// Package scheduler implements scheduling algorithms for GoSched including
// bin-packing, spreading, gang scheduling, priority-based preemption, and
// deadline-aware scheduling.
package scheduler

import (
	"context"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

// JobState represents the lifecycle state of a job.
type JobState string

// Job states.
const (
	JobStatePending   JobState = "PENDING"
	JobStateRunning   JobState = "RUNNING"
	JobStateCompleted JobState = "COMPLETED"
	JobStateFailed    JobState = "FAILED"
	JobStateCancelled JobState = "CANCELLED"
	JobStatePreempted JobState = "PREEMPTED"
)

// Priority levels for jobs (higher value = higher priority).
type Priority int32

// Standard priority levels.
const (
	PriorityLow      Priority = 100
	PriorityNormal   Priority = 500
	PriorityHigh     Priority = 1000
	PriorityCritical Priority = 10000
)

// Job represents a unit of work to be scheduled.
type Job struct {
	ID        string
	Name      string
	Tenant    string
	Queue     string
	Priority  Priority
	State     JobState
	Resources resource.Resources
	// TaskCount is the number of parallel tasks (for gang scheduling).
	TaskCount int
	// TaskResources is the resource requirement per task.
	TaskResources resource.Resources
	// Deadline is an optional deadline by which the job should complete.
	Deadline *time.Time
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int
	// Labels are key-value metadata for filtering and affinity.
	Labels map[string]string
	// NodeSelector restricts scheduling to nodes matching these labels.
	NodeSelector map[string]string
	// CreatedAt is when the job was submitted.
	CreatedAt time.Time
	// StartedAt is when the job began executing.
	StartedAt *time.Time
	// PreemptionPolicy controls whether this job can preempt others.
	PreemptionPolicy PreemptionPolicy
}

// PreemptionPolicy controls preemption behavior.
type PreemptionPolicy string

// Preemption policies.
const (
	PreemptionPolicyNever      PreemptionPolicy = "Never"
	PreemptionPolicyLowerOnly  PreemptionPolicy = "LowerPriorityOnly"
	PreemptionPolicyAlways     PreemptionPolicy = "Always"
)

// IsGangJob returns true if this job requires gang scheduling.
func (j *Job) IsGangJob() bool {
	return j.TaskCount > 1
}

// TotalResources returns the total resources needed for all tasks.
func (j *Job) TotalResources() resource.Resources {
	if j.TaskCount <= 1 {
		return j.Resources
	}
	total := resource.Zero()
	for i := 0; i < j.TaskCount; i++ {
		total = total.Add(j.TaskResources)
	}
	return total
}

// IsExpired returns true if the job's deadline has passed.
func (j *Job) IsExpired() bool {
	if j.Deadline == nil {
		return false
	}
	return time.Now().After(*j.Deadline)
}

// NodeInfo represents a compute node's current state for scheduling.
type NodeInfo struct {
	ID        string
	Hostname  string
	Labels    map[string]string
	Taints    []Taint
	Total     resource.Resources
	Used      resource.Resources
	Available resource.Resources
	GPUs      []resource.GPUDevice
	NUMA      *resource.NUMATopology
	// RunningJobs are currently allocated jobs on this node.
	RunningJobs []*Job
	// Healthy indicates if the node is accepting new work.
	Healthy bool
	// LastHeartbeat is the most recent heartbeat from the node agent.
	LastHeartbeat time.Time
}

// Taint marks a node with scheduling restrictions.
type Taint struct {
	Key    string
	Value  string
	Effect TaintEffect
}

// TaintEffect describes the effect of a taint.
type TaintEffect string

// Taint effects.
const (
	TaintEffectNoSchedule       TaintEffect = "NoSchedule"
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule"
	TaintEffectNoExecute        TaintEffect = "NoExecute"
)

// Assignment maps a job (or task) to a specific node.
type Assignment struct {
	JobID     string
	TaskIndex int
	NodeID    string
	Resources resource.Resources
}

// PreemptionPlan describes which jobs need to be preempted to make room.
type PreemptionPlan struct {
	// VictimJobIDs are the IDs of jobs to be preempted.
	VictimJobIDs []string
	// NodeID is the node where preemption would occur.
	NodeID string
	// FreedResources is how much capacity preemption would release.
	FreedResources resource.Resources
}

// Scheduler is the core scheduling interface.
type Scheduler interface {
	// Schedule assigns pending jobs to available nodes.
	Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error)
	// Preempt computes a plan to evict lower-priority jobs to make room.
	Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error)
}

// SchedulerPlugin extends the scheduling pipeline with custom logic.
type SchedulerPlugin interface {
	// Name returns the plugin identifier.
	Name() string
	// Filter removes ineligible nodes for a job.
	Filter(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*NodeInfo, error)
	// Score ranks eligible nodes (higher is better, 0-100).
	Score(ctx context.Context, job *Job, nodes []*NodeInfo) (map[string]int, error)
	// Reserve claims resources on the chosen node before binding.
	Reserve(ctx context.Context, job *Job, node *NodeInfo) error
	// Bind finalizes the assignment of a job to a node.
	Bind(ctx context.Context, job *Job, node *NodeInfo) error
}
