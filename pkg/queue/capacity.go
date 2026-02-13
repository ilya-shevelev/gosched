package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// CapacityQueue implements a capacity-based queue where each sub-queue has a
// guaranteed capacity percentage. Jobs from under-utilized queues can borrow
// capacity from over-provisioned queues.
type CapacityQueue struct {
	mu         sync.Mutex
	subQueues  map[string]*capacitySubQueue
	totalSlots int
}

type capacitySubQueue struct {
	name        string
	jobs        []*scheduler.Job
	capacityPct float64 // Guaranteed capacity as a percentage (0-100).
	maxPct      float64 // Maximum allowed capacity including borrowed (0-100).
	served      int     // Jobs served in current accounting period.
}

// CapacityQueueConfig configures a sub-queue.
type CapacityQueueConfig struct {
	Name        string
	CapacityPct float64 // Guaranteed capacity percentage.
	MaxPct      float64 // Maximum capacity percentage (for borrowing).
}

// NewCapacityQueue creates a new CapacityQueue.
func NewCapacityQueue(totalSlots int, configs []CapacityQueueConfig) (*CapacityQueue, error) {
	subQueues := make(map[string]*capacitySubQueue, len(configs))
	totalPct := 0.0

	for _, cfg := range configs {
		if cfg.CapacityPct < 0 || cfg.CapacityPct > 100 {
			return nil, fmt.Errorf("invalid capacity percentage for queue %q: %.1f", cfg.Name, cfg.CapacityPct)
		}
		maxPct := cfg.MaxPct
		if maxPct == 0 {
			maxPct = 100
		}
		if maxPct < cfg.CapacityPct {
			return nil, fmt.Errorf("max capacity for queue %q must be >= guaranteed capacity", cfg.Name)
		}
		subQueues[cfg.Name] = &capacitySubQueue{
			name:        cfg.Name,
			jobs:        make([]*scheduler.Job, 0),
			capacityPct: cfg.CapacityPct,
			maxPct:      maxPct,
		}
		totalPct += cfg.CapacityPct
	}

	if totalPct > 100 {
		return nil, fmt.Errorf("total guaranteed capacity exceeds 100%%: %.1f%%", totalPct)
	}

	return &CapacityQueue{
		subQueues:  subQueues,
		totalSlots: totalSlots,
	}, nil
}

// Enqueue adds a job to the specified sub-queue (determined by job.Queue).
func (q *CapacityQueue) Enqueue(_ context.Context, job *scheduler.Job) error {
	if job == nil {
		return fmt.Errorf("cannot enqueue nil job")
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	queueName := job.Queue
	if queueName == "" {
		queueName = "default"
	}

	sq, exists := q.subQueues[queueName]
	if !exists {
		return fmt.Errorf("queue %q does not exist", queueName)
	}
	sq.jobs = append(sq.jobs, job)
	return nil
}

// Dequeue returns up to count jobs, respecting capacity guarantees and
// allowing borrowing from underutilized queues.
func (q *CapacityQueue) Dequeue(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}

	// Phase 1: Each queue gets its guaranteed share.
	var result []*scheduler.Job
	remaining := count
	leftover := make([]*capacitySubQueue, 0)

	// Sort queues by utilization (least utilized first).
	active := make([]*capacitySubQueue, 0, len(q.subQueues))
	for _, sq := range q.subQueues {
		active = append(active, sq)
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].utilizationRatio() < active[j].utilizationRatio()
	})

	for _, sq := range active {
		if remaining <= 0 || len(sq.jobs) == 0 {
			continue
		}
		guaranteed := max(1, int(float64(count)*sq.capacityPct/100.0))
		take := min(guaranteed, remaining, len(sq.jobs))
		result = append(result, sq.jobs[:take]...)
		sq.jobs = sq.jobs[take:]
		sq.served += take
		remaining -= take

		if len(sq.jobs) > 0 {
			leftover = append(leftover, sq)
		}
	}

	// Phase 2: Distribute remaining slots to queues that can borrow.
	for _, sq := range leftover {
		if remaining <= 0 || len(sq.jobs) == 0 {
			continue
		}
		maxSlots := int(float64(q.totalSlots) * sq.maxPct / 100.0)
		canBorrow := maxSlots - sq.served
		if canBorrow <= 0 {
			continue
		}
		take := min(canBorrow, remaining, len(sq.jobs))
		result = append(result, sq.jobs[:take]...)
		sq.jobs = sq.jobs[take:]
		sq.served += take
		remaining -= take
	}

	return result, nil
}

// Peek returns up to count jobs without removing them.
func (q *CapacityQueue) Peek(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}

	var result []*scheduler.Job
	for _, sq := range q.subQueues {
		for _, job := range sq.jobs {
			if len(result) >= count {
				return result, nil
			}
			result = append(result, job)
		}
	}
	return result, nil
}

// PendingCount returns the total number of pending jobs.
func (q *CapacityQueue) PendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	total := 0
	for _, sq := range q.subQueues {
		total += len(sq.jobs)
	}
	return total
}

// ResetCounters resets the served counters for all sub-queues.
// Call this periodically to reset borrowing accounting.
func (q *CapacityQueue) ResetCounters() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, sq := range q.subQueues {
		sq.served = 0
	}
}

func (sq *capacitySubQueue) utilizationRatio() float64 {
	if sq.capacityPct == 0 {
		return 0
	}
	return float64(sq.served) / sq.capacityPct
}
