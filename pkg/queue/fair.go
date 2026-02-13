package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/ilya-shevelev/gosched/pkg/resource"
	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// FairQueue implements fair-share scheduling across multiple tenants. Each tenant
// gets a proportional share of dequeued jobs based on their quota weight.
type FairQueue struct {
	mu           sync.Mutex
	tenantQueues map[string]*tenantQueue
	quotaManager *resource.QuotaManager
}

type tenantQueue struct {
	tenant string
	jobs   []*scheduler.Job
	weight float64 // Relative share weight (higher = more share).
	served int     // Jobs served in the current round.
}

// NewFairQueue creates a new FairQueue backed by a quota manager.
func NewFairQueue(qm *resource.QuotaManager) *FairQueue {
	return &FairQueue{
		tenantQueues: make(map[string]*tenantQueue),
		quotaManager: qm,
	}
}

// SetWeight sets the fair-share weight for a tenant.
func (q *FairQueue) SetWeight(tenant string, weight float64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	tq, exists := q.tenantQueues[tenant]
	if !exists {
		q.tenantQueues[tenant] = &tenantQueue{
			tenant: tenant,
			jobs:   make([]*scheduler.Job, 0),
			weight: weight,
		}
		return
	}
	tq.weight = weight
}

// Enqueue adds a job to the tenant's sub-queue.
func (q *FairQueue) Enqueue(_ context.Context, job *scheduler.Job) error {
	if job == nil {
		return fmt.Errorf("cannot enqueue nil job")
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	tenant := job.Tenant
	if tenant == "" {
		tenant = "default"
	}

	tq, exists := q.tenantQueues[tenant]
	if !exists {
		tq = &tenantQueue{
			tenant: tenant,
			jobs:   make([]*scheduler.Job, 0),
			weight: 1.0,
		}
		q.tenantQueues[tenant] = tq
	}
	tq.jobs = append(tq.jobs, job)
	return nil
}

// Dequeue returns up to count jobs, distributed fairly across tenants
// according to their weights.
func (q *FairQueue) Dequeue(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}

	// Collect tenants with pending jobs.
	active := make([]*tenantQueue, 0)
	for _, tq := range q.tenantQueues {
		if len(tq.jobs) > 0 {
			active = append(active, tq)
		}
	}
	if len(active) == 0 {
		return nil, nil
	}

	// Compute fair shares. Each tenant gets a proportional number of slots.
	totalWeight := 0.0
	for _, tq := range active {
		totalWeight += tq.weight
	}

	// Sort by fair-share ratio ascending (least served first).
	sort.Slice(active, func(i, j int) bool {
		ri := q.quotaManager.FairShareRatio(active[i].tenant)
		rj := q.quotaManager.FairShareRatio(active[j].tenant)
		return ri < rj
	})

	var result []*scheduler.Job
	remaining := count

	// Round-robin with weighted allocation.
	for remaining > 0 {
		anyDequeued := false
		for _, tq := range active {
			if remaining <= 0 || len(tq.jobs) == 0 {
				continue
			}
			// Each tenant gets slots proportional to weight.
			share := max(1, int(float64(count)*tq.weight/totalWeight))
			take := min(share, remaining, len(tq.jobs))
			result = append(result, tq.jobs[:take]...)
			tq.jobs = tq.jobs[take:]
			tq.served += take
			remaining -= take
			anyDequeued = true
		}
		if !anyDequeued {
			break
		}
	}

	return result, nil
}

// Peek returns up to count jobs across all tenants without removing them.
func (q *FairQueue) Peek(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}

	var result []*scheduler.Job
	for _, tq := range q.tenantQueues {
		for _, job := range tq.jobs {
			if len(result) >= count {
				return result, nil
			}
			result = append(result, job)
		}
	}
	return result, nil
}

// PendingCount returns the total number of pending jobs across all tenants.
func (q *FairQueue) PendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	total := 0
	for _, tq := range q.tenantQueues {
		total += len(tq.jobs)
	}
	return total
}
