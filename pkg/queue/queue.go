// Package queue implements job queue strategies including FIFO, fair-share,
// and capacity queues for multi-tenant resource scheduling.
package queue

import (
	"context"
	"fmt"
	"sync"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// Queue is the interface for job queues.
type Queue interface {
	// Enqueue adds a job to the queue.
	Enqueue(ctx context.Context, job *scheduler.Job) error
	// Dequeue removes and returns up to count jobs from the queue.
	Dequeue(ctx context.Context, count int) ([]*scheduler.Job, error)
	// Peek returns up to count jobs without removing them.
	Peek(ctx context.Context, count int) ([]*scheduler.Job, error)
	// PendingCount returns the number of pending jobs.
	PendingCount() int
}

// FIFOQueue is a simple first-in-first-out queue.
type FIFOQueue struct {
	mu   sync.Mutex
	jobs []*scheduler.Job
}

// NewFIFOQueue creates a new FIFOQueue.
func NewFIFOQueue() *FIFOQueue {
	return &FIFOQueue{
		jobs: make([]*scheduler.Job, 0),
	}
}

// Enqueue adds a job to the back of the queue.
func (q *FIFOQueue) Enqueue(_ context.Context, job *scheduler.Job) error {
	if job == nil {
		return fmt.Errorf("cannot enqueue nil job")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, job)
	return nil
}

// Dequeue removes and returns up to count jobs from the front.
func (q *FIFOQueue) Dequeue(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}
	n := min(count, len(q.jobs))
	result := make([]*scheduler.Job, n)
	copy(result, q.jobs[:n])
	q.jobs = q.jobs[n:]
	return result, nil
}

// Peek returns up to count jobs from the front without removing them.
func (q *FIFOQueue) Peek(_ context.Context, count int) ([]*scheduler.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if count <= 0 {
		return nil, nil
	}
	n := min(count, len(q.jobs))
	result := make([]*scheduler.Job, n)
	copy(result, q.jobs[:n])
	return result, nil
}

// PendingCount returns the number of pending jobs.
func (q *FIFOQueue) PendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}
