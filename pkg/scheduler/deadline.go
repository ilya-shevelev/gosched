package scheduler

import (
	"context"
	"sort"
	"time"
)

// DeadlineScheduler prioritizes jobs with approaching deadlines, ensuring
// time-sensitive work is scheduled before it expires.
type DeadlineScheduler struct {
	inner Scheduler
	// GracePeriod is the minimum time before deadline at which we attempt
	// to schedule. Jobs within this window get highest priority.
	GracePeriod time.Duration
}

// NewDeadlineScheduler wraps an inner scheduler with deadline-aware ordering.
func NewDeadlineScheduler(inner Scheduler, gracePeriod time.Duration) *DeadlineScheduler {
	if inner == nil {
		inner = NewBinPackScheduler()
	}
	if gracePeriod == 0 {
		gracePeriod = 5 * time.Minute
	}
	return &DeadlineScheduler{
		inner:       inner,
		GracePeriod: gracePeriod,
	}
}

// Schedule reorders pending jobs to prioritize those with nearest deadlines,
// then delegates to the inner scheduler.
func (s *DeadlineScheduler) Schedule(ctx context.Context, pending []*Job, nodes []*NodeInfo) ([]*Assignment, error) {
	if len(pending) == 0 {
		return nil, nil
	}

	now := time.Now()

	// Filter out expired jobs.
	var active []*Job
	for _, job := range pending {
		if job.Deadline != nil && job.Deadline.Before(now) {
			// Skip expired jobs.
			continue
		}
		active = append(active, job)
	}

	if len(active) == 0 {
		return nil, nil
	}

	// Sort: deadline jobs first (nearest deadline first), then non-deadline
	// jobs by priority.
	sort.Slice(active, func(i, j int) bool {
		di := active[i].Deadline
		dj := active[j].Deadline

		iUrgent := di != nil && di.Sub(now) <= s.GracePeriod
		jUrgent := dj != nil && dj.Sub(now) <= s.GracePeriod

		// Both urgent: nearest deadline first.
		if iUrgent && jUrgent {
			return di.Before(*dj)
		}
		// Urgent beats non-urgent.
		if iUrgent != jUrgent {
			return iUrgent
		}

		// Both have deadlines but not urgent: nearest first.
		if di != nil && dj != nil {
			return di.Before(*dj)
		}
		// Deadline beats no-deadline.
		if (di != nil) != (dj != nil) {
			return di != nil
		}

		// Neither has a deadline: fall back to priority.
		if active[i].Priority != active[j].Priority {
			return active[i].Priority > active[j].Priority
		}
		return active[i].CreatedAt.Before(active[j].CreatedAt)
	})

	return s.inner.Schedule(ctx, active, nodes)
}

// Preempt generates preemption plans giving preference to deadline-urgent jobs.
func (s *DeadlineScheduler) Preempt(ctx context.Context, job *Job, nodes []*NodeInfo) ([]*PreemptionPlan, error) {
	return s.inner.Preempt(ctx, job, nodes)
}

// DeadlineUrgency returns a score from 0 to 100 indicating how urgent a job is
// based on its deadline proximity. 100 means the deadline is imminent or past.
// 0 means no deadline or far in the future.
func DeadlineUrgency(job *Job, now time.Time) int {
	if job.Deadline == nil {
		return 0
	}
	remaining := job.Deadline.Sub(now)
	if remaining <= 0 {
		return 100
	}
	// Scale from 0 to 100 over a 1-hour window.
	window := time.Hour
	if remaining >= window {
		return 0
	}
	fraction := 1.0 - float64(remaining)/float64(window)
	return int(fraction * 100)
}
