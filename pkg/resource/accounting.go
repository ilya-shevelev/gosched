package resource

import (
	"sync"
	"time"
)

// AccountingEntry records resource usage over a time window.
type AccountingEntry struct {
	Tenant    string
	JobID     string
	Resources Resources
	StartTime time.Time
	EndTime   time.Time
}

// Accountant tracks resource usage for chargeback and fair-share calculations.
type Accountant struct {
	mu      sync.RWMutex
	entries []AccountingEntry
	active  map[string]*AccountingEntry // keyed by jobID
}

// NewAccountant creates a new Accountant.
func NewAccountant() *Accountant {
	return &Accountant{
		entries: make([]AccountingEntry, 0),
		active:  make(map[string]*AccountingEntry),
	}
}

// StartUsage begins tracking resource usage for a job.
func (a *Accountant) StartUsage(tenant, jobID string, res Resources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.active[jobID] = &AccountingEntry{
		Tenant:    tenant,
		JobID:     jobID,
		Resources: res,
		StartTime: time.Now(),
	}
}

// StopUsage stops tracking usage for a job and records the final entry.
func (a *Accountant) StopUsage(jobID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	entry, exists := a.active[jobID]
	if !exists {
		return
	}
	entry.EndTime = time.Now()
	a.entries = append(a.entries, *entry)
	delete(a.active, jobID)
}

// TenantUsage returns total CPU-seconds and memory-byte-seconds for a tenant
// within the given time window.
func (a *Accountant) TenantUsage(tenant string, from, to time.Time) (cpuSeconds float64, memoryByteSeconds float64) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for i := range a.entries {
		e := &a.entries[i]
		if e.Tenant != tenant {
			continue
		}
		overlap := overlapDuration(e.StartTime, e.EndTime, from, to)
		if overlap <= 0 {
			continue
		}
		secs := overlap.Seconds()
		cpuSeconds += float64(e.Resources.CPU) * secs / float64(MilliCPUPerCore)
		memoryByteSeconds += float64(e.Resources.Memory) * secs
	}

	// Also account for currently active jobs.
	now := time.Now()
	for _, e := range a.active {
		if e.Tenant != tenant {
			continue
		}
		endTime := now
		if endTime.After(to) {
			endTime = to
		}
		overlap := overlapDuration(e.StartTime, endTime, from, to)
		if overlap <= 0 {
			continue
		}
		secs := overlap.Seconds()
		cpuSeconds += float64(e.Resources.CPU) * secs / float64(MilliCPUPerCore)
		memoryByteSeconds += float64(e.Resources.Memory) * secs
	}

	return cpuSeconds, memoryByteSeconds
}

// ActiveJobs returns the count of currently tracked active jobs.
func (a *Accountant) ActiveJobs() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.active)
}

// CompletedEntries returns all completed accounting entries. The returned slice
// is a copy safe for concurrent use.
func (a *Accountant) CompletedEntries() []AccountingEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]AccountingEntry, len(a.entries))
	copy(result, a.entries)
	return result
}

// overlapDuration returns the overlap between two time ranges.
func overlapDuration(aStart, aEnd, bStart, bEnd time.Time) time.Duration {
	start := aStart
	if bStart.After(start) {
		start = bStart
	}
	end := aEnd
	if bEnd.Before(end) {
		end = bEnd
	}
	if end.Before(start) {
		return 0
	}
	return end.Sub(start)
}
