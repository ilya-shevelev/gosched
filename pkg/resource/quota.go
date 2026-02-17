package resource

import (
	"fmt"
	"sync"
)

// Quota defines resource limits for a tenant or queue.
type Quota struct {
	// Max is the hard upper limit of resources.
	Max Resources
	// Guaranteed is the minimum guaranteed resources.
	Guaranteed Resources
}

// QuotaManager tracks resource quotas and usage per tenant.
type QuotaManager struct {
	mu     sync.RWMutex
	quotas map[string]*quotaEntry
}

type quotaEntry struct {
	quota Quota
	used  Resources
}

// NewQuotaManager creates a new QuotaManager.
func NewQuotaManager() *QuotaManager {
	return &QuotaManager{
		quotas: make(map[string]*quotaEntry),
	}
}

// SetQuota sets or updates the quota for a tenant.
func (qm *QuotaManager) SetQuota(tenant string, quota Quota) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		qm.quotas[tenant] = &quotaEntry{
			quota: quota,
			used:  Zero(),
		}
		return
	}
	entry.quota = quota
}

// RemoveQuota removes a tenant's quota. Returns an error if the tenant has
// resources currently allocated.
func (qm *QuotaManager) RemoveQuota(tenant string) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return fmt.Errorf("quota for tenant %q does not exist", tenant)
	}
	if !entry.used.IsZero() {
		return fmt.Errorf("cannot remove quota for tenant %q: resources still in use", tenant)
	}
	delete(qm.quotas, tenant)
	return nil
}

// TryAllocate attempts to allocate resources for a tenant. Returns true if the
// allocation fits within the tenant's quota, false otherwise.
func (qm *QuotaManager) TryAllocate(tenant string, request Resources) bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return false
	}
	proposed := entry.used.Add(request)
	if !entry.quota.Max.Fits(proposed) {
		return false
	}
	entry.used = proposed
	return true
}

// Release returns resources back to a tenant's quota pool.
func (qm *QuotaManager) Release(tenant string, released Resources) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return
	}
	entry.used = entry.used.Sub(released)
}

// Usage returns the current resource usage for a tenant.
func (qm *QuotaManager) Usage(tenant string) (Resources, bool) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return Zero(), false
	}
	return entry.used, true
}

// Available returns the remaining allocatable resources for a tenant.
func (qm *QuotaManager) Available(tenant string) (Resources, bool) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return Zero(), false
	}
	return entry.quota.Max.Sub(entry.used), true
}

// GetQuota returns the quota for a tenant.
func (qm *QuotaManager) GetQuota(tenant string) (Quota, bool) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return Quota{}, false
	}
	return entry.quota, true
}

// Tenants returns all tenant names with quotas.
func (qm *QuotaManager) Tenants() []string {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	tenants := make([]string, 0, len(qm.quotas))
	for t := range qm.quotas {
		tenants = append(tenants, t)
	}
	return tenants
}

// FairShareRatio returns the fraction of guaranteed resources currently used
// by a tenant. Values > 1.0 mean the tenant is using more than its guaranteed
// share (borrowing). Returns 0 if no guaranteed resources are set.
func (qm *QuotaManager) FairShareRatio(tenant string) float64 {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	entry, exists := qm.quotas[tenant]
	if !exists {
		return 0
	}
	return entry.quota.Guaranteed.UsageRatio(entry.used)
}
