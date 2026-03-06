// Package node provides node management, health tracking, and discovery
// for the GoSched scheduling system.
package node

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// HeartbeatTimeout is the duration after which a node is considered unhealthy
// if no heartbeat is received.
const HeartbeatTimeout = 30 * time.Second

// Manager tracks all known nodes and their health status.
type Manager struct {
	mu     sync.RWMutex
	nodes  map[string]*scheduler.NodeInfo
	logger *slog.Logger
}

// NewManager creates a new node Manager.
func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		nodes:  make(map[string]*scheduler.NodeInfo),
		logger: logger,
	}
}

// Register adds a new node to the cluster.
func (m *Manager) Register(info *scheduler.NodeInfo) error {
	if info == nil {
		return fmt.Errorf("node info is nil")
	}
	if info.ID == "" {
		return fmt.Errorf("node ID is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[info.ID]; exists {
		return fmt.Errorf("node %q already registered", info.ID)
	}

	info.Healthy = true
	info.LastHeartbeat = time.Now()
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}
	m.nodes[info.ID] = info
	m.logger.Info("node registered", "node_id", info.ID, "hostname", info.Hostname)
	return nil
}

// Deregister removes a node from the cluster.
func (m *Manager) Deregister(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	if len(node.RunningJobs) > 0 {
		return fmt.Errorf("node %q has %d running jobs; drain first", nodeID, len(node.RunningJobs))
	}
	delete(m.nodes, nodeID)
	m.logger.Info("node deregistered", "node_id", nodeID)
	return nil
}

// Heartbeat updates the last heartbeat time and resource usage for a node.
func (m *Manager) Heartbeat(nodeID string, used resource.Resources) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	node.LastHeartbeat = time.Now()
	node.Used = used
	node.Available = node.Total.Sub(used)
	node.Healthy = true
	return nil
}

// Get returns a node by ID.
func (m *Manager) Get(nodeID string) (*scheduler.NodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, false
	}
	return node, true
}

// List returns all registered nodes.
func (m *Manager) List() []*scheduler.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*scheduler.NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// Healthy returns only healthy nodes.
func (m *Manager) Healthy() []*scheduler.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*scheduler.NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		if n.Healthy {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// CheckHealth marks nodes as unhealthy if their heartbeat has expired.
// Returns the list of newly unhealthy node IDs.
func (m *Manager) CheckHealth() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var unhealthy []string

	for _, node := range m.nodes {
		if node.Healthy && now.Sub(node.LastHeartbeat) > HeartbeatTimeout {
			node.Healthy = false
			unhealthy = append(unhealthy, node.ID)
			m.logger.Warn("node marked unhealthy",
				"node_id", node.ID,
				"last_heartbeat", node.LastHeartbeat,
			)
		}
	}
	return unhealthy
}

// UpdateLabels sets labels on a node.
func (m *Manager) UpdateLabels(nodeID string, labels map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	for k, v := range labels {
		node.Labels[k] = v
	}
	return nil
}

// AddTaint adds a taint to a node.
func (m *Manager) AddTaint(nodeID string, taint scheduler.Taint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	// Check for duplicate.
	for _, t := range node.Taints {
		if t.Key == taint.Key && t.Effect == taint.Effect {
			return nil
		}
	}
	node.Taints = append(node.Taints, taint)
	return nil
}

// RemoveTaint removes a taint from a node by key and effect.
func (m *Manager) RemoveTaint(nodeID, key string, effect scheduler.TaintEffect) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	filtered := make([]scheduler.Taint, 0, len(node.Taints))
	for _, t := range node.Taints {
		if t.Key == key && t.Effect == effect {
			continue
		}
		filtered = append(filtered, t)
	}
	node.Taints = filtered
	return nil
}

// AllocateJob records a job as running on a node.
func (m *Manager) AllocateJob(nodeID string, job *scheduler.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	if !node.Available.Fits(job.Resources) {
		return fmt.Errorf("node %q has insufficient resources", nodeID)
	}
	node.RunningJobs = append(node.RunningJobs, job)
	node.Used = node.Used.Add(job.Resources)
	node.Available = node.Total.Sub(node.Used)
	return nil
}

// DeallocateJob removes a job from a node's running list.
func (m *Manager) DeallocateJob(nodeID, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %q not found", nodeID)
	}
	filtered := make([]*scheduler.Job, 0, len(node.RunningJobs))
	var found bool
	for _, j := range node.RunningJobs {
		if j.ID == jobID {
			node.Used = node.Used.Sub(j.Resources)
			found = true
			continue
		}
		filtered = append(filtered, j)
	}
	if !found {
		return fmt.Errorf("job %q not found on node %q", jobID, nodeID)
	}
	node.RunningJobs = filtered
	node.Available = node.Total.Sub(node.Used)
	return nil
}

// Count returns the total number of registered nodes.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}
