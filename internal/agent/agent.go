// Package agent implements the GoSched node agent that runs on each compute
// node, manages cgroup resource isolation, reports node status, and executes
// workloads.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// Config holds the agent configuration.
type Config struct {
	NodeID          string
	MasterAddr      string
	HeartbeatPeriod time.Duration
	Labels          map[string]string
	DataDir         string
}

// Agent is the node agent that manages workloads on a single compute node.
type Agent struct {
	config    Config
	logger    *slog.Logger
	mu        sync.RWMutex
	running   map[string]*WorkloadHandle
	nodeInfo  *scheduler.NodeInfo
	stopCh    chan struct{}
}

// WorkloadHandle tracks a running workload.
type WorkloadHandle struct {
	Job       *scheduler.Job
	StartedAt time.Time
	Cancel    context.CancelFunc
	Done      chan struct{}
	ExitCode  int
	Err       error
}

// NewAgent creates a new node agent.
func NewAgent(cfg Config, logger *slog.Logger) (*Agent, error) {
	if cfg.NodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("get hostname: %w", err)
		}
		cfg.NodeID = hostname
	}
	if cfg.HeartbeatPeriod == 0 {
		cfg.HeartbeatPeriod = 10 * time.Second
	}
	if cfg.Labels == nil {
		cfg.Labels = make(map[string]string)
	}
	if logger == nil {
		logger = slog.Default()
	}

	nodeInfo := detectNodeResources(cfg)

	return &Agent{
		config:   cfg,
		logger:   logger,
		running:  make(map[string]*WorkloadHandle),
		nodeInfo: nodeInfo,
		stopCh:   make(chan struct{}),
	}, nil
}

// NodeInfo returns the current node information.
func (a *Agent) NodeInfo() *scheduler.NodeInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nodeInfo
}

// Start begins the agent's heartbeat loop.
func (a *Agent) Start(ctx context.Context) error {
	a.logger.Info("agent starting",
		"node_id", a.config.NodeID,
		"master", a.config.MasterAddr,
	)

	ticker := time.NewTicker(a.config.HeartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.stopCh:
			return nil
		case <-ticker.C:
			a.sendHeartbeat()
		}
	}
}

// Stop gracefully shuts down the agent.
func (a *Agent) Stop() {
	close(a.stopCh)

	a.mu.Lock()
	defer a.mu.Unlock()

	for id, handle := range a.running {
		a.logger.Info("stopping workload", "job_id", id)
		handle.Cancel()
		<-handle.Done
	}
	a.logger.Info("agent stopped", "node_id", a.config.NodeID)
}

// Launch starts a workload on this node.
func (a *Agent) Launch(parentCtx context.Context, job *scheduler.Job) (*WorkloadHandle, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.running[job.ID]; exists {
		return nil, fmt.Errorf("job %q already running on this node", job.ID)
	}

	// Check available resources.
	if !a.nodeInfo.Available.Fits(job.Resources) {
		return nil, fmt.Errorf("insufficient resources on node %q for job %q", a.config.NodeID, job.ID)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	handle := &WorkloadHandle{
		Job:       job,
		StartedAt: time.Now(),
		Cancel:    cancel,
		Done:      make(chan struct{}),
	}

	// Set up cgroup isolation.
	cgroupPath, err := setupCgroup(job.ID, job.Resources)
	if err != nil {
		cancel()
		a.logger.Warn("cgroup setup failed, continuing without isolation",
			"job_id", job.ID, "error", err)
	}

	// Update resource tracking.
	a.nodeInfo.Used = a.nodeInfo.Used.Add(job.Resources)
	a.nodeInfo.Available = a.nodeInfo.Total.Sub(a.nodeInfo.Used)
	a.nodeInfo.RunningJobs = append(a.nodeInfo.RunningJobs, job)
	a.running[job.ID] = handle

	// Execute workload in background.
	go func() {
		defer close(handle.Done)
		defer func() {
			a.releaseResources(job)
			if cgroupPath != "" {
				cleanupCgroup(cgroupPath, a.logger)
			}
		}()

		executor := selectExecutor(job)
		handle.ExitCode, handle.Err = executor.Execute(ctx, job)

		if handle.Err != nil {
			a.logger.Error("workload failed", "job_id", job.ID, "error", handle.Err)
		} else {
			a.logger.Info("workload completed", "job_id", job.ID, "exit_code", handle.ExitCode)
		}
	}()

	a.logger.Info("launched workload",
		"job_id", job.ID,
		"resources", job.Resources.String(),
	)
	return handle, nil
}

// Kill terminates a running workload.
func (a *Agent) Kill(jobID string) error {
	a.mu.RLock()
	handle, exists := a.running[jobID]
	a.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job %q not running on this node", jobID)
	}

	handle.Cancel()
	<-handle.Done
	return nil
}

// RunningJobs returns the IDs of currently running workloads.
func (a *Agent) RunningJobs() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	ids := make([]string, 0, len(a.running))
	for id := range a.running {
		ids = append(ids, id)
	}
	return ids
}

// CurrentUsage returns the current resource usage.
func (a *Agent) CurrentUsage() resource.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nodeInfo.Used
}

func (a *Agent) releaseResources(job *scheduler.Job) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.nodeInfo.Used = a.nodeInfo.Used.Sub(job.Resources)
	a.nodeInfo.Available = a.nodeInfo.Total.Sub(a.nodeInfo.Used)

	// Remove from running jobs.
	filtered := make([]*scheduler.Job, 0, len(a.nodeInfo.RunningJobs))
	for _, j := range a.nodeInfo.RunningJobs {
		if j.ID != job.ID {
			filtered = append(filtered, j)
		}
	}
	a.nodeInfo.RunningJobs = filtered
	delete(a.running, job.ID)
}

func (a *Agent) sendHeartbeat() {
	a.mu.RLock()
	used := a.nodeInfo.Used
	a.mu.RUnlock()

	a.logger.Debug("sending heartbeat",
		"node_id", a.config.NodeID,
		"used_cpu", used.CPU,
		"used_memory", used.Memory,
		"running_jobs", len(a.running),
	)
}

// detectNodeResources probes the system for available resources.
func detectNodeResources(cfg Config) *scheduler.NodeInfo {
	cpuCores := runtime.NumCPU()

	// Use sysinfo for memory; fall back to a reasonable default.
	var memBytes int64 = 8 * int64(resource.GiB) // Default 8GiB.

	total := resource.Resources{
		CPU:    resource.MilliCPU(cpuCores) * resource.MilliCPUPerCore,
		Memory: resource.Bytes(memBytes),
		Custom: make(map[string]int64),
	}

	return &scheduler.NodeInfo{
		ID:        cfg.NodeID,
		Hostname:  cfg.NodeID,
		Labels:    cfg.Labels,
		Total:     total,
		Used:      resource.Zero(),
		Available: total,
		Healthy:   true,
		LastHeartbeat: time.Now(),
	}
}
