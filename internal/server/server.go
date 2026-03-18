// Package server implements the GoSched master server with gRPC and HTTP endpoints.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/node"
	"github.com/ilya-shevelev/gosched/pkg/observability"
	"github.com/ilya-shevelev/gosched/pkg/queue"
	"github.com/ilya-shevelev/gosched/pkg/resource"
	"github.com/ilya-shevelev/gosched/pkg/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Config holds the master server configuration.
type Config struct {
	GRPCAddr       string
	HTTPAddr       string
	SchedulePeriod time.Duration
	Algorithm      string
}

// Server is the GoSched master server.
type Server struct {
	config      Config
	logger      *slog.Logger
	grpcServer  *grpc.Server
	httpServer  *http.Server
	scheduler   scheduler.Scheduler
	queue       queue.Queue
	nodeManager *node.Manager
	quotaManager *resource.QuotaManager
	metrics     *observability.Metrics
	mu          sync.RWMutex
	jobs        map[string]*scheduler.Job
	stopCh      chan struct{}
}

// NewServer creates a new master server.
func NewServer(cfg Config, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9090"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.SchedulePeriod == 0 {
		cfg.SchedulePeriod = 1 * time.Second
	}

	quotaManager := resource.NewQuotaManager()
	nodeManager := node.NewManager(logger)

	metrics, err := observability.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("initialize metrics: %w", err)
	}

	// Select scheduling algorithm.
	var sched scheduler.Scheduler
	switch cfg.Algorithm {
	case "spread":
		sched = scheduler.NewSpreadScheduler()
	case "priority":
		sched = scheduler.NewPriorityScheduler(nil)
	case "gang":
		sched = scheduler.NewGangScheduler(nil)
	default:
		sched = scheduler.NewBinPackScheduler()
	}

	jobQueue := queue.NewFIFOQueue()

	return &Server{
		config:       cfg,
		logger:       logger,
		scheduler:    sched,
		queue:        jobQueue,
		nodeManager:  nodeManager,
		quotaManager: quotaManager,
		metrics:      metrics,
		jobs:         make(map[string]*scheduler.Job),
		stopCh:       make(chan struct{}),
	}, nil
}

// Start begins the server's gRPC, HTTP, and scheduling loops.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 3)

	// Start gRPC server.
	go func() {
		if err := s.startGRPC(); err != nil {
			errCh <- fmt.Errorf("gRPC server: %w", err)
		}
	}()

	// Start HTTP server (metrics + REST API).
	go func() {
		if err := s.startHTTP(); err != nil {
			errCh <- fmt.Errorf("HTTP server: %w", err)
		}
	}()

	// Start scheduling loop.
	go func() {
		s.schedulingLoop(ctx)
	}()

	// Start health check loop.
	go func() {
		s.healthCheckLoop(ctx)
	}()

	s.logger.Info("server started",
		"grpc_addr", s.config.GRPCAddr,
		"http_addr", s.config.HTTPAddr,
		"algorithm", s.config.Algorithm,
	)

	select {
	case <-ctx.Done():
		return s.Stop()
	case err := <-errCh:
		return err
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	close(s.stopCh)
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// SubmitJob adds a job to the scheduling queue.
func (s *Server) SubmitJob(ctx context.Context, job *scheduler.Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	if job.ID == "" {
		return fmt.Errorf("job ID is required")
	}

	s.mu.Lock()
	if _, exists := s.jobs[job.ID]; exists {
		s.mu.Unlock()
		return fmt.Errorf("job %q already exists", job.ID)
	}
	job.State = scheduler.JobStatePending
	job.CreatedAt = time.Now()
	s.jobs[job.ID] = job
	s.mu.Unlock()

	if err := s.queue.Enqueue(ctx, job); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	s.metrics.RecordJobSubmit(ctx, job.Queue)
	s.logger.Info("job submitted", "job_id", job.ID, "priority", job.Priority)
	return nil
}

// GetJob returns a job by ID.
func (s *Server) GetJob(jobID string) (*scheduler.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[jobID]
	return job, exists
}

// ListJobs returns all known jobs.
func (s *Server) ListJobs() []*scheduler.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*scheduler.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// RegisterNode registers a new compute node.
func (s *Server) RegisterNode(info *scheduler.NodeInfo) error {
	return s.nodeManager.Register(info)
}

// NodeHeartbeat processes a heartbeat from a node agent.
func (s *Server) NodeHeartbeat(nodeID string, used resource.Resources) error {
	return s.nodeManager.Heartbeat(nodeID, used)
}

func (s *Server) startGRPC() error {
	lis, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.GRPCAddr, err)
	}

	s.grpcServer = grpc.NewServer()

	// Register health check.
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(s.grpcServer, healthServer)
	healthServer.SetServingStatus("gosched", healthpb.HealthCheckResponse_SERVING)

	return s.grpcServer.Serve(lis)
}

func (s *Server) startHTTP() error {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint.
	mux.Handle("/metrics", observability.MetricsHandler())

	// Health endpoint.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// Readiness endpoint.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.nodeManager.Count() == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "no nodes registered")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// Job count endpoint.
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.RLock()
		jobCount := len(s.jobs)
		s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jobs":%d,"nodes":%d,"pending":%d}`,
			jobCount, s.nodeManager.Count(), s.queue.PendingCount())
	})

	s.httpServer = &http.Server{
		Addr:              s.config.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s.httpServer.ListenAndServe()
}

func (s *Server) schedulingLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.SchedulePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runSchedulingCycle(ctx)
		}
	}
}

func (s *Server) runSchedulingCycle(ctx context.Context) {
	start := time.Now()

	// Dequeue pending jobs.
	pending, err := s.queue.Dequeue(ctx, 100)
	if err != nil {
		s.logger.Error("dequeue failed", "error", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	// Get healthy nodes.
	nodes := s.nodeManager.Healthy()
	if len(nodes) == 0 {
		// Re-enqueue jobs since there are no nodes.
		for _, job := range pending {
			_ = s.queue.Enqueue(ctx, job)
		}
		return
	}

	// Run scheduler.
	assignments, err := s.scheduler.Schedule(ctx, pending, nodes)
	duration := time.Since(start)
	s.metrics.RecordSchedulingCycle(ctx, duration, len(assignments), err)

	if err != nil {
		s.logger.Error("scheduling failed", "error", err, "duration", duration)
		// Re-enqueue unassigned jobs.
		for _, job := range pending {
			_ = s.queue.Enqueue(ctx, job)
		}
		return
	}

	// Apply assignments.
	assigned := make(map[string]bool)
	for _, a := range assignments {
		if err := s.nodeManager.AllocateJob(a.NodeID, s.jobs[a.JobID]); err != nil {
			s.logger.Error("allocation failed", "job_id", a.JobID, "node_id", a.NodeID, "error", err)
			continue
		}

		s.mu.Lock()
		if job, ok := s.jobs[a.JobID]; ok {
			job.State = scheduler.JobStateRunning
			now := time.Now()
			job.StartedAt = &now
		}
		s.mu.Unlock()

		assigned[a.JobID] = true
		s.logger.Info("job assigned", "job_id", a.JobID, "node_id", a.NodeID)
	}

	// Re-enqueue unassigned jobs.
	for _, job := range pending {
		if !assigned[job.ID] {
			_ = s.queue.Enqueue(ctx, job)
		}
	}

	if len(assignments) > 0 {
		s.logger.Info("scheduling cycle completed",
			"assigned", len(assignments),
			"pending", s.queue.PendingCount(),
			"duration", duration,
		)
	}
}

func (s *Server) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			unhealthy := s.nodeManager.CheckHealth()
			for _, nodeID := range unhealthy {
				s.logger.Warn("node unhealthy", "node_id", nodeID)
			}
		}
	}
}
