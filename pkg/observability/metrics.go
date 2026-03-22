// Package observability provides metrics, tracing, and structured logging
// for the GoSched scheduling system using OpenTelemetry and Prometheus.
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
)

// Metrics holds all GoSched metrics instruments.
type Metrics struct {
	// Scheduling metrics.
	SchedulingAttempts  metric.Int64Counter
	SchedulingDuration  metric.Float64Histogram
	SchedulingErrors    metric.Int64Counter
	AssignmentsCreated  metric.Int64Counter

	// Job metrics.
	JobsSubmitted   metric.Int64Counter
	JobsCompleted   metric.Int64Counter
	JobsFailed      metric.Int64Counter
	JobsPreempted   metric.Int64Counter
	JobQueueDepth   metric.Int64UpDownCounter
	JobWaitTime     metric.Float64Histogram

	// Node metrics.
	NodesRegistered   metric.Int64UpDownCounter
	NodesHealthy      metric.Int64UpDownCounter
	NodeCPUUsage      metric.Float64Gauge
	NodeMemoryUsage   metric.Float64Gauge
	NodeGPUUsage      metric.Float64Gauge

	// Cluster metrics.
	ClusterCPUTotal     metric.Float64Gauge
	ClusterCPUUsed      metric.Float64Gauge
	ClusterMemoryTotal  metric.Float64Gauge
	ClusterMemoryUsed   metric.Float64Gauge

	meter metric.Meter
}

// NewMetrics initializes all metric instruments.
func NewMetrics() (*Metrics, error) {
	exporter, err := otelprometheus.New()
	if err != nil {
		return nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	meter := provider.Meter("gosched")
	m := &Metrics{meter: meter}

	// Initialize all instruments.
	m.SchedulingAttempts, err = meter.Int64Counter("gosched_scheduling_attempts_total",
		metric.WithDescription("Total number of scheduling attempts"))
	if err != nil {
		return nil, err
	}

	m.SchedulingDuration, err = meter.Float64Histogram("gosched_scheduling_duration_seconds",
		metric.WithDescription("Duration of scheduling cycles"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
	if err != nil {
		return nil, err
	}

	m.SchedulingErrors, err = meter.Int64Counter("gosched_scheduling_errors_total",
		metric.WithDescription("Total number of scheduling errors"))
	if err != nil {
		return nil, err
	}

	m.AssignmentsCreated, err = meter.Int64Counter("gosched_assignments_created_total",
		metric.WithDescription("Total number of job assignments created"))
	if err != nil {
		return nil, err
	}

	m.JobsSubmitted, err = meter.Int64Counter("gosched_jobs_submitted_total",
		metric.WithDescription("Total number of jobs submitted"))
	if err != nil {
		return nil, err
	}

	m.JobsCompleted, err = meter.Int64Counter("gosched_jobs_completed_total",
		metric.WithDescription("Total number of jobs completed"))
	if err != nil {
		return nil, err
	}

	m.JobsFailed, err = meter.Int64Counter("gosched_jobs_failed_total",
		metric.WithDescription("Total number of jobs failed"))
	if err != nil {
		return nil, err
	}

	m.JobsPreempted, err = meter.Int64Counter("gosched_jobs_preempted_total",
		metric.WithDescription("Total number of jobs preempted"))
	if err != nil {
		return nil, err
	}

	m.JobQueueDepth, err = meter.Int64UpDownCounter("gosched_job_queue_depth",
		metric.WithDescription("Current number of jobs in the queue"))
	if err != nil {
		return nil, err
	}

	m.JobWaitTime, err = meter.Float64Histogram("gosched_job_wait_time_seconds",
		metric.WithDescription("Time jobs spend waiting in queue"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600))
	if err != nil {
		return nil, err
	}

	m.NodesRegistered, err = meter.Int64UpDownCounter("gosched_nodes_registered",
		metric.WithDescription("Current number of registered nodes"))
	if err != nil {
		return nil, err
	}

	m.NodesHealthy, err = meter.Int64UpDownCounter("gosched_nodes_healthy",
		metric.WithDescription("Current number of healthy nodes"))
	if err != nil {
		return nil, err
	}

	m.NodeCPUUsage, err = meter.Float64Gauge("gosched_node_cpu_usage_ratio",
		metric.WithDescription("CPU usage ratio per node"))
	if err != nil {
		return nil, err
	}

	m.NodeMemoryUsage, err = meter.Float64Gauge("gosched_node_memory_usage_ratio",
		metric.WithDescription("Memory usage ratio per node"))
	if err != nil {
		return nil, err
	}

	m.NodeGPUUsage, err = meter.Float64Gauge("gosched_node_gpu_usage_ratio",
		metric.WithDescription("GPU usage ratio per node"))
	if err != nil {
		return nil, err
	}

	m.ClusterCPUTotal, err = meter.Float64Gauge("gosched_cluster_cpu_total_cores",
		metric.WithDescription("Total CPU cores in the cluster"))
	if err != nil {
		return nil, err
	}

	m.ClusterCPUUsed, err = meter.Float64Gauge("gosched_cluster_cpu_used_cores",
		metric.WithDescription("Used CPU cores in the cluster"))
	if err != nil {
		return nil, err
	}

	m.ClusterMemoryTotal, err = meter.Float64Gauge("gosched_cluster_memory_total_bytes",
		metric.WithDescription("Total memory in the cluster"))
	if err != nil {
		return nil, err
	}

	m.ClusterMemoryUsed, err = meter.Float64Gauge("gosched_cluster_memory_used_bytes",
		metric.WithDescription("Used memory in the cluster"))
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RecordSchedulingCycle records metrics for a scheduling cycle.
func (m *Metrics) RecordSchedulingCycle(ctx context.Context, duration time.Duration, assignments int, err error) {
	m.SchedulingAttempts.Add(ctx, 1)
	m.SchedulingDuration.Record(ctx, duration.Seconds())
	if err != nil {
		m.SchedulingErrors.Add(ctx, 1)
	}
	if assignments > 0 {
		m.AssignmentsCreated.Add(ctx, int64(assignments))
	}
}

// RecordJobSubmit records a job submission.
func (m *Metrics) RecordJobSubmit(ctx context.Context, queue string) {
	m.JobsSubmitted.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", queue)))
	m.JobQueueDepth.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", queue)))
}

// RecordJobComplete records a job completion.
func (m *Metrics) RecordJobComplete(ctx context.Context, queue string, waitTime time.Duration) {
	m.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", queue)))
	m.JobQueueDepth.Add(ctx, -1, metric.WithAttributes(attribute.String("queue", queue)))
	m.JobWaitTime.Record(ctx, waitTime.Seconds(), metric.WithAttributes(attribute.String("queue", queue)))
}

// MetricsHandler returns an HTTP handler for Prometheus metrics scraping.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	)
}

// NewLogger creates a structured slog logger for GoSched.
func NewLogger(level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}

// Tracer returns the GoSched tracer for distributed tracing.
func Tracer() trace.Tracer {
	return otel.Tracer("gosched")
}
