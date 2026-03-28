// Example gang-scheduling demonstrates GoSched's gang scheduling for
// multi-task parallel workloads like distributed training or MPI jobs.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ilya-shevelev/gosched/pkg/resource"
	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

func main() {
	ctx := context.Background()

	// Create a cluster with 4 GPU nodes.
	nodes := make([]*scheduler.NodeInfo, 4)
	for i := range nodes {
		nodeID := fmt.Sprintf("gpu-node-%d", i+1)
		nodes[i] = &scheduler.NodeInfo{
			ID:       nodeID,
			Hostname: nodeID + ".cluster.local",
			Labels:   map[string]string{"gpu": "true", "gpu.model": "A100"},
			Total: resource.Resources{
				CPU:    16000,
				Memory: 64 * resource.GiB,
				GPU:    resource.GPUResources{Count: 4, Model: "A100"},
				Custom: make(map[string]int64),
			},
			Used: resource.Zero(),
			Available: resource.Resources{
				CPU:    16000,
				Memory: 64 * resource.GiB,
				GPU:    resource.GPUResources{Count: 4, Model: "A100"},
				Custom: make(map[string]int64),
			},
			Healthy:       true,
			LastHeartbeat: time.Now(),
		}
	}

	// Distributed training job requiring 8 GPUs across multiple nodes.
	trainingJob := &scheduler.Job{
		ID:        "distributed-training-1",
		Name:      "LLM Fine-tuning (8 GPU)",
		Tenant:    "ml-team",
		Priority:  scheduler.PriorityHigh,
		State:     scheduler.JobStatePending,
		TaskCount: 4, // 4 tasks, each using 2 GPUs.
		TaskResources: resource.Resources{
			CPU:    4000,
			Memory: 16 * resource.GiB,
			GPU:    resource.GPUResources{Count: 2},
			Custom: make(map[string]int64),
		},
		Labels: map[string]string{
			"workload.type": "training",
			"framework":     "pytorch",
		},
		CreatedAt: time.Now(),
	}

	// A smaller job that should also be scheduled.
	inferenceJob := &scheduler.Job{
		ID:        "inference-service-1",
		Name:      "Model Inference",
		Tenant:    "ml-team",
		Priority:  scheduler.PriorityNormal,
		State:     scheduler.JobStatePending,
		TaskCount: 1,
		Resources: resource.Resources{
			CPU:    2000,
			Memory: 8 * resource.GiB,
			GPU:    resource.GPUResources{Count: 1},
			Custom: make(map[string]int64),
		},
		Labels: map[string]string{
			"workload.type": "inference",
		},
		CreatedAt: time.Now(),
	}

	// A job that is too large to fit (should fail gang constraint).
	tooLargeJob := &scheduler.Job{
		ID:        "massive-training",
		Name:      "Massive Training (32 GPU)",
		Tenant:    "ml-team",
		Priority:  scheduler.PriorityNormal,
		State:     scheduler.JobStatePending,
		TaskCount: 8, // 8 tasks * 4 GPUs = 32 GPUs (only 16 available).
		TaskResources: resource.Resources{
			CPU:    8000,
			Memory: 32 * resource.GiB,
			GPU:    resource.GPUResources{Count: 4},
			Custom: make(map[string]int64),
		},
		Labels:    map[string]string{"workload.type": "training"},
		CreatedAt: time.Now(),
	}

	pending := []*scheduler.Job{trainingJob, inferenceJob, tooLargeJob}

	// Gang scheduler with bin-pack fallback for single-task jobs.
	gang := scheduler.NewGangScheduler(scheduler.NewBinPackScheduler())

	fmt.Println("=== Gang Scheduling Demo ===")
	fmt.Printf("Cluster: %d nodes, each with 4 GPUs\n", len(nodes))
	fmt.Printf("Pending jobs: %d\n\n", len(pending))

	assignments, err := gang.Schedule(ctx, pending, nodes)
	if err != nil {
		log.Fatalf("Scheduling failed: %v", err)
	}

	// Group assignments by job.
	byJob := make(map[string][]*scheduler.Assignment)
	for _, a := range assignments {
		byJob[a.JobID] = append(byJob[a.JobID], a)
	}

	for _, job := range pending {
		tasks := byJob[job.ID]
		if len(tasks) == 0 {
			fmt.Printf("Job %q (%s): NOT SCHEDULED (gang constraint: need %d tasks)\n",
				job.Name, job.ID, job.TaskCount)
			continue
		}
		fmt.Printf("Job %q (%s): %d tasks scheduled\n", job.Name, job.ID, len(tasks))
		for _, a := range tasks {
			fmt.Printf("  Task %d -> Node %s\n", a.TaskIndex, a.NodeID)
		}
	}

	fmt.Printf("\nTotal assignments: %d\n", len(assignments))
	fmt.Println("Gang scheduling completed.")
}
