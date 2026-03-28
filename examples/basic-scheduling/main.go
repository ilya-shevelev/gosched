// Example basic-scheduling demonstrates how to use GoSched's scheduling
// algorithms to assign jobs to nodes.
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

	// Create a cluster of nodes with different capacities.
	nodes := []*scheduler.NodeInfo{
		{
			ID:        "gpu-node-1",
			Hostname:  "gpu-node-1.example.com",
			Labels:    map[string]string{"gpu": "true", "zone": "us-east-1a"},
			Total:     resource.Resources{CPU: 16000, Memory: 64 * resource.GiB, GPU: resource.GPUResources{Count: 4}, Custom: make(map[string]int64)},
			Used:      resource.Zero(),
			Available: resource.Resources{CPU: 16000, Memory: 64 * resource.GiB, GPU: resource.GPUResources{Count: 4}, Custom: make(map[string]int64)},
			Healthy:   true,
			LastHeartbeat: time.Now(),
		},
		{
			ID:        "cpu-node-1",
			Hostname:  "cpu-node-1.example.com",
			Labels:    map[string]string{"zone": "us-east-1b"},
			Total:     resource.Resources{CPU: 32000, Memory: 128 * resource.GiB, Custom: make(map[string]int64)},
			Used:      resource.Zero(),
			Available: resource.Resources{CPU: 32000, Memory: 128 * resource.GiB, Custom: make(map[string]int64)},
			Healthy:   true,
			LastHeartbeat: time.Now(),
		},
		{
			ID:        "cpu-node-2",
			Hostname:  "cpu-node-2.example.com",
			Labels:    map[string]string{"zone": "us-east-1a"},
			Total:     resource.Resources{CPU: 16000, Memory: 32 * resource.GiB, Custom: make(map[string]int64)},
			Used:      resource.Zero(),
			Available: resource.Resources{CPU: 16000, Memory: 32 * resource.GiB, Custom: make(map[string]int64)},
			Healthy:   true,
			LastHeartbeat: time.Now(),
		},
	}

	// Create jobs with varying resource requirements.
	jobs := []*scheduler.Job{
		{
			ID:        "training-job-1",
			Name:      "ML Model Training",
			Tenant:    "data-science",
			Priority:  scheduler.PriorityHigh,
			State:     scheduler.JobStatePending,
			Resources: resource.Resources{CPU: 4000, Memory: 16 * resource.GiB, GPU: resource.GPUResources{Count: 2}, Custom: make(map[string]int64)},
			Labels:    map[string]string{"workload.type": "training"},
			CreatedAt: time.Now(),
		},
		{
			ID:        "etl-job-1",
			Name:      "Daily ETL Pipeline",
			Tenant:    "data-eng",
			Priority:  scheduler.PriorityNormal,
			State:     scheduler.JobStatePending,
			Resources: resource.NewResources(8000, 32*resource.GiB),
			Labels:    map[string]string{"workload.type": "etl"},
			CreatedAt: time.Now(),
		},
		{
			ID:        "web-api-1",
			Name:      "Web API Service",
			Tenant:    "platform",
			Priority:  scheduler.PriorityNormal,
			State:     scheduler.JobStatePending,
			Resources: resource.NewResources(2000, 4*resource.GiB),
			Labels:    map[string]string{"workload.type": "service"},
			CreatedAt: time.Now(),
		},
	}

	// --- BinPack Scheduling ---
	fmt.Println("=== BinPack Scheduling ===")
	binpack := scheduler.NewBinPackScheduler()
	assignments, err := binpack.Schedule(ctx, jobs, nodes)
	if err != nil {
		log.Fatalf("BinPack scheduling failed: %v", err)
	}
	printAssignments(assignments)

	// --- Spread Scheduling ---
	fmt.Println("\n=== Spread Scheduling ===")
	spread := scheduler.NewSpreadScheduler()
	assignments, err = spread.Schedule(ctx, jobs, nodes)
	if err != nil {
		log.Fatalf("Spread scheduling failed: %v", err)
	}
	printAssignments(assignments)

	// --- Priority Scheduling ---
	fmt.Println("\n=== Priority Scheduling ===")
	priority := scheduler.NewPriorityScheduler(scheduler.NewBinPackScheduler())
	assignments, err = priority.Schedule(ctx, jobs, nodes)
	if err != nil {
		log.Fatalf("Priority scheduling failed: %v", err)
	}
	printAssignments(assignments)

	fmt.Println("\nAll scheduling strategies completed successfully.")
}

func printAssignments(assignments []*scheduler.Assignment) {
	if len(assignments) == 0 {
		fmt.Println("  No assignments made")
		return
	}
	for _, a := range assignments {
		fmt.Printf("  Job %-20s -> Node %-15s (task %d, resources: %s)\n",
			a.JobID, a.NodeID, a.TaskIndex, a.Resources.String())
	}
}
