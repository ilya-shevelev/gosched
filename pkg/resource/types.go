// Package resource defines the resource model for GoSched including CPU, memory,
// GPU, storage, and custom resource types used across the scheduling system.
package resource

import (
	"fmt"
	"math"
)

// MilliCPU represents CPU in millicores (1000m = 1 CPU core).
type MilliCPU int64

// Bytes represents memory or storage in bytes.
type Bytes int64

// Common CPU constants.
const (
	MilliCPUPerCore MilliCPU = 1000
)

// Common memory constants.
const (
	KiB Bytes = 1024
	MiB Bytes = 1024 * KiB
	GiB Bytes = 1024 * MiB
	TiB Bytes = 1024 * GiB
)

// GPUType identifies a GPU model family.
type GPUType string

// Known GPU types.
const (
	GPUTypeNvidia   GPUType = "nvidia"
	GPUTypeAMD      GPUType = "amd"
	GPUTypeIntel    GPUType = "intel"
	GPUTypeUnknown  GPUType = "unknown"
)

// GPUDevice represents a single GPU device on a node.
type GPUDevice struct {
	// ID is a unique identifier for this GPU on the node.
	ID string
	// Type is the GPU vendor/family.
	Type GPUType
	// Model is the specific GPU model (e.g., "A100", "H100").
	Model string
	// MemoryBytes is the total GPU memory in bytes.
	MemoryBytes Bytes
	// ComputeCapability for NVIDIA GPUs (e.g., "8.0").
	ComputeCapability string
	// NUMANode is the NUMA node this GPU is attached to (-1 if unknown).
	NUMANode int
	// PCIAddress is the PCI bus address.
	PCIAddress string
	// InUse indicates whether the GPU is currently allocated.
	InUse bool
}

// GPUResources describes GPU resource requirements or availability.
type GPUResources struct {
	// Count is the number of GPUs required or available.
	Count int
	// Type restricts to a specific GPU type (empty means any).
	Type GPUType
	// MinMemoryBytes is the minimum GPU memory required per device.
	MinMemoryBytes Bytes
	// Model restricts to a specific GPU model (empty means any).
	Model string
}

// NUMATopology describes the NUMA layout of a node.
type NUMATopology struct {
	// Nodes maps NUMA node ID to its resources.
	Nodes map[int]*NUMANode
}

// NUMANode represents a single NUMA node's resources.
type NUMANode struct {
	ID     int
	CPU    MilliCPU
	Memory Bytes
	GPUs   []string // GPU device IDs attached to this NUMA node.
}

// Resources represents a set of compute resources.
type Resources struct {
	CPU     MilliCPU
	Memory  Bytes
	GPU     GPUResources
	Storage Bytes
	Custom  map[string]int64
}

// Zero returns an empty Resources.
func Zero() Resources {
	return Resources{
		Custom: make(map[string]int64),
	}
}

// NewResources creates a Resources with CPU (millicores) and Memory (bytes).
func NewResources(cpu MilliCPU, memory Bytes) Resources {
	return Resources{
		CPU:    cpu,
		Memory: memory,
		Custom: make(map[string]int64),
	}
}

// Add returns the sum of two Resources.
func (r Resources) Add(other Resources) Resources {
	result := Resources{
		CPU:     r.CPU + other.CPU,
		Memory:  r.Memory + other.Memory,
		Storage: r.Storage + other.Storage,
		GPU: GPUResources{
			Count: r.GPU.Count + other.GPU.Count,
		},
		Custom: make(map[string]int64),
	}
	for k, v := range r.Custom {
		result.Custom[k] = v
	}
	for k, v := range other.Custom {
		result.Custom[k] += v
	}
	return result
}

// Sub returns the difference r - other. Negative values are clamped to zero.
func (r Resources) Sub(other Resources) Resources {
	result := Resources{
		CPU:     max(r.CPU-other.CPU, 0),
		Memory:  max(r.Memory-other.Memory, 0),
		Storage: max(r.Storage-other.Storage, 0),
		GPU: GPUResources{
			Count: max(r.GPU.Count-other.GPU.Count, 0),
		},
		Custom: make(map[string]int64),
	}
	for k, v := range r.Custom {
		ov := other.Custom[k]
		result.Custom[k] = max(v-ov, 0)
	}
	return result
}

// Fits returns true if all resource dimensions of other fit within r.
func (r Resources) Fits(other Resources) bool {
	if r.CPU < other.CPU {
		return false
	}
	if r.Memory < other.Memory {
		return false
	}
	if r.Storage < other.Storage {
		return false
	}
	if r.GPU.Count < other.GPU.Count {
		return false
	}
	for k, v := range other.Custom {
		if r.Custom[k] < v {
			return false
		}
	}
	return true
}

// IsZero returns true if all resource dimensions are zero.
func (r Resources) IsZero() bool {
	if r.CPU != 0 || r.Memory != 0 || r.Storage != 0 || r.GPU.Count != 0 {
		return false
	}
	for _, v := range r.Custom {
		if v != 0 {
			return false
		}
	}
	return true
}

// UsageRatio returns the fraction of r that is consumed by used,
// taking the maximum across CPU, memory, and GPU dimensions.
// Returns 0 if r is zero in all dimensions.
func (r Resources) UsageRatio(used Resources) float64 {
	var maxRatio float64
	if r.CPU > 0 {
		ratio := float64(used.CPU) / float64(r.CPU)
		maxRatio = math.Max(maxRatio, ratio)
	}
	if r.Memory > 0 {
		ratio := float64(used.Memory) / float64(r.Memory)
		maxRatio = math.Max(maxRatio, ratio)
	}
	if r.GPU.Count > 0 {
		ratio := float64(used.GPU.Count) / float64(r.GPU.Count)
		maxRatio = math.Max(maxRatio, ratio)
	}
	return maxRatio
}

// String returns a human-readable representation.
func (r Resources) String() string {
	s := fmt.Sprintf("CPU=%dm Memory=%s", r.CPU, formatBytes(r.Memory))
	if r.GPU.Count > 0 {
		s += fmt.Sprintf(" GPU=%d", r.GPU.Count)
	}
	if r.Storage > 0 {
		s += fmt.Sprintf(" Storage=%s", formatBytes(r.Storage))
	}
	return s
}

func formatBytes(b Bytes) string {
	switch {
	case b >= TiB:
		return fmt.Sprintf("%.1fTi", float64(b)/float64(TiB))
	case b >= GiB:
		return fmt.Sprintf("%.1fGi", float64(b)/float64(GiB))
	case b >= MiB:
		return fmt.Sprintf("%.1fMi", float64(b)/float64(MiB))
	case b >= KiB:
		return fmt.Sprintf("%.1fKi", float64(b)/float64(KiB))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
