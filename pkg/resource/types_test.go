package resource

import (
	"testing"
)

func TestResources_Fits(t *testing.T) {
	tests := []struct {
		name     string
		capacity Resources
		request  Resources
		want     bool
	}{
		{
			name:     "empty fits empty",
			capacity: Zero(),
			request:  Zero(),
			want:     true,
		},
		{
			name:     "capacity fits request",
			capacity: NewResources(4000, 8*GiB),
			request:  NewResources(2000, 4*GiB),
			want:     true,
		},
		{
			name:     "exact fit",
			capacity: NewResources(4000, 8*GiB),
			request:  NewResources(4000, 8*GiB),
			want:     true,
		},
		{
			name:     "cpu exceeds",
			capacity: NewResources(2000, 8*GiB),
			request:  NewResources(4000, 4*GiB),
			want:     false,
		},
		{
			name:     "memory exceeds",
			capacity: NewResources(4000, 2*GiB),
			request:  NewResources(2000, 4*GiB),
			want:     false,
		},
		{
			name: "gpu exceeds",
			capacity: Resources{
				CPU: 4000, Memory: 8 * GiB,
				GPU:    GPUResources{Count: 1},
				Custom: make(map[string]int64),
			},
			request: Resources{
				CPU: 2000, Memory: 4 * GiB,
				GPU:    GPUResources{Count: 2},
				Custom: make(map[string]int64),
			},
			want: false,
		},
		{
			name: "custom resource exceeds",
			capacity: Resources{
				CPU: 4000, Memory: 8 * GiB,
				Custom: map[string]int64{"fpga": 2},
			},
			request: Resources{
				CPU: 2000, Memory: 4 * GiB,
				Custom: map[string]int64{"fpga": 4},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.capacity.Fits(tt.request)
			if got != tt.want {
				t.Errorf("Fits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResources_Add(t *testing.T) {
	a := NewResources(2000, 4*GiB)
	b := NewResources(1000, 2*GiB)
	sum := a.Add(b)

	if sum.CPU != 3000 {
		t.Errorf("CPU: got %d, want 3000", sum.CPU)
	}
	if sum.Memory != 6*GiB {
		t.Errorf("Memory: got %d, want %d", sum.Memory, 6*GiB)
	}
}

func TestResources_Sub(t *testing.T) {
	a := NewResources(4000, 8*GiB)
	b := NewResources(1000, 2*GiB)
	diff := a.Sub(b)

	if diff.CPU != 3000 {
		t.Errorf("CPU: got %d, want 3000", diff.CPU)
	}
	if diff.Memory != 6*GiB {
		t.Errorf("Memory: got %d, want %d", diff.Memory, 6*GiB)
	}

	// Test clamping to zero.
	under := b.Sub(a)
	if under.CPU != 0 {
		t.Errorf("CPU clamped: got %d, want 0", under.CPU)
	}
	if under.Memory != 0 {
		t.Errorf("Memory clamped: got %d, want 0", under.Memory)
	}
}

func TestResources_IsZero(t *testing.T) {
	if !Zero().IsZero() {
		t.Error("Zero() should be zero")
	}

	nonZero := NewResources(1, 0)
	if nonZero.IsZero() {
		t.Error("Non-zero resources should not be zero")
	}
}

func TestResources_UsageRatio(t *testing.T) {
	total := NewResources(4000, 8*GiB)

	tests := []struct {
		name string
		used Resources
		want float64
	}{
		{
			name: "no usage",
			used: Zero(),
			want: 0,
		},
		{
			name: "half cpu",
			used: NewResources(2000, 0),
			want: 0.5,
		},
		{
			name: "full memory",
			used: NewResources(0, 8*GiB),
			want: 1.0,
		},
		{
			name: "mixed (max wins)",
			used: NewResources(2000, 6*GiB),
			want: 0.75, // Memory is 6/8 = 0.75, CPU is 2/4 = 0.5.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := total.UsageRatio(tt.used)
			if got != tt.want {
				t.Errorf("UsageRatio() = %f, want %f", got, tt.want)
			}
		})
	}
}

func TestResources_String(t *testing.T) {
	r := Resources{
		CPU:    2000,
		Memory: 4 * GiB,
		GPU:    GPUResources{Count: 2},
		Custom: make(map[string]int64),
	}
	s := r.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}
