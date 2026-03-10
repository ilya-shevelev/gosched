package agent

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/ilya-shevelev/gosched/pkg/resource"
)

const cgroupV2Base = "/sys/fs/cgroup"

// setupCgroup creates a cgroup for resource isolation of a job.
// Returns the cgroup path for later cleanup, or empty string if cgroups are
// not available (non-Linux or insufficient permissions).
func setupCgroup(jobID string, res resource.Resources) (string, error) {
	if runtime.GOOS != "linux" {
		return "", nil
	}

	cgroupPath := filepath.Join(cgroupV2Base, "gosched", jobID)

	if err := os.MkdirAll(cgroupPath, 0o755); err != nil {
		return "", fmt.Errorf("create cgroup directory: %w", err)
	}

	// Set CPU limit (cpu.max format: "quota period" in microseconds).
	if res.CPU > 0 {
		// Convert millicores to microseconds per 100ms period.
		periodUS := 100000 // 100ms in microseconds.
		quotaUS := int(res.CPU) * periodUS / 1000
		cpuMax := fmt.Sprintf("%d %d", quotaUS, periodUS)
		if err := writeCgroupFile(cgroupPath, "cpu.max", cpuMax); err != nil {
			return cgroupPath, fmt.Errorf("set cpu.max: %w", err)
		}
	}

	// Set memory limit.
	if res.Memory > 0 {
		memLimit := strconv.FormatInt(int64(res.Memory), 10)
		if err := writeCgroupFile(cgroupPath, "memory.max", memLimit); err != nil {
			return cgroupPath, fmt.Errorf("set memory.max: %w", err)
		}
	}

	return cgroupPath, nil
}

// cleanupCgroup removes the cgroup directory.
func cleanupCgroup(cgroupPath string, logger *slog.Logger) {
	if cgroupPath == "" {
		return
	}
	if err := os.Remove(cgroupPath); err != nil {
		logger.Warn("failed to remove cgroup", "path", cgroupPath, "error", err)
	}
}

// writeCgroupFile writes a value to a cgroup control file.
func writeCgroupFile(cgroupPath, filename, value string) error {
	path := filepath.Join(cgroupPath, filename)
	return os.WriteFile(path, []byte(value), 0o644)
}

// addProcessToCgroup moves a process into a cgroup.
func addProcessToCgroup(cgroupPath string, pid int) error {
	return writeCgroupFile(cgroupPath, "cgroup.procs", strconv.Itoa(pid))
}
