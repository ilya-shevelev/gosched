package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// Executor runs a workload.
type Executor interface {
	Execute(ctx context.Context, job *scheduler.Job) (exitCode int, err error)
}

// ShellExecutor runs commands via the system shell.
type ShellExecutor struct{}

// Execute runs the job's command via /bin/sh.
func (e *ShellExecutor) Execute(ctx context.Context, job *scheduler.Job) (int, error) {
	command, ok := job.Labels["exec.command"]
	if !ok {
		return 1, fmt.Errorf("no exec.command label on job %q", job.ID)
	}

	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Env = buildEnv(job)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, fmt.Errorf("execute command: %w", err)
	}
	return 0, nil
}

// ContainerExecutor runs containerized workloads.
type ContainerExecutor struct {
	Runtime string // "docker" or "containerd".
}

// Execute runs the job as a container.
func (e *ContainerExecutor) Execute(ctx context.Context, job *scheduler.Job) (int, error) {
	image, ok := job.Labels["container.image"]
	if !ok {
		return 1, fmt.Errorf("no container.image label on job %q", job.ID)
	}

	runtime := e.Runtime
	if runtime == "" {
		runtime = "docker"
	}

	args := []string{"run", "--rm", "--name", "gosched-" + job.ID}

	// Resource limits.
	if job.Resources.CPU > 0 {
		cpus := fmt.Sprintf("%.2f", float64(job.Resources.CPU)/1000.0)
		args = append(args, "--cpus", cpus)
	}
	if job.Resources.Memory > 0 {
		args = append(args, "--memory", fmt.Sprintf("%d", job.Resources.Memory))
	}
	if job.Resources.GPU.Count > 0 {
		args = append(args, "--gpus", fmt.Sprintf("%d", job.Resources.GPU.Count))
	}

	// Environment variables from labels.
	for k, v := range job.Labels {
		if strings.HasPrefix(k, "env.") {
			envVar := strings.TrimPrefix(k, "env.")
			args = append(args, "-e", envVar+"="+v)
		}
	}

	// Command override.
	args = append(args, image)
	if cmd, ok := job.Labels["container.command"]; ok {
		args = append(args, strings.Fields(cmd)...)
	}

	cmd := exec.CommandContext(ctx, runtime, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, fmt.Errorf("execute container: %w", err)
	}
	return 0, nil
}

// SparkExecutor submits Spark jobs.
type SparkExecutor struct {
	SparkHome string
}

// Execute submits a Spark job via spark-submit.
func (e *SparkExecutor) Execute(ctx context.Context, job *scheduler.Job) (int, error) {
	mainClass, ok := job.Labels["spark.main.class"]
	if !ok {
		return 1, fmt.Errorf("no spark.main.class label on job %q", job.ID)
	}
	jarPath, ok := job.Labels["spark.jar"]
	if !ok {
		return 1, fmt.Errorf("no spark.jar label on job %q", job.ID)
	}

	sparkSubmit := "spark-submit"
	if e.SparkHome != "" {
		sparkSubmit = e.SparkHome + "/bin/spark-submit"
	}

	args := []string{
		"--class", mainClass,
		"--master", job.Labels["spark.master"],
	}

	if cores, ok := job.Labels["spark.executor.cores"]; ok {
		args = append(args, "--executor-cores", cores)
	}
	if mem, ok := job.Labels["spark.executor.memory"]; ok {
		args = append(args, "--executor-memory", mem)
	}
	if numExec, ok := job.Labels["spark.num.executors"]; ok {
		args = append(args, "--num-executors", numExec)
	}

	args = append(args, jarPath)

	// Append application arguments.
	if appArgs, ok := job.Labels["spark.app.args"]; ok {
		args = append(args, strings.Fields(appArgs)...)
	}

	cmd := exec.CommandContext(ctx, sparkSubmit, args...)
	cmd.Env = buildEnv(job)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, fmt.Errorf("execute spark-submit: %w", err)
	}
	return 0, nil
}

// selectExecutor chooses the right executor based on job labels.
func selectExecutor(job *scheduler.Job) Executor {
	if job.Labels == nil {
		return &ShellExecutor{}
	}
	switch job.Labels["workload.type"] {
	case "container":
		return &ContainerExecutor{Runtime: job.Labels["container.runtime"]}
	case "spark":
		return &SparkExecutor{SparkHome: job.Labels["spark.home"]}
	default:
		return &ShellExecutor{}
	}
}

func buildEnv(job *scheduler.Job) []string {
	env := os.Environ()
	env = append(env,
		"GOSCHED_JOB_ID="+job.ID,
		"GOSCHED_JOB_NAME="+job.Name,
		"GOSCHED_TENANT="+job.Tenant,
	)
	for k, v := range job.Labels {
		if strings.HasPrefix(k, "env.") {
			envVar := strings.TrimPrefix(k, "env.")
			env = append(env, envVar+"="+v)
		}
	}
	return env
}
