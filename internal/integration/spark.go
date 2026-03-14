package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SparkClient communicates with Spark's cluster manager REST API.
type SparkClient struct {
	baseURL    string
	httpClient *http.Client
}

// SparkApplication represents a Spark application.
type SparkApplication struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	State     string `json:"state"`
	StartTime int64  `json:"starttime"`
	Duration  int64  `json:"duration"`
	Attempts  []struct {
		AttemptID string `json:"attemptId"`
		StartTime string `json:"startTime"`
		EndTime   string `json:"endTime"`
		Duration  int64  `json:"duration"`
		Completed bool   `json:"completed"`
	} `json:"attempts"`
}

// SparkExecutor represents a Spark executor.
type SparkExecutor struct {
	ID          string `json:"id"`
	HostPort    string `json:"hostPort"`
	RDDBlocks   int    `json:"rddBlocks"`
	MemoryUsed  int64  `json:"memoryUsed"`
	MaxMemory   int64  `json:"maxMemory"`
	TotalCores  int    `json:"totalCores"`
	ActiveTasks int    `json:"activeTasks"`
	IsActive    bool   `json:"isActive"`
}

// SparkJob represents a Spark job within an application.
type SparkJob struct {
	JobID            int    `json:"jobId"`
	Name             string `json:"name"`
	Status           string `json:"status"`
	NumTasks         int    `json:"numTasks"`
	NumActiveTasks   int    `json:"numActiveTasks"`
	NumCompletedTasks int   `json:"numCompletedTasks"`
	NumFailedTasks   int    `json:"numFailedTasks"`
}

// NewSparkClient creates a Spark REST API client.
func NewSparkClient(baseURL string) *SparkClient {
	return &SparkClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ListApplications returns all Spark applications.
func (c *SparkClient) ListApplications(ctx context.Context) ([]SparkApplication, error) {
	var apps []SparkApplication
	if err := c.get(ctx, "/api/v1/applications", &apps); err != nil {
		return nil, err
	}
	return apps, nil
}

// GetApplication retrieves a specific Spark application by ID.
func (c *SparkClient) GetApplication(ctx context.Context, appID string) (*SparkApplication, error) {
	var app SparkApplication
	if err := c.get(ctx, "/api/v1/applications/"+appID, &app); err != nil {
		return nil, err
	}
	return &app, nil
}

// ListExecutors returns executors for a Spark application.
func (c *SparkClient) ListExecutors(ctx context.Context, appID string) ([]SparkExecutor, error) {
	var executors []SparkExecutor
	path := fmt.Sprintf("/api/v1/applications/%s/executors", appID)
	if err := c.get(ctx, path, &executors); err != nil {
		return nil, err
	}
	return executors, nil
}

// ListJobs returns jobs within a Spark application.
func (c *SparkClient) ListJobs(ctx context.Context, appID string) ([]SparkJob, error) {
	var jobs []SparkJob
	path := fmt.Sprintf("/api/v1/applications/%s/jobs", appID)
	if err := c.get(ctx, path, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (c *SparkClient) get(ctx context.Context, path string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
