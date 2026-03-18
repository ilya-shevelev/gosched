// Command goschedctl is the CLI tool for interacting with a GoSched cluster.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	version    = "dev"
	commit     = "unknown"
	date       = "unknown"
	masterAddr string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "goschedctl",
		Short: "GoSched CLI - manage jobs, nodes, and cluster state",
		Long:  "goschedctl is the command-line interface for the GoSched distributed resource scheduler.",
	}

	rootCmd.PersistentFlags().StringVarP(&masterAddr, "master", "m", "http://localhost:8080", "Master server HTTP address")

	rootCmd.AddCommand(
		newVersionCmd(),
		newStatusCmd(),
		newJobCmd(),
		newNodeCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("goschedctl %s (commit: %s, built: %s)\n", version, commit, date)
		},
	}
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := httpGet("/api/v1/status")
			if err != nil {
				return fmt.Errorf("get status: %w", err)
			}
			defer resp.Body.Close()

			var status map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			data, _ := json.MarshalIndent(status, "", "  ")
			fmt.Println(string(data))
			return nil
		},
	}
}

func newJobCmd() *cobra.Command {
	jobCmd := &cobra.Command{
		Use:   "job",
		Short: "Manage jobs",
	}

	jobCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all jobs",
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := httpGet("/api/v1/status")
			if err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println(string(body))
			return nil
		},
	})

	return jobCmd
}

func newNodeCmd() *cobra.Command {
	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Manage nodes",
	}

	nodeCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all nodes",
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := httpGet("/api/v1/status")
			if err != nil {
				return fmt.Errorf("list nodes: %w", err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println(string(body))
			return nil
		},
	})

	return nodeCmd
}

func httpGet(path string) (*http.Response, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	return client.Get(masterAddr + path)
}
