// Command gosched-agent runs the GoSched node agent on each compute node.
// It reports resources, manages cgroup isolation, and executes workloads.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ilya-shevelev/gosched/internal/agent"
	"github.com/ilya-shevelev/gosched/pkg/observability"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	var (
		nodeID      = flag.String("node-id", "", "Node ID (defaults to hostname)")
		masterAddr  = flag.String("master", "localhost:9090", "Master server gRPC address")
		heartbeat   = flag.Duration("heartbeat", 10*time.Second, "Heartbeat interval")
		labels      = flag.String("labels", "", "Node labels as key=value,key=value")
		dataDir     = flag.String("data-dir", "/var/lib/gosched", "Data directory")
		logLevel    = flag.String("log-level", "info", "Log level: debug, info, warn, error")
		showVersion = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("gosched-agent %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	level := parseLogLevel(*logLevel)
	logger := observability.NewLogger(level)

	cfg := agent.Config{
		NodeID:          *nodeID,
		MasterAddr:      *masterAddr,
		HeartbeatPeriod: *heartbeat,
		Labels:          parseLabels(*labels),
		DataDir:         *dataDir,
	}

	ag, err := agent.NewAgent(cfg, logger)
	if err != nil {
		logger.Error("failed to create agent", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		ag.Stop()
		cancel()
	}()

	logger.Info("starting gosched-agent",
		"version", version,
		"node_id", cfg.NodeID,
		"master", cfg.MasterAddr,
	)

	if err := ag.Start(ctx); err != nil && ctx.Err() == nil {
		logger.Error("agent failed", "error", err)
		os.Exit(1)
	}
}

func parseLabels(s string) map[string]string {
	labels := make(map[string]string)
	if s == "" {
		return labels
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			labels[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return labels
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
