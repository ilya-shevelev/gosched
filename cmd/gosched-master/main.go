// Command gosched-master starts the GoSched master server which handles job
// scheduling, node management, and cluster coordination.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ilya-shevelev/gosched/internal/server"
	"github.com/ilya-shevelev/gosched/pkg/observability"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	var (
		grpcAddr       = flag.String("grpc-addr", ":9090", "gRPC listen address")
		httpAddr       = flag.String("http-addr", ":8080", "HTTP listen address (metrics, health)")
		algorithm      = flag.String("algorithm", "binpack", "Scheduling algorithm: binpack, spread, priority, gang")
		configFile     = flag.String("config", "", "Configuration file path")
		logLevel       = flag.String("log-level", "info", "Log level: debug, info, warn, error")
		showVersion    = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("gosched-master %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	level := parseLogLevel(*logLevel)
	logger := observability.NewLogger(level)

	var cfg server.Config
	if *configFile != "" {
		fileCfg, err := server.LoadConfig(*configFile)
		if err != nil {
			logger.Error("failed to load config", "error", err)
			os.Exit(1)
		}
		cfg = fileCfg.ToServerConfig()
	} else {
		cfg = server.Config{
			GRPCAddr:  *grpcAddr,
			HTTPAddr:  *httpAddr,
			Algorithm: *algorithm,
		}
	}

	srv, err := server.NewServer(cfg, logger)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	logger.Info("starting gosched-master",
		"version", version,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
	)

	if err := srv.Start(ctx); err != nil && ctx.Err() == nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
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
