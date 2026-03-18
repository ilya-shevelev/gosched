package server

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// FileConfig represents the server configuration file format.
type FileConfig struct {
	Server struct {
		GRPCAddr       string `yaml:"grpc_addr"`
		HTTPAddr       string `yaml:"http_addr"`
		SchedulePeriod string `yaml:"schedule_period"`
		Algorithm      string `yaml:"algorithm"`
	} `yaml:"server"`

	Raft struct {
		Enabled   bool     `yaml:"enabled"`
		NodeID    string   `yaml:"node_id"`
		BindAddr  string   `yaml:"bind_addr"`
		DataDir   string   `yaml:"data_dir"`
		Bootstrap bool     `yaml:"bootstrap"`
		Peers     []string `yaml:"peers"`
	} `yaml:"raft"`

	Quotas map[string]struct {
		MaxCPU        int    `yaml:"max_cpu"`
		MaxMemory     string `yaml:"max_memory"`
		GuaranteedCPU int    `yaml:"guaranteed_cpu"`
		GuaranteedMem string `yaml:"guaranteed_memory"`
	} `yaml:"quotas"`

	Queues []struct {
		Name        string  `yaml:"name"`
		Type        string  `yaml:"type"`
		CapacityPct float64 `yaml:"capacity_pct"`
		MaxPct      float64 `yaml:"max_pct"`
		Weight      float64 `yaml:"weight"`
	} `yaml:"queues"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg FileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	return &cfg, nil
}

// ToServerConfig converts a FileConfig to a server Config.
func (fc *FileConfig) ToServerConfig() Config {
	cfg := Config{
		GRPCAddr:  fc.Server.GRPCAddr,
		HTTPAddr:  fc.Server.HTTPAddr,
		Algorithm: fc.Server.Algorithm,
	}

	if fc.Server.SchedulePeriod != "" {
		if d, err := time.ParseDuration(fc.Server.SchedulePeriod); err == nil {
			cfg.SchedulePeriod = d
		}
	}

	return cfg
}
