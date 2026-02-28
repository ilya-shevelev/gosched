// Package consensus provides a Raft consensus wrapper for master HA using
// the hashicorp/raft library.
package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// CommandType identifies the type of replicated command.
type CommandType uint8

// Command types.
const (
	CommandJobSubmit   CommandType = 1
	CommandJobUpdate   CommandType = 2
	CommandJobDelete   CommandType = 3
	CommandNodeRegister CommandType = 4
	CommandNodeUpdate  CommandType = 5
	CommandNodeRemove  CommandType = 6
)

// Command is a replicated state machine command.
type Command struct {
	Type    CommandType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RaftNode wraps hashicorp/raft for GoSched master HA.
type RaftNode struct {
	raft   *raft.Raft
	fsm    *FSM
	logger *slog.Logger
}

// Config holds Raft configuration.
type Config struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool
	Peers     []string
}

// NewRaftNode creates and starts a Raft node.
func NewRaftNode(cfg Config, logger *slog.Logger) (*RaftNode, error) {
	if logger == nil {
		logger = slog.Default()
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.SnapshotInterval = 30 * time.Second

	// Set up stores.
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "gosched-raft", cfg.NodeID)
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	// Set up transport.
	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = "127.0.0.1:0"
	}
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind address: %w", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create TCP transport: %w", err)
	}

	fsm := NewFSM(logger)

	r, err := raft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: transport.LocalAddr(),
			},
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer),
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			logger.Warn("bootstrap failed", "error", err)
		}
	}

	return &RaftNode{
		raft:   r,
		fsm:    fsm,
		logger: logger,
	}, nil
}

// Apply submits a command to the Raft log for replication.
func (rn *RaftNode) Apply(cmd Command, timeout time.Duration) error {
	if rn.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	f := rn.raft.Apply(data, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	return nil
}

// IsLeader returns true if this node is the current leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (rn *RaftNode) LeaderAddr() string {
	_, id := rn.raft.LeaderWithID()
	return string(id)
}

// AddVoter adds a new voting member to the cluster.
func (rn *RaftNode) AddVoter(id, addr string) error {
	f := rn.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// RemoveServer removes a member from the cluster.
func (rn *RaftNode) RemoveServer(id string) error {
	f := rn.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return f.Error()
}

// Shutdown gracefully stops the Raft node.
func (rn *RaftNode) Shutdown() error {
	f := rn.raft.Shutdown()
	return f.Error()
}

// FSM is the finite state machine for the GoSched Raft cluster.
type FSM struct {
	mu     sync.RWMutex
	state  map[string]json.RawMessage
	logger *slog.Logger
}

// NewFSM creates a new FSM.
func NewFSM(logger *slog.Logger) *FSM {
	return &FSM{
		state:  make(map[string]json.RawMessage),
		logger: logger,
	}
}

// Apply implements raft.FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("failed to unmarshal command", "error", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	key := fmt.Sprintf("%d", cmd.Type)
	f.state[key] = cmd.Payload
	return nil
}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy state.
	stateCopy := make(map[string]json.RawMessage, len(f.state))
	for k, v := range f.state {
		cp := make(json.RawMessage, len(v))
		copy(cp, v)
		stateCopy[k] = cp
	}
	return &fsmSnapshot{state: stateCopy}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state map[string]json.RawMessage
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	return nil
}

type fsmSnapshot struct {
	state map[string]json.RawMessage
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
