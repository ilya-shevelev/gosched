# GoSched

[![CI](https://github.com/ilya-shevelev/gosched/actions/workflows/ci.yml/badge.svg)](https://github.com/ilya-shevelev/gosched/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ilya-shevelev/gosched)](https://goreportcard.com/report/github.com/ilya-shevelev/gosched)
[![License](https://img.shields.io/github/license/ilya-shevelev/gosched)](LICENSE)

**Distributed Resource Scheduler for Heterogeneous Compute Workloads**

GoSched is a cloud-native resource scheduler designed as an alternative to Hadoop YARN, with first-class support for GPU-aware scheduling, gang scheduling, preemption, and multi-tenant fair-share queuing.

## Architecture

```
                    +---------------------+
                    |    goschedctl CLI    |
                    +----------+----------+
                               |
                    +----------v----------+
                    | API Server           |
                    | (gRPC + HTTP/REST)   |
                    +----------+----------+
                               |
              +----------------+----------------+
              |                                 |
    +---------v---------+            +----------v----------+
    |   Queue Manager   |            |  Raft Consensus     |
    | Fair/Capacity/FIFO|            |  (Master HA)        |
    +---------+---------+            +---------------------+
              |
    +---------v---------+
    | Scheduling Engine  |
    | BinPack | Spread   |
    | Gang | Deadline    |
    | Priority+Preempt   |
    +---------+---------+
              |
    +---------v---------+
    | Placement Engine   |
    | Affinity/Taint     |
    | GPU Topology       |
    +---------+---------+
              |
    +---------v---------+
    |   Node Agents      |   Prometheus
    | cgroup isolation   +----> /metrics
    | resource reporting |
    +----+----+----+-----+
         |    |    |
      Spark Hadoop Container
```

## Features

- **Multiple Scheduling Algorithms**: BinPack (minimize fragmentation), Spread (maximize fault tolerance), Gang (all-or-nothing for parallel jobs), Priority (with preemption), Deadline-aware
- **GPU-Aware Scheduling**: NUMA topology awareness, GPU type/model matching, memory requirements
- **Multi-Tenant Fair Share**: Quota-based resource management with guaranteed minimums and borrowing
- **Queue Types**: FIFO, Fair-share (weighted), Capacity (with borrowing)
- **Affinity/Anti-Affinity**: Node selectors, label-based affinity rules, taint tolerations
- **Preemption**: Priority-based preemption with configurable policies
- **Plugin System**: Extensible scheduler plugins for Spark, Hadoop, and Kubernetes workloads
- **High Availability**: Raft consensus for master failover
- **Observability**: Prometheus metrics, OpenTelemetry tracing, structured JSON logging
- **Node Agent**: cgroup v2 resource isolation, heartbeat-based health monitoring
- **Workload Executors**: Shell commands, containers (Docker), Spark (spark-submit)
- **Integration**: YARN ResourceManager REST client, Kubernetes API helpers, Spark cluster manager client

## Quick Start

### Build from Source

```bash
git clone https://github.com/ilya-shevelev/gosched.git
cd gosched
make build
```

### Run the Master

```bash
./bin/gosched-master \
  --grpc-addr=:9090 \
  --http-addr=:8080 \
  --algorithm=binpack
```

### Run the Agent

```bash
./bin/gosched-agent \
  --master=localhost:9090 \
  --labels=gpu=true,zone=us-east-1a
```

### Check Cluster Status

```bash
./bin/goschedctl status
```

### Docker

```bash
# Build images
make docker

# Run master
docker run -p 9090:9090 -p 8080:8080 gosched-master:dev

# Run agent
docker run --privileged gosched-agent:dev --master=<master-ip>:9090
```

### Helm (Kubernetes)

```bash
helm install gosched deploy/helm/gosched \
  --set master.config.algorithm=spread \
  --set master.replicaCount=3
```

## Configuration

GoSched can be configured via CLI flags or a YAML configuration file:

```yaml
server:
  grpc_addr: ":9090"
  http_addr: ":8080"
  algorithm: "binpack"
  schedule_period: "1s"

raft:
  enabled: true
  node_id: "master-1"
  bind_addr: "0.0.0.0:7000"
  bootstrap: true
  peers:
    - "master-2:7000"
    - "master-3:7000"

queues:
  - name: production
    type: capacity
    capacity_pct: 60
    max_pct: 80
  - name: development
    type: capacity
    capacity_pct: 30
    max_pct: 50
  - name: testing
    type: capacity
    capacity_pct: 10
    max_pct: 100
```

## Scheduling Algorithms

| Algorithm | Strategy | Best For |
|-----------|----------|----------|
| **BinPack** | Fill nodes tightly, minimize fragmentation | Cost optimization, dense packing |
| **Spread** | Distribute evenly across nodes | Fault tolerance, load balancing |
| **Gang** | All-or-nothing for multi-task jobs | Distributed training, MPI |
| **Priority** | Higher priority first, with preemption | Mixed-priority workloads |
| **Deadline** | Nearest deadline first | SLA-sensitive batch jobs |

## Python SDK

```python
from gosched import GoSchedClient

client = GoSchedClient("http://localhost:8080")
status = client.get_status()
print(f"Nodes: {status.nodes}, Pending: {status.pending}")
```

## Project Structure

```
gosched/
├── cmd/                     # Binary entry points
│   ├── gosched-master/      # Master server
│   ├── gosched-agent/       # Node agent
│   └── goschedctl/          # CLI tool
├── api/proto/v1/            # Protobuf definitions
├── pkg/                     # Public libraries
│   ├── scheduler/           # Scheduling algorithms
│   ├── resource/            # Resource model and quotas
│   ├── affinity/            # Affinity/taint rules
│   ├── queue/               # Queue implementations
│   ├── plugin/              # Scheduler plugins
│   ├── consensus/           # Raft wrapper
│   ├── node/                # Node management
│   └── observability/       # Metrics and tracing
├── internal/                # Private packages
│   ├── agent/               # Node agent implementation
│   ├── server/              # gRPC + HTTP server
│   └── integration/         # External system clients
├── sdk/python/              # Python SDK
├── deploy/                  # Docker + Helm
├── examples/                # Usage examples
└── docs/                    # Documentation
```

## Development

```bash
# Format code
make fmt

# Run linter
make lint

# Run tests
make test

# Run go vet
make vet

# Build all binaries
make build
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
