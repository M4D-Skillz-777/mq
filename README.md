# DMQ - Distributed Message Queue

A Kafka-like distributed message queue built in Go for educational purposes. Implements core distributed systems concepts including Raft consensus, consumer groups, and replication.

![Go Version](https://img.shields.io/badge/go-1.21+-blue)
![Status](https://img.shields.io/badge/status-functional-green)
![License](https://img.shields.io/badge/license-MIT-yellow)

## Features

- **Write-Ahead Log (WAL)**: Durable, append-only storage with segment management
- **Raft Consensus**: Leader election and log replication
- **Consumer Groups**: Automatic partition rebalancing and offset management
- **Replication**: ISR (In-Sync Replicas) with configurable acks (0, 1, all)
- **gRPC API**: High-performance RPC with Protocol Buffers
- **Benchmarked**: 46k msg/s throughput, 1ms p99 latency

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Producer   │────▶│   Broker    │────▶│   Storage   │
│  (Go)       │     │  (Leader)   │     │    (WAL)    │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌─────────┐   ┌─────────┐   ┌─────────┐
        │Follower │   │Follower │   │Follower │
        │  (Raft) │   │  (Raft) │   │  (Raft) │
        └─────────┘   └─────────┘   └─────────┘
```

## Quick Start

### Prerequisites

- Go 1.21+
- Protocol Buffers compiler (for regenerating protos)

### Installation

```bash
git clone https://github.com/adwaiy/mq.git
cd mq
go mod tidy
```

### Run Single Broker

```bash
# Build and run
go run ./cmd/broker -id=broker-1 -port=9001 -data=./data

# Or build binary first
go build -o bin/broker ./cmd/broker
./bin/broker -id=broker-1 -port=9001 -data=./data
```

### Run 3-Node Cluster

Terminal 1:
```bash
go run ./cmd/broker -id=1 -port=9001 -data=./data/node1
```

Terminal 2:
```bash
go run ./cmd/broker -id=2 -port=9002 -data=./data/node2 -peers="1:localhost:9001"
```

Terminal 3:
```bash
go run ./cmd/broker -id=3 -port=9003 -data=./data/node3 -peers="1:localhost:9001,2:localhost:9002"
```

### Test with CLI

```bash
go run ./cmd/cli
```

### Run Benchmarks

```bash
go run ./cmd/benchmark -brokers=localhost:9001 -count=50000 -concurrency=20
```

## Usage Example

### Producer

```go
import "github.com/adwaiy/mq/client"

producer, _ := client.NewProducer(client.ProducerConfig{
    BrokerAddr: "localhost:9001",
    Acks:       1,  // 0=fire-forget, 1=leader, -1=all
})
defer producer.Close()

producer.Send("my-topic", []byte("key"), []byte("hello world"), nil)
```

### Consumer

```go
consumer, _ := client.NewConsumer(client.ConsumerConfig{
    GroupID:    "my-group",
    Topics:     []string{"my-topic"},
    BrokerAddr: "localhost:9001",
    AutoCommit: true,
})
defer consumer.Close()

consumer.Subscribe(func(msg *mqpb.Message) {
    fmt.Printf("Received: %s\n", string(msg.Value))
})

select {} // Run forever
```

## Project Structure

```
mq/
├── cmd/                    # Commands
│   ├── broker/            # Broker binary
│   ├── cli/               # CLI tool
│   ├── benchmark/         # Benchmark suite
│   ├── test_raft/         # Raft tests
│   ├── test_consumers/    # Consumer group tests
│   └── test_cluster/      # Multi-broker tests
├── internal/              # Private implementation
│   ├── api/               # gRPC handlers
│   ├── broker/            # Replication logic
│   ├── consumer/          # Consumer groups
│   ├── raft/              # Consensus algorithm
│   └── storage/           # WAL storage
├── client/                # Go client libraries
│   ├── producer.go
│   └── consumer.go
├── proto/                 # Protocol Buffers
└── configs/               # Configuration files
```

## Benchmark Results

**Single Node Performance**:

| Acks Level | Throughput | Latency (p99) |
|------------|------------|---------------|
| 0 (Fire-forget) | 46,098 msg/s | 1.12 ms |
| 1 (Leader) | 42,889 msg/s | 1.02 ms |
| -1 (All) | 42,050 msg/s | 1.18 ms |

Run benchmarks:
```bash
go run ./cmd/benchmark -brokers=localhost:9001 -count=50000 -concurrency=20 -size=512
```

## Key Components

### Storage Layer (`internal/storage/wal.go`)
- Write-ahead log with segment-based storage
- Sparse offset indexing
- Time-based indexing for seeks

### Raft Consensus (`internal/raft/node.go`)
- Leader election with randomized timeouts
- Log replication via AppendEntries RPC
- Term-based voting for safety

### Consumer Groups (`internal/consumer/`)
- Group coordination with rebalancing
- Range and RoundRobin assignment strategies
- Offset commits to internal topic

### Replication (`internal/broker/replicator.go`)
- ISR (In-Sync Replica) tracking
- Pull-based replication (followers fetch from leader)
- High watermark tracking

## Reading Guide

New to the codebase? Read in this order:

1. `pkg/codec/codec.go` - Message format
2. `internal/storage/wal.go` - Storage foundation
3. `internal/api/server.go` - API handlers
4. `internal/consumer/coordinator.go` - Consumer groups
5. `internal/broker/replicator.go` - Replication
6. `internal/raft/node.go` - Raft consensus

See `READING_GUIDE.md` for detailed path.

## Configuration

Broker flags:
- `-id`: Broker ID (default: broker-1)
- `-port`: Listen port (default: 9001)
- `-data`: Data directory (default: ./data)
- `-peers`: Comma-separated peers (id:host:port)

Example:
```bash
./bin/broker -id=1 -port=9001 -data=./data -peers="2:localhost:9002,3:localhost:9003"
```

## Testing

```bash
# Test Raft consensus
go run ./cmd/test_raft

# Test consumer groups
go run ./cmd/test_consumers

# Test multi-broker cluster
go run ./cmd/test_cluster

# Run all benchmarks
go run ./cmd/benchmark -brokers=localhost:9001
```

## Future Scope

This project was built for educational purposes. Potential enhancements:

- **Memory-mapped indexes**: O(log n) offset lookups instead of O(n) scan
- **Log compaction**: Key-based retention for compacted topics
- **Message compression**: gzip/snappy for bandwidth reduction
- **Raft snapshots**: Log truncation for long-running clusters
- **Dynamic membership**: Add/remove brokers without restart
- **Kafka protocol compatibility**: Wire protocol adapter for Kafka clients

## Learn More

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann

## License

MIT

## Acknowledgments

Built for educational purposes to understand distributed systems concepts.
