<div align="center">

# HydraCache

**A self-healing distributed in-memory cache with automatic failover**

[![CI](https://github.com/hydracache/hydracache/actions/workflows/ci.yml/badge.svg)](https://github.com/hydracache/hydracache/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/hydracache/hydracache)](https://goreportcard.com/report/github.com/hydracache/hydracache)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev/)

</div>

---

HydraCache is a distributed in-memory cache built from scratch in Go. It survives node failures automatically without client downtime — no manual intervention, no data loss for in-flight requests.

Designed as a systems engineering project, not a tutorial.

---

## Why HydraCache

Most cache tutorials stop at a single-node key-value store. HydraCache goes further — it implements the distributed systems primitives that production caches actually need: consistent hashing, replication, failure detection, leader election, and self-healing.

Every component is built from first principles. No Redis source code copied. No external consensus libraries.

---

## Features

- **Consistent Hashing** — hash ring with virtual nodes, minimal key redistribution on topology changes
- **Replication** — async replication with lag tracking and automatic replica promotion
- **Self-Healing** — phi-accrual failure detection triggers automatic failover
- **Leader Election** — simplified Raft with term numbers and quorum
- **Persistence** — write-ahead log with CRC32 validation and periodic snapshots
- **Redis Protocol** — full RESP compatibility, works with `redis-cli` and existing clients
- **TTL & Eviction** — lazy + active expiration, LRU/LFU policies
- **Monitoring** — Prometheus metrics endpoint, real-time React dashboard
- **CLI** — production-quality command-line client over TCP
- **Docker** — one-command 5-node cluster with Prometheus + Grafana

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Go 1.22+ |
| Protocol | RESP (Redis Serialization Protocol) |
| Frontend | React, TypeScript, Tailwind CSS, Recharts |
| Metrics | Prometheus, Grafana |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |

---

## Architecture

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │◄───►│ Node 2  │◄───►│ Node 3  │
│ (Leader)│     │(Replica)│     │(Replica)│
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
     └───────────────┼───────────────┘
                     │
           ┌─────────▼─────────┐
           │ Consistent Hash   │
           │ Ring (150 VNodes) │
           └───────────────────┘
```

> **[Read full architecture documentation →](docs/ARCHITECTURE.md)**

---

## Quick Start

### Docker (recommended)

```bash
git clone https://github.com/hydracache/hydracache.git
cd hydracache
docker compose up -d
```

Launches 5 cache nodes, dashboard, Prometheus, and Grafana.

### From Source

```bash
go build -o hydracache ./cmd/server
go build -o hc ./cmd/cli

./hydracache -addr :7379 -http :8379
```

### CLI

```bash
hc ping                              # PONG
hc set user:1 "Alice"                # OK
hc get user:1                        # Alice
hc del user:1                        # (integer) 1
hc set session:abc "data" EX 3600    # OK
hc ttl session:abc                   # (integer) 3598
```

---

## Example

```
$ redis-cli -p 7379
127.0.0.1:7379> SET cache:hits 0
OK
127.0.0.1:7379> INCR cache:hits
(integer) 1
127.0.0.1:7379> GET cache:hits
"1"
127.0.0.1:7379> TTL cache:hits
(integer) -1
```

---

## Project Structure

```
hydracache/
├── cmd/
│   ├── server/          # Cache node entry point
│   └── cli/             # Command-line client
├── internal/
│   ├── cache/           # In-memory cache engine
│   ├── protocol/        # RESP protocol (parser, encoder, decoder)
│   ├── network/         # TCP server and client
│   ├── cluster/         # Topology management
│   ├── hashring/        # Consistent hashing
│   ├── replication/     # Async replication
│   ├── heartbeat/       # Failure detection
│   ├── election/        # Leader election
│   ├── persistence/     # WAL and snapshots
│   ├── pubsub/          # Publish/subscribe
│   ├── lock/            # Distributed locking
│   ├── metrics/         # Prometheus metrics
│   ├── simulator/       # Failure simulation
│   ├── chaostest/       # Chaos testing harness
│   ├── config/          # Configuration management
│   ├── logging/         # Structured logging
│   └── utils/           # Shared utilities
├── dashboard/           # React monitoring UI
├── deploy/              # Docker and Prometheus config
├── scripts/             # Helper scripts
└── docs/                # Architecture and design docs
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System design, component interaction, data flow |
| [Benchmarks](docs/BENCHMARKS.md) | Performance testing methodology and results |
| [Design Decisions](docs/DESIGN_DECISIONS.md) | Why each design choice was made |
| [Consistent Hashing](docs/CONSISTENT_HASHING.md) | Hash ring implementation and key redistribution |
| [Replication](docs/REPLICATION.md) | Async replication, lag tracking, promotion |
| [Self-Healing](docs/SELF_HEALING.md) | Failure detection and automatic recovery |
| [Persistence](docs/PERSISTENCE.md) | WAL, snapshots, and crash recovery |
| [API Reference](docs/API.md) | HTTP API endpoints and protocol details |

---

## Roadmap

- [x] Single-node cache with TTL and eviction
- [x] RESP protocol and TCP server
- [x] Consistent hashing with virtual nodes
- [x] Async replication with lag tracking
- [x] Phi-accrual failure detection
- [x] Simplified Raft leader election
- [x] Self-healing and automatic failover
- [x] WAL persistence and crash recovery
- [x] Prometheus metrics and Grafana dashboards
- [x] React monitoring dashboard
- [x] Docker Compose deployment
- [ ] Synchronous replication mode
- [ ] Redis Cluster protocol (MOVED/ASK)
- [ ] TLS support
- [ ] ACL and authentication
- [ ] gRPC API layer

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

---

## License

[MIT](LICENSE)
