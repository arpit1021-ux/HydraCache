<div align="center">

# HydraCache

**A distributed in-memory cache built from first principles in Go**

[![CI](https://github.com/arpit1021-ux/HydraCache/actions/workflows/ci.yml/badge.svg)](https://github.com/arpit1021-ux/HydraCache/actions)
[![Release](https://img.shields.io/github/v/release/arpit1021-ux/HydraCache)](https://github.com/arpit1021-ux/HydraCache/releases/latest)
[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

---

HydraCache is a systems engineering project, not a tutorial: a cache that
survives node failure automatically, implementing the primitives a
production distributed cache actually needs — consistent hashing,
replication, failure detection, leader election — from scratch, with no
Redis source copied and no external consensus library.

It speaks RESP2 over TCP, so `redis-cli` and `go-redis` work against it
unmodified for the command set it implements — see
[COMMANDS.md](COMMANDS.md) for exactly what that is, and isn't.

## Features

- **Consistent hashing** — 150 virtual nodes per physical node, minimal key redistribution on membership change
- **Replication** — per-shard async/sync replication, lag tracking, epoch-fenced writes
- **Self-healing** — phi-accrual failure detection, automatic replica promotion
- **Leader election** — simplified Raft (pre-vote, durable terms, quorum, lease)
- **Persistence** — CRC-validated write-ahead log, torn-tail recovery, periodic snapshots
- **Security** — AUTH + per-key-pattern ACLs, TLS and mutual TLS
- **Observability** — Prometheus metrics (real latency histograms), a live React dashboard
- **RESP2 protocol** — proven against a real `go-redis` client, not just an internal test client

## Quick start

```bash
git clone https://github.com/arpit1021-ux/HydraCache.git
cd HydraCache
docker compose -f deploy/docker-compose.yml up -d
```

Brings up a 5-node cluster, Prometheus, and Grafana. Or grab a prebuilt
binary from the [latest release](https://github.com/arpit1021-ux/HydraCache/releases/latest),
or build from source:

```bash
go build -o hydracache ./cmd/server && go build -o hc ./cmd/cli
./hydracache -addr :7379 -http :8379 -data-dir ./data/wal
```

```bash
hc ping                              # PONG
hc set user:1 "Alice"                # OK
hc get user:1                        # Alice
hc set session:abc "data" EX 3600    # OK
hc ttl session:abc                   # (integer) 3598
```

A full read-through-cache reference app (Postgres-backed, with a live
"kill a node" control) lives in
[examples/readthrough](examples/readthrough/README.md).

## Architecture

```
Client ──► TCP/RESP Listener ──► Command Handler ──► In-Memory Cache
                                        │                   │
                                        ├──► WAL            └──► Metrics
                                        ├──► Hash Ring ──► Replication ──► Peer Nodes
                                        └──► Cluster Coordination ──► Peer Nodes
                                             (gossip · phi-accrual · election)
```

See the [interactive architecture diagram](docs/diagrams/hydracache-architecture.html)
and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full picture.

## Documentation

| | |
|---|---|
| [COMMANDS.md](COMMANDS.md) | Exactly what's implemented, partial, or unsupported |
| [OPERATIONS.md](OPERATIONS.md) | Deploying, monitoring, backup/restore, troubleshooting |
| [docs/](docs/) | Architecture, design decisions, replication, persistence, benchmarks |
| [deploy/k8s](deploy/k8s/README.md) | Kubernetes manifests (5-node StatefulSet) |
| [CHANGELOG.md](CHANGELOG.md) | What's changed |
| [INTERVIEW_PREP.md](INTERVIEW_PREP.md) | A from-scratch technical walkthrough of the whole system |

## Status

v1.0.0 is tagged and released — see [releases](https://github.com/arpit1021-ux/HydraCache/releases).
This project documents its own gaps as rigorously as its features: no
Redis Cluster protocol (by design — see [COMMANDS.md](COMMANDS.md)), no
pub/sub over the wire, no hot-reload for AUTH/TLS config. Full list in
COMMANDS.md and OPERATIONS.md.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
