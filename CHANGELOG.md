# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Entries are grouped by category rather than by commit, since a raw
commit-by-commit list would include a lot of in-the-moment lint fixes and
flake-chasing that aren't meaningful to anyone reading this file to
understand *what the system does*. The full, unfiltered history is always
`git log --oneline`.

## [Unreleased]

## [1.0.0] - 2026-09-27

### Added

- Single-node in-memory cache engine: TTL (lazy + active expiration), LRU
  and LFU eviction policies, real memory-bounds enforcement.
- RESP2 wire protocol (parser, encoder, TCP server) — compatible with
  `redis-cli` and `go-redis`, proven against a real `go-redis` client in
  `internal/network/goredis_compat_test.go`, not just an internal test
  client.
- Consistent hashing (150 virtual nodes/physical node, FNV-1a) for key
  routing and sharding.
- Asynchronous and synchronous replication, per-shard primary/replica
  topology, replication lag tracking, gap detection and catch-up.
- Phi-accrual failure detection (adaptive, not a fixed heartbeat timeout),
  with an injectable clock for deterministic tests.
- Gossip-based cluster membership (SWIM-style, with incarnation numbers for
  self-refutation of stale/false "dead" rumors).
- A simplified Raft-style leader election for cluster-coordinator status:
  durable term/vote persistence, pre-vote, quorum from total known
  membership, leader lease, graceful resignation on shutdown.
- Write-ahead log with CRC32-validated entries, torn-tail recovery, three
  configurable sync modes (`always` / `everysec` / `never`), and periodic
  snapshots.
- AUTH + per-user, per-key-pattern ACLs; TLS and mutual TLS for both the
  client-facing listener and outbound peer connections.
- Connection limits enforced before `Accept()` (not after), per-operation
  read/write timeouts, bounded graceful shutdown, structured JSON/text
  logging with a per-connection request ID.
- `HELLO`/`CLIENT` protocol negotiation, `CLUSTER INFO`/`CLUSTER MYID`,
  `SETNX` — closing real gaps a go-redis-driven compatibility test found.
- Prometheus metrics (`/metrics`) including a genuine cumulative latency
  histogram, and a JSON stats/cluster API (`/api/stats`, `/api/cluster`)
  polled by a React monitoring dashboard.
- A command-line client (`hc`) and a reference application
  (`examples/readthrough`) demonstrating HydraCache as a cache-aside layer
  in front of Postgres, including a live "kill a node" control.
- A Docker Compose 5-node cluster (plus Prometheus/Grafana), a chaos-testing
  harness (`internal/chaostest`) driving five real failure scenarios against
  it, Kubernetes manifests for a 5-node StatefulSet deployment, and a
  tagged-release CI workflow for multi-arch Docker images and binaries.
- `COMMANDS.md` (an exhaustive, generated-from-the-dispatch-code command
  compatibility reference) and `INTERVIEW_PREP.md` (a from-scratch technical
  walkthrough of the whole system).

### Changed

- `ReplicaStatus`'s underlying type from `int` to `int32`, matching the
  atomic-backed-enum pattern `cluster.Role`/`cluster.Health` already used,
  so its atomic storage is a same-width conversion instead of a narrowing
  one.
- Generic protocol errors now use Redis's own `ERR`-prefixed convention
  (and lowercase the command name in arity errors), matching real Redis
  behavior that client libraries pattern-match on.
- The metrics/dashboard HTTP server now runs with real read/write/idle
  timeouts and is included in the graceful-shutdown sequence, instead of a
  bare, untimed, un-shutdownable `http.ListenAndServe`.
- Data-at-rest file permissions (WAL, snapshot, election term store, log
  output) tightened from world-readable (0644) to owner-only (0600).
- The Docker image's runtime user now has a pinned, known UID/GID (1000)
  instead of whatever `adduser` assigns by default — required for the
  Kubernetes manifests' `fsGroup`/`runAsUser` to be a fact rather than a
  guess.

### Fixed

- A crash reachable by any TCP connection, even unauthenticated: a
  negative RESP bulk-string length reached an allocation with a negative
  size and panicked the whole process. The parser now rejects negative and
  over-maximum lengths before allocating anything.
- `MaxConns` wasn't actually enforced before `Accept()` — excess
  connections still consumed a file descriptor and a goroutine before being
  rejected. The semaphore slot is now reserved before accepting.
- LFU eviction's tie-break on equal frequency was non-deterministic (driven
  by Go's intentionally-randomized map iteration order) — fixed with a
  monotonic sequence counter that breaks ties toward the stalest entry.
- WAL entry encoding could silently truncate a field whose length exceeded
  what its `uint32` length prefix could represent, instead of erroring.
- `Server.Shutdown()` could hang indefinitely on a single stuck idle
  connection; it's now bounded with a force-close fallback.
- A stale local `golangci-lint` incremental cache produced a false-clean
  result while the identical commit correctly failed in CI on a real
  `govet shadow` finding — every lint invocation for the rest of the
  project now uses an explicitly fresh cache directory.
- `SETNX` (the classic two-argument form `go-redis`'s `SetNX` method
  actually sends) was entirely unimplemented — found by testing against a
  real `go-redis` client instead of only ever testing against
  hand-written test code.
- `CLUSTER` was registered as a recognized command (so arity validation
  passed) but had no dispatch case at all, so every `CLUSTER` subcommand
  fell through to a misleading "unknown command" — despite the CLI's own
  help text advertising subcommands (`nodes`, `slots`) that were never
  implemented. `CLUSTER INFO`/`MYID` are now real; `NODES`/`SLOTS` are
  explicitly refused with an explanation (implementing them in Redis
  Cluster's own wire format would misroute a real Cluster-aware client,
  since this project's hashing isn't CRC16-mod-16384).
- README/CONTRIBUTING badges and clone URLs pointed at a different GitHub
  repository than this project's actual location.

### Removed

- Six instances, found over the course of this project's own internal
  audit, of the same pattern — a fully built, unit-tested capability with
  zero production callers: a `logging` package constructed and immediately
  discarded in `main.go`; a `pubsub.Broker` likewise; `RecordLatency`
  (now wired in for real, see Added); `cluster.Node`'s `Load`/`MemoryMB`
  fields, which were JSON-marshaled everywhere but never populated by any
  code path, and which gossip's own wire format never even transmitted
  between nodes in the first place; a Bloom filter with no integration
  point anywhere in the cache's actual architecture.
- Dashboard elements that displayed fabricated data unconditionally, with
  no "demo data" label — a permanently-static "Latency Distribution" chart
  that never reflected real traffic, and per-node "CPU%"/"Memory" figures
  that were always exactly zero against any real, connected backend.
  Replaced with real data (the new latency histogram; a "last seen" time
  computed from real gossip activity) where a real source existed, and
  removed entirely where it didn't.
- A stale `unused`-linter exclusion for `internal/cluster/manager.go`
  ("may be needed for future features") — removing it surfaced zero new
  findings, meaning it had already stopped hiding anything.

### Security

- Constant-time password comparison (`AUTH`), preventing a timing
  side-channel that a naive string comparison would expose.
- `gosec` added to the lint suite; every real finding fixed (see Changed/
  Fixed above) rather than the linter being disabled or its output ignored.
- Inter-node RPCs (`GOSSIP`/`REPLICATE`/`ELECTION_*`) are exempt from
  client-facing ACL checks by design — a peer node never calls `AUTH`, and
  without the exemption, enabling client authentication would break
  clustering entirely. Documented explicitly so it isn't mistaken for an
  oversight.
