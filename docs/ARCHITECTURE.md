# HydraCache — Architecture Design Document

**Version**: 1.0  
**Status**: Draft  
**Authors**: HydraCache Engineering  
**Date**: 2026-07-02  
**Language**: Go 1.22+  
**Inspired by**: Redis Cluster, DynamoDB, Cassandra, CockroachDB

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Component Diagram](#3-component-diagram)
4. [Sequence Diagrams](#4-sequence-diagrams)
5. [Data Flow](#5-data-flow)
6. [Consistent Hashing](#6-consistent-hashing)
7. [Replication Strategy](#7-replication-strategy)
8. [Heartbeat & Failure Detection](#8-heartbeat--failure-detection)
9. [Leader Election](#9-leader-election)
10. [Self-Healing Flow](#10-self-healing-flow)
11. [Tradeoff Analysis](#11-tradeoff-analysis)
12. [Failure Modes](#12-failure-modes)
13. [Consistency Model](#13-consistency-model)
14. [Memory Model](#14-memory-model)
15. [Wire Protocol](#15-wire-protocol)

---

## 1. System Overview

### 1.1 Purpose

HydraCache is a self-healing distributed in-memory cache system designed for high-throughput, low-latency workloads. It provides automatic data partitioning, replication, failure detection, and recovery without requiring external coordination services such as ZooKeeper or etcd.

### 1.2 Design Goals

| Goal | Target | Rationale |
|------|--------|-----------|
| **Latency** | < 1ms p99 for GET/SET | In-memory operation path; no disk I/O |
| **Throughput** | > 500K ops/sec per node | Single-threaded event loop per shard |
| **Availability** | 99.99% uptime | Automatic failover within 5 seconds |
| **Partition Tolerance** | Survives N-1 node failures (N > 2) | Quorum-based replication |
| **Self-Healing** | Zero manual intervention | Gossip + leader-driven recovery |
| **Operational Simplicity** | Single binary, no external deps | Embedded Raft-inspired consensus |

### 1.3 Non-Goals

- Persistent durability (this is a cache, not a database)
- Complex query capabilities (no secondary indexes, no ranges)
- Cross-datacenter replication (single DC only in v1)
- Multi-tenancy (single tenant per cluster)

### 1.4 Key Properties

HydraCache operates as a **shared-nothing** architecture where each node owns a partition of the keyspace. Data is distributed across nodes using **consistent hashing with virtual nodes**, ensuring minimal data movement when the cluster topology changes. Each partition is replicated to `R` additional nodes, providing fault tolerance while maintaining write availability through quorum semantics.

The system follows an **AP-oriented** consistency model (per the CAP theorem) — it favors availability and partition tolerance over strong consistency. Reads from replicas may return stale data, but the system guarantees monotonic reads within a session via read-repair.

---

## 2. Architecture Diagram

```
                          ┌─────────────────────────────────────────────────────────┐
                          │                    CLIENT LAYER                         │
                          │                                                         │
                          │    ┌──────────┐  ┌──────────┐  ┌──────────┐            │
                          │    │ Client A │  │ Client B │  │ Client C │            │
                          │    └────┬─────┘  └────┬─────┘  └────┬─────┘            │
                          │         │             │             │                   │
                          │         ▼             ▼             ▼                   │
                          │    ┌─────────────────────────────────────┐              │
                          │    │         Load Balancer (L7)          │              │
                          │    │    (Round-robin / Least-conn)       │              │
                          │    └──────────────────┬──────────────────┘              │
                          └───────────────────────┼─────────────────────────────────┘
                                                  │
                          ┌───────────────────────┼─────────────────────────────────┐
                          │                  PROXY LAYER                            │
                          │                       │                                 │
                          │    ┌──────────────────▼──────────────────┐              │
                          │    │         HydraCache Proxy            │              │
                          │    │                                     │              │
                          │    │  ┌─────────┐  ┌──────────────┐     │              │
                          │    │  │ Command │  │ Hash Ring    │     │              │
                          │    │  │ Router  │──│ Lookup       │     │              │
                          │    │  └────┬────┘  └──────────────┘     │              │
                          │    │       │                            │              │
                          │    │  ┌────▼────────────────────────┐   │              │
                          │    │  │  Connection Pool Manager    │   │              │
                          │    │  │  (per-node pool, health     │   │              │
                          │    │  │   checks, circuit breaker)  │   │              │
                          │    │  └────┬────────────────────────┘   │              │
                          │    └───────┼────────────────────────────┘              │
                          └────────────┼───────────────────────────────────────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────────────────────┐
            │                     CLUSTER LAYER                                   │
            │                          │                                          │
            │   ┌──────────────────────┼────────────────────────────────┐         │
            │   │                      │                                │         │
            │   │    Node A (Primary: slots 0-5460)                     │         │
            │   │    ┌──────────────────────────────────────┐           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Store    │  │ Replication Mgr  │  │           │         │
            │   │    │  │ (shard)  │  │ (async repl)     │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Gossip   │  │ Failure Detector │  │           │         │
            │   │    │  │ Protocol │  │ (phi-accrual)    │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Raft     │  │ Slot Migration   │  │           │         │
            │   │    │  │ Consensus│  │ Engine           │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    └──────────────────────────────────────┘           │         │
            │   │                      │                                │         │
            │   │    ─────────── TCP Replication Links ───────────      │         │
            │   │                      │                                │         │
            │   │    Node B (Primary: slots 5461-10922)                 │         │
            │   │    ┌──────────────────────────────────────┐           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Store    │  │ Replication Mgr  │  │           │         │
            │   │    │  │ (shard)  │  │ (async repl)     │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Gossip   │  │ Failure Detector │  │           │         │
            │   │    │  │ Protocol │  │ (phi-accrual)    │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Raft     │  │ Slot Migration   │  │           │         │
            │   │    │  │ Consensus│  │ Engine           │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    └──────────────────────────────────────┘           │         │
            │   │                      │                                │         │
            │   │    ─────────── TCP Replication Links ───────────      │         │
            │   │                      │                                │         │
            │   │    Node C (Primary: slots 10923-16383)                │         │
            │   │    ┌──────────────────────────────────────┐           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Store    │  │ Replication Mgr  │  │           │         │
            │   │    │  │ (shard)  │  │ (async repl)     │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Gossip   │  │ Failure Detector │  │           │         │
            │   │    │  │ Protocol │  │ (phi-accrual)    │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    │  ┌──────────┐  ┌──────────────────┐  │           │         │
            │   │    │  │ Raft     │  │ Slot Migration   │  │           │         │
            │   │    │  │ Consensus│  │ Engine           │  │           │         │
            │   │    │  └──────────┘  └──────────────────┘  │           │         │
            │   │    └──────────────────────────────────────┘           │         │
            │   └───────────────────────────────────────────────────────┘         │
            └─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Diagram

### 3.1 Component Inventory

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           HYDRACACHE NODE ARCHITECTURE                         │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                         TCP Server (listener)                           │   │
│  │  Accepts inbound connections. Parses RESP protocol. Dispatches to       │   │
│  │  command handler. Maintains per-client state (SELECT db, AUTH, etc.)    │   │
│  └───────────────────────────────────┬──────────────────────────────────────┘   │
│                                      │                                         │
│  ┌───────────────────────────────────▼──────────────────────────────────────┐   │
│  │                       Command Router                                    │   │
│  │  Determines which slot a command maps to (CRC16 mod 16384).             │   │
│  │  Routes to local store if slot is owned locally, or redirects to        │   │
│  │  the owning node via MOVED/ASK response. Handles cluster bus commands.  │   │
│  └──────────┬────────────────────────────────┬──────────────────────────────┘   │
│             │                                │                                 │
│  ┌──────────▼──────────┐          ┌──────────▼──────────────────────────┐       │
│  │   Local Store       │          │   Cluster Bus (gossip)              │       │
│  │   (in-memory hash   │          │   UDP-based gossip protocol for     │       │
│  │    map + LRU)       │          │   node discovery, failure detection,│       │
│  │                     │          │   slot ownership announcements,     │       │
│  │   - HashTable       │          │   and metadata exchange.            │       │
│  │   - ExpireDict      │          │                                     │       │
│  │   - LRU Eviction    │          │   - Message serialization           │       │
│  │   - TTL management  │          │   - Anti-entropy (Merkle tree)      │       │
│  │   - Memory tracking │          │   - Vector clock propagation        │       │
│  └──────────┬──────────┘          └──────────┬──────────────────────────┘       │
│             │                                │                                 │
│  ┌──────────▼──────────┐          ┌──────────▼──────────────────────────┐       │
│  │   Replication Mgr   │          │   Failure Detector                  │       │
│  │                     │          │                                     │       │
│  │   - Async replication│         │   - Phi-accrual failure detector    │       │
│  │   - Partial resync  │          │   - Exponential moving average      │       │
│  │   - Backlog buffer  │          │   - Adaptive threshold              │       │
│  │   - Lag tracking    │          │   - Heartbeat processing            │       │
│  │   - Promotion on    │          │   - Suspect/confirm lifecycle       │       │
│  │     primary failure │          │                                     │       │
│  └──────────┬──────────┘          └──────────┬──────────────────────────┘       │
│             │                                │                                 │
│  ┌──────────▼──────────┐          ┌──────────▼──────────────────────────┐       │
│  │   Slot Migration    │          │   Leader Election (Raft)            │       │
│  │   Engine            │          │                                     │       │
│  │                     │          │   - Term management                 │       │
│  │   - MIGRATING state │          │   - Log replication                 │       │
│  │   - IMPORTING state │          │   - Heartbeat leadership            │       │
│  │   - STABLE state    │          │   - Follower → Candidate → Leader   │       │
│  │   -原子 slot transfer│         │   - Config change proposals         │       │
│  │   - Blocking client │          │                                     │       │
│  │     during migration│          │                                     │       │
│  └─────────────────────┘          └─────────────────────────────────────┘       │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                      Configuration Manager                              │   │
│  │  Reads YAML/JSON config. Manages dynamic reconfiguration.              │   │
│  │  Exposes /admin endpoints for cluster operations (add/remove node,     │   │
│  │  reshard, manual failover, cluster info).                               │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Interface Definitions

```go
// Store is the core key-value interface
type Store interface {
    Get(key string) (Value, error)
    Set(key string, value Value, opts SetOptions) error
    Delete(key string) (bool, error)
    Expire(key string, ttl time.Duration) error
    TTL(key string) (time.Duration, error)
    Keys(pattern string) ([]string, error)
    Scan(cursor string, count int, pattern string) (string, []string, error)
    MemoryUsage() int64
    Len() int64
}

// Replicator handles async replication to replicas
type Replicator interface {
    StartReplication(primary NodeID, replica NodeID) error
    StopReplication(primary NodeID, replica NodeID) error
    FullResync(primary NodeID, replica NodeID) error
    PartialResync(primary NodeID, replica NodeID, offset uint64) error
    GetReplicationLag(nodeID NodeID) (int64, error)
    GetBacklogSize() int
    WaitForReplication(keys []string, timeout time.Duration) error
}

// FailureDetector monitors node health via phi-accrual
type FailureDetector interface {
    Heartbeat(nodeID NodeID)
    IsAlive(nodeID NodeID) bool
    GetPhi(nodeID NodeID) float64
    SetThreshold(phi float64)
    GetSuspectedNodes() []NodeID
}

// ClusterBus handles inter-node communication
type ClusterBus interface {
    Send(nodeID NodeID, msg ClusterMessage) error
    Broadcast(msg ClusterMessage)
    Join(nodeID NodeID, addr string) error
    Leave(nodeID NodeID) error
    GetNodes() []NodeInfo
    GetSlotOwnership(slot int) NodeID
}

// LeaderElector implements simplified Raft consensus
type LeaderElector interface {
    Start() error
    Stop() error
    IsLeader() bool
    Propose(command []byte) (ApplyResult, error)
    GetLeader() NodeID
    GetTerm() uint64
}

// SlotMigrator handles slot transfer between nodes
type SlotMigrator interface {
    StartMigration(slots []int, from NodeID, to NodeID) error
    AbortMigration(slots []int) error
    GetMigrationStatus(slots []int) MigrationStatus
    ImportSlot(slot int, data []byte) error
}
```

---

## 4. Sequence Diagrams

### 4.1 SET Operation Flow

```
┌────────┐     ┌────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐
│ Client │     │ Proxy/     │     │ Hash Ring    │     │ Primary Node │     │ Replica  │
│        │     │ Router     │     │ Lookup       │     │              │     │          │
└───┬────┘     └─────┬──────┘     └──────┬───────┘     └──────┬───────┘     └────┬─────┘
    │                │                   │                    │                  │
    │  SET k v       │                   │                    │                  │
    │───────────────►│                   │                    │                  │
    │                │                   │                    │                  │
    │                │  Compute slot:    │                    │                  │
    │                │  CRC16(k) % 16384 │                    │                  │
    │                │──────────────────►│                    │                  │
    │                │                   │                    │                  │
    │                │  slot=4721        │                    │                  │
    │                │  owner=NodeA      │                    │                  │
    │                │◄──────────────────│                    │                  │
    │                │                   │                    │                  │
    │                │  Forward to NodeA │                    │                  │
    │                │──────────────────────────────────────►│                  │
    │                │                   │                    │                  │
    │                │                   │     SET k v        │                  │
    │                │                   │                    │                  │
    │                │                   │  ┌─────────────────┤                  │
    │                │                   │  │ Write to local  │                  │
    │                │                   │  │ hash map        │                  │
    │                │                   │  │ Append to repl  │                  │
    │                │                   │  │ backlog         │                  │
    │                │                   │  └─────────────────┤                  │
    │                │                   │                    │                  │
    │                │                   │  Async REPLICATE   │                  │
    │                │                   │──────────────────────────────────────►│
    │                │                   │                    │                  │
    │                │                   │                    │  ┌──────────────┤
    │                │                   │                    │  │ Apply to     │
    │                │                   │                    │  │ replica store │
    │                │                   │                    │  └──────────────┤
    │                │                   │                    │                  │
    │                │  +OK              │                    │                  │
    │                │◄──────────────────────────────────────│                  │
    │                │                   │                    │                  │
    │  +OK           │                   │                    │                  │
    │◄───────────────│                   │                    │                  │
    │                │                   │                    │                  │
```

**Key observations:**
- The hash ring lookup is O(log N) where N is the number of virtual nodes
- Replication is asynchronous — the primary responds before replicas confirm
- The replication backlog enables partial resync on replica reconnect
- MOVED redirect occurs if the client hits a non-owning node

### 4.2 GET Operation Flow (Cache Hit + Miss)

```
┌────────┐     ┌────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐
│ Client │     │ Proxy/     │     │ Hash Ring    │     │ Primary Node │     │ Replica  │
│        │     │ Router     │     │ Lookup       │     │              │     │ (fallback)│
└───┬────┘     └─────┬──────┘     └──────┬───────┘     └──────┬───────┘     └────┬─────┘
    │                │                   │                    │                  │
    │  GET k         │                   │                    │                  │
    │───────────────►│                   │                    │                  │
    │                │                   │                    │                  │
    │                │  Compute slot:    │                    │                  │
    │                │  slot=4721        │                    │                  │
    │                │  owner=NodeA      │                    │                  │
    │                │──────────────────►│                    │                  │
    │                │                   │                    │                  │
    │                │  Forward to NodeA │                    │                  │
    │                │──────────────────────────────────────►│                  │
    │                │                   │                    │                  │
    │                │                   │  ┌─────────────────┤                  │
    │                │                   │  │ Lookup key in   │                  │
    │                │                   │  │ hash map        │                  │
    │                │                   │  │ Check TTL       │                  │
    │                │                   │  │ Check liveness  │                  │
    │                │                   │  └────────┬────────┤                  │
    │                │                   │           │        │                  │
    │                │                   │     ┌─────▼─────┐  │                  │
    │                │                   │     │  HIT?     │  │                  │
    │                │                   │     └─────┬─────┘  │                  │
    │                │                   │       YES │   NO   │                  │
    │                │                   │           │        │                  │
    │                │                   │  ┌────────▼────────┐                  │
    │                │                   │  │ Return value    │                  │
    │                │                   │  │ Refresh LRU     │                  │
    │                │                   │  └────────┬────────┘                  │
    │                │  $value           │           │                          │
    │                │◄──────────────────────────────│                          │
    │  $value        │                   │           │                          │
    │◄───────────────│                   │           │                          │
    │                │                   │           │                          │
    │ ═══════ CACHE MISS SCENARIO ═══════════════════════════════════════════   │
    │                │                   │           │                          │
    │                │                   │  ┌────────▼────────┐                  │
    │                │                   │  │ Key not found   │                  │
    │                │                   │  │ in local store  │                  │
    │                │                   │  └────────┬────────┘                  │
    │                │  (nil)            │           │                          │
    │                │◄──────────────────────────────│                          │
    │                │                   │           │                          │
    │                │  Try replica:     │           │                          │
    │                │─────────────────────────────────────────────────────────►│
    │                │                   │           │                  GET k   │
    │                │                   │           │                  ┌───────┤
    │                │                   │           │                  │Lookup │
    │                │                   │           │                  └───┬───┤
    │                │  $value (or nil)  │           │                      │   │
    │                │◄────────────────────────────────────────────────────┘   │
    │  (nil)         │                   │           │                          │
    │◄───────────────│                   │           │                          │
    │                │                   │           │                          │
```

**Key observations:**
- On cache hit, the primary returns immediately without contacting replicas
- On cache miss, the proxy may attempt a replica read (configurable)
- LRU access tracking enables intelligent eviction
- TTL expiration is lazy (checked on access) with a background sweeper

### 4.3 Node Failure Detection and Failover

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node A  │     │  Node B  │     │  Node C  │     │  Node D  │     │  Client  │
│ (primary)│     │(replica) │     │(replica) │     │ (leader) │     │          │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │                │
     │ ════ NORMAL OPERATION ════      │                │                │
     │                │                │                │                │
     │◄─── Heartbeat ─┤                │                │                │
     │    (gossip)    │                │                │                │
     │                │◄─── Heartbeat ─┤                │                │
     │                │    (gossip)    │                │                │
     │                │                │◄─── Heartbeat ─┤                │
     │                │                │                │                │
     │ ════ NODE A CRASHES ════       │                │                │
     │                │                │                │                │
     │  ╳╳╳ DEAD ╳╳╳  │                │                │                │
     │                │                │                │                │
     │                │  Heartbeat     │                │                │
     │                │  timeout!      │                │                │
     │                │──────────────► │                │                │
     │                │                │                │                │
     │                │  Phi increases │                │                │
     │                │  phi=3.2       │                │                │
     │                │  (threshold=4) │                │                │
     │                │                │                │                │
     │                │  Still waiting │                │                │
     │                │  phi=4.1       │                │                │
     │                │  ▲▲▲ SUSPECTED ▲▲▲             │                │
     │                │                │                │                │
     │                │  Broadcast:    │                │                │
     │                │  "A suspected" │                │                │
     │                │───────────────────────────────►│                │
     │                │                │                │                │
     │                │                │  Broadcast:    │                │
     │                │                │  "A suspected" │                │
     │                │                │───────────────►│                │
     │                │                │                │                │
     │                │                │  ┌─────────────┤                │
     │                │                │  │ Node C also │                │
     │                │                │  │ detects A   │                │
     │                │                │  │ phi=5.0     │                │
     │                │                │  │ CONFIRMED   │                │
     │                │                │  └─────────────┤                │
     │                │                │                │                │
     │                │                │  Failover      │                │
     │                │                │  proposal to   │                │
     │                │                │  leader (D)    │                │
     │                │                │───────────────►│                │
     │                │                │                │                │
     │                │                │  ┌─────────────┤                │
     │                │                │  │ Leader:     │                │
     │                │                │  │ Validate    │                │
     │                │                │  │ A is down   │                │
     │                │                │  │ Promote B   │                │
     │                │                │  │ to primary  │                │
     │                │                │  └─────────────┤                │
     │                │                │                │                │
     │                │  You are now   │                │                │
     │                │  PRIMARY for   │                │                │
     │                │  slots 0-5460  │                │                │
     │                │◄────────────────────────────────│                │
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ B becomes   │                │                │
     │                │  │ primary     │                │                │
     │                │  │ Broadcasts  │                │                │
     │                │  │ new config  │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  Cluster config updated         │                │
     │                │────────────────────────────────────────────────►│
     │                │                │                │                │
```

**Key observations:**
- Phi-accrual detector uses adaptive thresholds — faster detection in stable networks
- Multiple nodes must agree before promoting a replica (quorum)
- The leader validates the failure before approving promotion
- Configuration change is propagated via gossip to all nodes

### 4.4 Rebalancing Flow (Node Join)

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node E  │     │  Node A  │     │  Node B  │     │  Node C  │     │  Leader  │
│ (new)    │     │          │     │          │     │          │     │  (D)     │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │                │
     │  Cluster bus   │                │                │                │
     │  announce      │                │                │                │
     │──────────────────────────────────────────────────────────────────►│
     │  "I exist,    │                │                │                │
     │   addr=X"     │                │                │                │
     │                │                │                │                │
     │                │                │                │  ┌─────────────┤
     │                │                │                │  │ Leader:     │
     │                │                │                │  │ Compute new │
     │                │                │                │  │ slot map    │
     │                │                │                │  │ Redistribute│
     │                │                │                │  │ 16384 slots │
     │                │                │                │  └─────────────┤
     │                │                │                │                │
     │  SLOTMAP v2    │                │                │                │
     │  (broadcast)   │                │                │                │
     │◄─────────────────────────────────────────────────────────────────│
     │                │                │                │                │
     │                │  SLOTMAP v2    │                │                │
     │                │◄────────────────────────────────────────────────│
     │                │                │                │                │
     │                │                │  SLOTMAP v2    │                │
     │                │                │◄───────────────────────────────│
     │                │                │                │                │
     │                │                │                │  SLOTMAP v2    │
     │                │                │                │◄───────────────│
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ Node A:     │                │                │
     │                │  │ slots to    │                │                │
     │                │  │ migrate:    │                │                │
     │                │  │ [2730-5460] │                │                │
     │                │  │ → Node E    │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  MIGRATE slot 2730              │                │
     │                │  data to E    │                │                │
     │────────────────────────────────►│                │                │
     │  ┌─────────────┤                │                │                │
     │  │ E receives  │                │                │                │
     │  │ slot data   │                │                │                │
     │  │ Sets IMPORT │                │                │                │
     │  │ state       │                │                │                │
     │  └─────────────┤                │                │                │
     │                │                │                │                │
     │  MIGRATION     │                │                │                │
     │  COMPLETE for  │                │                │                │
     │  slot 2730     │                │                │                │
     │◄───────────────│                │                │                │
     │                │                │                │                │
     │  ... repeat for all slots ...   │                │                │
     │                │                │                │                │
     │  ALL SLOTS     │                │                │                │
     │  MIGRATED      │                │                │                │
     │─────────────────────────────────────────────────────────────────►│
     │                │                │                │                │
     │                │                │                │  ┌─────────────┤
     │                │                │                │  │ Leader:     │
     │                │                │                │  │ Finalize    │
     │                │                │                │  │ new topology│
     │                │                │                │  └─────────────┤
     │                │                │                │                │
     │  SLOTMAP v3    │                │                │                │
     │  (final)       │                │                │                │
     │◄─────────────────────────────────────────────────────────────────│
     │  ┌─────────────┤                │                │                │
     │  │ E now       │                │                │                │
     │  │ STABLE      │                │                │                │
     │  │ owns slots  │                │                │                │
     │  └─────────────┤                │                │                │
```

**Key observations:**
- Slot migration is atomic per slot — a slot is either fully on source or fully on target
- During migration, the MIGRATING node serves reads but redirects writes via ASK
- The IMPORTING node only accepts keys for slots it is importing (with ASK flag)
- Slot map versioning (v1→v2→v3) prevents split-brain during transitions

### 4.5 Leader Election Flow

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node A  │     │  Node B  │     │  Node C  │     │  Node D  │     │  Node E  │
│ follower │     │ follower │     │ follower │     │ follower │     │ follower │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │                │
     │ ════ INITIAL STATE: term=1, leader=D ═══════════════════════════│
     │                │                │                │                │
     │ ════ D CRASHES ════             │                │                │
     │                │                │                │                │
     │                │                │                │  ╳╳╳ DEAD ╳╳╳  │
     │                │                │                │                │
     │  No heartbeat  │                │                │                │
     │  from leader   │                │                │                │
     │  for 5s        │                │                │                │
     │                │                │                │                │
     │ ════ NODE B BECOMES CANDIDATE ═══│══════════════════════════════│
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ election    │                │                │
     │                │  │ timeout     │                │                │
     │                │  │ rand 150ms  │                │                │
     │                │  │ elapsed!    │                │                │
     │                │  │             │                │                │
     │                │  │ Increment   │                │                │
     │                │  │ term: 1→2   │                │                │
     │                │  │ Vote for    │                │                │
     │                │  │ self        │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  RequestVote   │                │                │
     │                │  (term=2,      │                │                │
     │                │   lastLogIdx)  │                │                │
     │                │───────────────►│                │                │
     │                │                │                │                │
     │                │  RequestVote   │                │                │
     │                │  (term=2)      │                │                │
     │                │───────────────────────────────►│                │
     │                │                │                │                │
     │                │  RequestVote   │                │                │
     │                │  (term=2)      │                │                │
     │                │───────────────────────────────────────────────►│
     │                │                │                │                │
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ C checks:   │                │                │
     │                │  │ term=2 > 1? │                │                │
     │                │  │ YES → grant │                │                │
     │                │  │ vote        │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  VoteGranted   │                │                │
     │                │  (term=2)      │                │                │
     │                │◄───────────────│                │                │
     │                │                │                │                │
     │                │  VoteGranted   │                │                │
     │                │  (term=2)      │                │                │
     │                │◄───────────────────────────────────────────────│
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ B has       │                │                │
     │                │  │ majority    │                │                │
     │                │  │ (3/5)       │                │                │
     │                │  │ BECOMES     │                │                │
     │                │  │ LEADER      │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  AppendEntries │                │                │
     │                │  (term=2,      │                │                │
     │                │   heartbeat)   │                │                │
     │                │───────────────►│                │                │
     │                │                │                │                │
     │                │  AppendEntries │                │                │
     │                │───────────────────────────────►│                │
     │                │                │                │                │
     │                │  AppendEntries │                │                │
     │                │───────────────────────────────────────────────►│
     │                │                │                │                │
     │  Update to     │                │                │                │
     │  term=2        │                │                │                │
     │◄───────────────│                │                │                │
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ All followers│               │                │
     │                │  │ acknowledge │                │                │
     │                │  │ B as leader │                │                │
     │                │  │ in term=2   │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
```

**Key observations:**
- Election timeout is randomized (150-300ms) to prevent split votes
- A candidate only grants vote if the requesting term is ≥ its own term
- A candidate only grants vote if the requesting log is at least as up-to-date
- Leader sends heartbeats every 150ms to maintain authority
- If election fails (split vote), nodes wait for a random timeout and retry

### 4.6 Heartbeat Gossip Propagation

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node A  │     │  Node B  │     │  Node C  │     │  Node D  │     │  Node E  │
│ 10.0.0.1 │     │ 10.0.0.2 │     │ 10.0.0.3 │     │ 10.0.0.4 │     │ 10.0.0.5 │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │                │
     │ ════ PERIODIC GOSSIP TICK (every 1s) ═══════════════════════════│
     │                │                │                │                │
     │  ┌─────────────┤                │                │                │
     │  │ A constructs │               │                │                │
     │  │ gossip msg:  │               │                │                │
     │  │ - own state  │               │                │                │
     │  │ - known nodes│               │                │                │
     │  │ - slot map   │               │                │                │
     │  │ - vector clk │               │                │                │
     │  └─────────────┤                │                │                │
     │                │                │                │                │
     │  Select 2 random peers          │                │                │
     │───────────────►│                │                │                │
     │  (gossip msg)  │                │                │                │
     │                │                │                │                │
     │                │  ┌─────────────┤                │                │
     │                │  │ B merges    │                │                │
     │                │  │ gossip state│                │                │
     │                │  │ Detects:    │                │                │
     │                │  │ A reports   │                │                │
     │                │  │ D as healthy│                │                │
     │                │  │ Update own  │                │                │
     │                │  │ view        │                │                │
     │                │  └─────────────┤                │                │
     │                │                │                │                │
     │                │  Forward gossip│                │                │
     │                │  to 2 new      │                │                │
     │                │  random peers  │                │                │
     │                │───────────────────────────────►│                │
     │                │                │                │                │
     │                │                │  ┌─────────────┤                │
     │                │                │  │ C merges    │                │
     │                │                │  │ state       │                │
     │                │                │  │ Now knows   │                │
     │                │                │  │ about A, B  │                │
     │                │                │  │ D, E health │                │
     │                │                │  └─────────────┤                │
     │                │                │                │                │
     │                │                │                │  Forward to    │
     │                │                │                │  2 random      │
     │                │                │                │───────────────►│
     │                │                │                │                │
     │  ┌─────────────┤                │                │                │
     │  │ After N     │                │                │                │
     │  │ rounds,     │                │                │                │
     │  │ ALL nodes   │                │                │                │
     │  │ have        │                │                │                │
     │  │ converged   │                │                │                │
     │  │ view of     │                │                │                │
     │  │ cluster     │                │                │                │
     │  └─────────────┤                │                │                │
     │                │                │                │                │
     │ ════ CONVERGENCE: log₂(N) rounds ══════════════════════════════│
     │                │                │                │                │
     │  Node states:  │                │                │                │
     │  A: {A:1, B:1, │ C:1, D:1, E:1}               │                │
     │  B: {A:1, B:1, │ C:1, D:1, E:1}               │                │
     │  C: {A:1, B:1, │ C:1, D:1, E:1}               │                │
     │  D: {A:1, B:1, │ C:1, D:1, E:1}               │                │
     │  E: {A:1, B:1, │ C:1, D:1, E:1}               │                │
```

**Key observations:**
- Gossip converges in O(log N) rounds — for 100 nodes, ~7 rounds
- Each message is small (O(N) metadata, not data) — typically < 1KB
- Anti-entropy via Merckle tree comparison catches missed updates
- Gossip piggybacks failure detector heartbeats to reduce message overhead
- Vector clocks track causal ordering of state changes

---

## 5. Data Flow

### 5.1 Write Path

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         WRITE PATH                                      │
│                                                                         │
│  Client Request                                                         │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────────┐                                                       │
│  │ Parse RESP   │ Decode command, extract key, value, args             │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Slot Lookup  │ slot = CRC16(key) % 16384                            │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐     ┌──────────────┐                                  │
│  │ Slot Owner?  │──NO─│ MOVED/ASK   │ Return redirect to client       │
│  └──────┬───────┘     └──────────────┘                                  │
│         │ YES                                                           │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Acquire Lock │ Per-key write lock (sharded lock table)              │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Write to     │ Hash table put + LRU update + TTL scheduling         │
│  │ Local Store  │                                                       │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ├──► Memory Accounting (update used_memory)                     │
│         │                                                               │
│         ├──► Expire Scheduling (if TTL set)                             │
│         │                                                               │
│         ├──► Write-Ahead Log (optional, for crash recovery)             │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Append to    │ Append command to replication backlog ring buffer    │
│  │ Repl Backlog │ Size = repl_backlog_size (default 1MB)              │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Async Replic │ Fan-out to R replicas asynchronously                 │
│  │              │ Via dedicated replication goroutine per replica       │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ├──► Replica 1: Apply to store + ACK                           │
│         ├──► Replica 2: Apply to store + ACK                           │
│         └──► Replica R: Apply to store + ACK                           │
│                                                                         │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Return +OK   │ Do NOT wait for replica ACKs (async replication)     │
│  │ to Client    │                                                       │
│  └──────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Read Path

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         READ PATH                                       │
│                                                                         │
│  Client Request                                                         │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────────┐                                                       │
│  │ Parse RESP   │ Decode command, extract key                          │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Slot Lookup  │ slot = CRC16(key) % 16384                            │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐     ┌──────────────┐                                  │
│  │ Slot Owner?  │──NO─│ MOVED        │ Return redirect to client       │
│  └──────┬───────┘     └──────────────┘                                  │
│         │ YES                                                           │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Acquire Lock │ Per-key read lock (shared, allows concurrent reads)  │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Lookup Key   │ Hash table get                                        │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Key Found?   │                                                       │
│  └──┬───────┬───┘                                                       │
│   YES      NO                                                           │
│    │        │                                                           │
│    ▼        ▼                                                           │
│  ┌─────┐  ┌──────────┐                                                 │
│  │ TTL │  │ MISS     │                                                 │
│  │ Exp?│  │ Try      │                                                 │
│  └──┬──┘  │ Replica  │                                                 │
│   Y │ N   │ (config) │                                                 │
│    │ │    └────┬─────┘                                                 │
│    │ │         │                                                       │
│    ▼ ▼         ▼                                                       │
│  ┌───────┐  ┌──────┐                                                   │
│  │ Delete│  │Return│                                                   │
│  │ +Miss │  │nil   │                                                   │
│  └───┬───┘  └──┬───┘                                                   │
│      │         │                                                       │
│      ▼         ▼                                                       │
│  ┌────────────────┐                                                     │
│  │ Update LRU     │ Move key to head of LRU list                      │
│  │ Access Time    │                                                     │
│  └────────┬───────┘                                                     │
│           │                                                             │
│           ▼                                                             │
│  ┌────────────────┐                                                     │
│  │ Return Value   │ Encode as RESP bulk string                        │
│  │ to Client      │                                                     │
│  └────────────────┘                                                     │
│                                                                         │
│  READ REPAIR (on replica miss):                                         │
│  ┌────────────────┐                                                     │
│  │ If replica read │                                                   │
│  │ returns stale   │                                                   │
│  │ data, trigger   │                                                   │
│  │ async repair    │                                                   │
│  └────────────────┘                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Delete Path

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DELETE PATH                                     │
│                                                                         │
│  Client: DEL key                                                        │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────────┐                                                       │
│  │ Slot Lookup  │ → Slot owner node                                    │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Soft Delete  │ Mark key as "deleted" in hash table                  │
│  │              │ (lazy deletion, tombstone-style)                     │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ├──► Cancel expire timer (if any)                              │
│         ├──► Update memory accounting                                  │
│         └──► Add to replication backlog                                │
│                                                                         │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Async Replic │ Fan-out DEL to replicas                             │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │ Background   │ Periodic scan removes tombstoned keys                │
│  │ Compaction   │ (every 100ms, processes 100 keys per cycle)          │
│  └──────────────┘                                                       │
│                                                                         │
│  NOTE: DEL is synchronous for the caller — client receives integer      │
│  count of deleted keys. The background compaction is a performance      │
│  optimization, not a correctness requirement.                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Consistent Hashing

### 6.1 Hash Ring Visualization

```
                              Consistent Hash Ring (16384 slots)
                    
                         ┌────────────────────────────────┐
                         │            0                   │
                    5460 ┤                                │ 10923
                         │                                │
                         │    ┌─────────────────────┐     │
                         │    │  ·   ·   ·   ·      │     │
                         │  · │ A1  ·  B1  ·  C1    │ ·   │
                         │    │  ·   ·   ·   ·      │     │
                         │  · │  ·   ·   ·   ·      │ ·   │
              Node A     │    │ A2  ·  B2  ·  C2    │     │    Node B
              (0-5460)   │  · │  ·   ·   ·   ·      │ ·   │    (5461-10922)
                         │    │ A3  ·  B3  ·  C3    │     │
                         │  · │  ·   ·   ·   ·      │ ·   │
                         │    │ A4  ·  B4  ·  C4    │     │
                         │  · │  ·   ·   ·   ·      │ ·   │
                         │    │ A5  ·  B5  ·  C5    │     │
                         │  · └─────────────────────┘ ·   │
                         │                                │
                         │   Node C (10923-16383)         │
                         │   · represents virtual nodes   │
                         └────────────────────────────────┘
                                  8192 (bottom)

    Legend:
    A1-A5 = Virtual nodes for Node A (owned slots: 0-5460)
    B1-B5 = Virtual nodes for Node B (owned slots: 5461-10922)
    C1-C5 = Virtual nodes for Node C (owned slots: 10923-16383)

    Each key is mapped to a slot via: slot = CRC16(key) % 16384
    The key is assigned to the first virtual node encountered clockwise.
```

### 6.2 Virtual Nodes Detail

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     VIRTUAL NODE DISTRIBUTION                                  │
│                                                                                │
│  Real Node A ──┬──► VA-0  ──► hash("A:0") % 16384 ──► slot 127              │
│                ├──► VA-1  ──► hash("A:1") % 16384 ──► slot 8934             │
│                ├──► VA-2  ──► hash("A:2") % 16384 ──► slot 3291             │
│                ├──► VA-3  ──► hash("A:3") % 16384 ──► slot 14200            │
│                ├──► VA-4  ──► hash("A:4") % 16384 ──► slot 6722             │
│                ├──► ...                                                       │
│                └──► VA-N  ──► hash("A:N") % 16384 ──► slot 4587             │
│                                                                                │
│  Number of virtual nodes per real node: 150 (configurable)                    │
│  This provides approximately uniform distribution across all 16384 slots.     │
│                                                                                │
│  KEY LOOKUP:                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │  1. key → CRC16(key) % 16384 = slot                                  │   │
│  │  2. slot → binary search sorted virtual node list → O(log N)          │   │
│  │  3. virtual node → real node (from routing table)                     │   │
│  │  4. Route to real node                                                │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│                                                                                │
│  LOAD BALANCING:                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │  With 150 virtual nodes per real node, standard deviation of          │   │
│  │  load is approximately sqrt(150) ≈ 12.2, giving ~0.8% imbalance     │   │
│  │  for a 3-node cluster. This matches Redis Cluster behavior.          │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│                                                                                │
│  SLOTS MAP STRUCTURE:                                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │  SlotMap[16384] → NodeID                                              │   │
│  │                                                                       │   │
│  │  Example (3 nodes):                                                   │   │
│  │  slots[0]     = NodeA                                                 │   │
│  │  slots[1]     = NodeA                                                 │   │
│  │  ...                                                                  │   │
│  │  slots[5460]  = NodeA                                                 │   │
│  │  slots[5461]  = NodeB                                                 │   │
│  │  ...                                                                  │   │
│  │  slots[10922] = NodeB                                                 │   │
│  │  slots[10923] = NodeC                                                 │   │
│  │  ...                                                                  │   │
│  │  slots[16383] = NodeC                                                 │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Replication Strategy

### 7.1 Asynchronous Replication Architecture

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                      ASYNC REPLICATION FLOW                                    │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    PRIMARY NODE                                         │  │
│  │                                                                         │  │
│  │  Client: SET foo bar                                                    │  │
│  │       │                                                                 │  │
│  │       ▼                                                                 │  │
│  │  ┌──────────┐     ┌──────────────┐     ┌──────────────────┐            │  │
│  │  │ Write to │────►│ Replication  │────►│  Replication     │            │  │
│  │  │ local    │     │ Backlog      │     │  Manager         │            │  │
│  │  │ store    │     │ (ring buffer)│     │                  │            │  │
│  │  └──────────┘     │              │     │  Per-replica:    │            │  │
│  │                   │  ┌────────┐  │     │  - send goroutine│            │  │
│  │                   │  │ offset │  │     │  - ACK handler   │            │  │
│  │                   │  │ ─────► │  │     │  - lag tracker   │            │  │
│  │                   │  │ r1: 42 │  │     └────────┬─────────┘            │  │
│  │                   │  │ r2: 38 │  │              │                      │  │
│  │                   │  │ r3: 0  │  │              │                      │  │
│  │                   │  └────────┘  │              │                      │  │
│  │                   └──────────────┘              │                      │  │
│  └────────────────────────────────────────────────┼───────────────────────┘  │
│                                                    │                         │
│            ┌───────────────────────────────────────┼───────────────┐          │
│            │                                       │               │          │
│            ▼                                       ▼               ▼          │
│  ┌──────────────────┐              ┌──────────────────┐  ┌──────────────┐    │
│  │ Replica 1        │              │ Replica 2        │  │ Replica 3    │    │
│  │                  │              │                  │  │ (lagging)    │    │
│  │ REPLAY commands  │              │ REPLAY commands  │  │              │    │
│  │ from backlog     │              │ from backlog     │  │ May need     │    │
│  │                  │              │                  │  │ FULL RESYNC  │    │
│  │ ACK to primary   │              │ ACK to primary   │  │ if backlog   │    │
│  │ after apply      │              │ after apply      │  │ overflows    │    │
│  │                  │              │                  │  │              │    │
│  │ Lag: 4 commands  │              │ Lag: 8 commands  │  │ Lag: ???     │    │
│  └──────────────────┘              └──────────────────┘  └──────────────┘    │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                     REPLICATION BACKLOG                                  │  │
│  │                                                                         │  │
│  │  Size: 1MB (configurable via repl_backlog_size)                        │  │
│  │                                                                         │  │
│  │  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐   │  │
│  │  │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │ S │   │  │
│  │  │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │ E │   │  │
│  │  │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │ T │   │  │
│  │  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘   │  │
│  │  ◄── head (oldest)                    tail (newest) ──►               │  │
│  │                                                                         │  │
│  │  When backlog fills: oldest entries are overwritten                    │  │
│  │  Replicas that fall behind overflow → trigger FULL RESYNC              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Replication Lag Tracking

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     REPLICATION LAG TRACKER                                    │
│                                                                                │
│  Lag = primary_offset - replica_offset                                        │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  Metric              │ Description            │ Alert Threshold         │  │
│  │──────────────────────┼────────────────────────┼─────────────────────────│  │
│  │  repl_offset         │ Current replication    │ (informational)        │  │
│  │                      │ offset on primary      │                         │  │
│  │──────────────────────┼────────────────────────┼─────────────────────────│  │
│  │  repl_lag_bytes      │ Bytes behind primary   │ > 1MB → warning        │  │
│  │                      │                        │ > 10MB → critical      │  │
│  │──────────────────────┼────────────────────────┼─────────────────────────│  │
│  │  repl_lag_seconds    │ Estimated time behind  │ > 1s → warning         │  │
│  │                      │ (based on write rate)  │ > 5s → critical        │  │
│  │──────────────────────┼────────────────────────┼─────────────────────────│  │
│  │  repl_ack_pending    │ Number of unACKed      │ > 1000 → warning       │  │
│  │                      │ replication commands   │                         │  │
│  │──────────────────────┼────────────────────────┼─────────────────────────│  │
│  │  full_resync_count   │ Total full resyncs     │ > 3/hour → investigate  │  │
│  │                      │ completed              │                         │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  LAG MONITORING LOOP:                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  Every 100ms:                                                           │  │
│  │    for each replica r:                                                  │  │
│  │      lag = primary.backlog_offset - r.ack_offset                       │  │
│  │      r.lag_bytes = lag                                                 │  │
│  │      r.lag_seconds = lag / avg_write_rate (last 10s)                   │  │
│  │      if lag > repl_backlog_size:                                       │  │
│  │        trigger_full_resync(r)  // replica fell behind                  │  │
│  │        log.warn("replica %s fell behind, triggering full resync", r)   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Promotion Flow

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     REPLICA PROMOTION FLOW                                     │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  Step 1: Detect primary failure                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │  Phi-accrual detector on each replica:                          │   │  │
│  │  │  phi(primary) > threshold (default: 4.0)                        │   │  │
│  │  │  → Mark primary as SUSPECTED                                    │   │  │
│  │  │  → Wait for agreement from (N/2 + 1) replicas                  │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                         │  │
│  │  Step 2: Select best replica for promotion                              │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │  Candidate selection criteria:                                  │   │  │
│  │  │  1. Highest replication offset (most data)                      │   │  │
│  │  │  2. Lowest lag to primary (most up-to-date)                     │   │  │
│  │  │  3. Node health (CPU, memory, network)                          │   │  │
│  │  │                                                                 │   │  │
│  │  │  Sort by: offset DESC, then lag ASC, then health DESC          │   │  │
│  │  │  Select top candidate                                           │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                         │  │
│  │  Step 3: Leader approves promotion                                      │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │  Leader receives failover proposal                              │   │  │
│  │  │  Validates: primary is actually down (cross-check with others)  │   │  │
│  │  │  Validates: candidate is healthy and has sufficient data        │   │  │
│  │  │  Approves promotion via cluster config update                   │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                         │  │
│  │  Step 4: Promote replica                                                │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │  Replica transitions: REPLICA → PRIMARY                         │   │  │
│  │  │  - Stops accepting replication from (dead) primary              │   │  │
│  │  │  - Starts accepting client writes                               │   │  │
│  │  │  - Broadcasts new role via gossip                               │   │  │
│  │  │  - Updates slot ownership map                                   │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                         │  │
│  │  Step 5: Reconfigure cluster                                            │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │  Leader broadcasts new slotmap to all nodes                     │   │  │
│  │  │  Former replicas of dead primary:                               │   │  │
│  │  │  - Repoint to new primary                                       │   │  │
│  │  │  - Trigger partial/full resync from new primary                 │   │  │
│  │  │                                                                 │   │  │
│  │  │  New primary:                                                   │   │  │
│  │  │  - Starts replication to remaining replicas                    │   │  │
│  │  │  - Monitors replication lag                                     │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Heartbeat & Failure Detection

### 8.1 Gossip Protocol

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                        GOSSIP PROTOCOL                                         │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  GOSSIP MESSAGE FORMAT                                                   │  │
│  │                                                                         │  │
│  │  type GossipMessage struct {                                           │  │
│  │      SenderID    NodeID                                                │  │
│  │      Term        uint64       // monotonic counter                     │  │
│  │      NodeStates  []NodeState  // sender's view of all nodes            │  │
│  │      SlotMap     []SlotRange  // slot ownership (versioned)            │  │
│  │      VectorClock VectorClock  // causal ordering                       │  │
│  │  }                                                                     │  │
│  │                                                                         │  │
│  │  type NodeState struct {                                               │  │
│  │      ID          NodeID                                                │  │
│  │      Address     string                                                │  │
│  │      Status      NodeStatus  // alive, suspected, dead                │  │
│  │      LastSeen    time.Time                                             │  │
│  │      Phi         float64     // phi-accrual value                     │  │
│  │      Role        Role        // primary, replica, candidate, leader   │  │
│  │  }                                                                     │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  GOSSIP ALGORITHM                                                        │  │
│  │                                                                         │  │
│  │  Every gossip_interval (1s):                                            │  │
│  │                                                                         │  │
│  │  1. Select k = min(3, len(peers)) random peers (fanout)               │  │
│  │  2. For each selected peer:                                            │  │
│  │     a. Send full gossip message                                        │  │
│  │     b. Receive peer's gossip message                                   │  │
│  │     c. Merge: for each node in peer's message:                        │  │
│  │        - If peer's term > local term for that node:                   │  │
│  │          adopt peer's state                                            │  │
│  │        - If peer's term == local term AND peer has more info:          │  │
│  │          merge (take the "better" state)                               │  │
│  │  3. Update own vector clock                                            │  │
│  │  4. Broadcast slotmap if changed                                       │  │
│  │                                                                         │  │
│  │  CONVERGENCE: O(log N) rounds for full cluster awareness               │  │
│  │  For N=100: ~7 rounds × 1s = 7 seconds for full convergence          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  GOSSIP OVERHEAD CALCULATION                                            │  │
│  │                                                                         │  │
│  │  Message size: ~100 bytes per node state × N nodes ≈ 10KB for 100 nodes│  │
│  │  Fanout: 3 peers per tick                                              │  │
│  │  Frequency: 1 tick/second                                              │  │
│  │  Total bandwidth per node: 3 × 10KB × 2 (send+recv) = 60KB/s        │  │
│  │  For 100-node cluster: 6MB/s total (negligible)                       │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Phi-Accrual Failure Detector

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     PHI-ACCRUAL FAILURE DETECTOR                              │
│                                                                                │
│  Reference: "The Phi Accrual Failure Detector" (Hayashibara et al., 2004)    │
│  Also used by: Cassandra, Akka                                               │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  CORE IDEA                                                               │  │
│  │                                                                         │  │
│  │  Instead of binary alive/dead, phi-accrual outputs a suspicion level   │  │
│  │  (phi value) that increases as more time passes without a heartbeat.   │  │
│  │                                                                         │  │
│  │  phi = -log10(P_later(t))                                               │  │
│  │  where P_later(t) is the probability that the next heartbeat will      │  │
│  │  arrive more than t seconds after the previous one.                    │  │
│  │                                                                         │  │
│  │  phi = 1.0  → 90% chance node is down                                  │  │
│  │  phi = 2.0  → 99% chance node is down                                  │  │
│  │  phi = 3.0  → 99.9% chance node is down                                │  │
│  │  phi = 4.0  → 99.99% chance node is down (DEFAULT THRESHOLD)          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  HEARTBEAT PROCESSING                                                    │  │
│  │                                                                         │  │
│  │  type PhiAccrualDetector struct {                                      │  │
│  │      intervals    []float64    // recent inter-heartbeat intervals     │  │
│  │      windowSize   int          // sliding window size (default: 100)   │  │
│  │      threshold    float64      // phi value to declare dead (default: 4)│  │
│  │      minSamples   int          // min samples before declaring dead    │  │
│  │  }                                                                     │  │
│  │                                                                         │  │
│  │  func (d *PhiAccrualDetector) Heartbeat(nodeID NodeID) {              │  │
│  │      now := time.Now()                                                 │  │
│  │      if last, ok := d.lastHeartbeat[nodeID]; ok {                     │  │
│  │          interval := now.Sub(last).Seconds()                           │  │
│  │          d.intervals[nodeID] = append(d.intervals[nodeID], interval)  │  │
│  │          if len(d.intervals[nodeID]) > d.windowSize {                 │  │
│  │              d.intervals[nodeID] = d.intervals[nodeID][1:]            │  │
│  │          }                                                             │  │
│  │      }                                                                 │  │
│  │      d.lastHeartbeat[nodeID] = now                                     │  │
│  │  }                                                                     │  │
│  │                                                                         │  │
│  │  func (d *PhiAccrualDetector) GetPhi(nodeID NodeID) float64 {         │  │
│  │      elapsed := time.Since(d.lastHeartbeat[nodeID]).Seconds()          │  │
│  │      mean, variance := d.stats(nodeID)                                │  │
│  │      stddev := math.Sqrt(variance)                                     │  │
│  │      // Assuming Gaussian distribution of intervals                   │  │
│  │      cdf := normalCDF(elapsed, mean, stddev)                          │  │
│  │      phi := -math.Log10(1 - cdf)                                      │  │
│  │      return phi                                                        │  │
│  │  }                                                                     │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHI VALUE VISUALIZATION                                                │  │
│  │                                                                         │  │
│  │  phi                                                                    │  │
│  │   5 ┤                                               ╱ ALIVE → DEAD     │  │
│  │   4 ┤─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─╱─ THRESHOLD          │  │
│  │   3 ┤                                          ╱                       │  │
│  │   2 ┤                                     ╱                            │  │
│  │   1 ┤                                ╱                                 │  │
│  │   0 ┤──────────────────────────────────────────── time since heartbeat │  │
│  │       0s    1s    2s    3s    4s    5s                               │  │
│  │                                                                         │  │
│  │  The curve shape adapts based on observed inter-heartbeat intervals.   │  │
│  │  If heartbeats are typically 1s apart, phi rises sharply after 1s.     │  │
│  │  If heartbeats are typically 2s apart, phi rises more gradually.       │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Leader Election

### 9.1 Simplified Raft Protocol

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     RAFT-INSPIRED LEADER ELECTION                             │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  STATE MACHINE (per node)                                               │  │
│  │                                                                         │  │
│  │                    ┌──────────────┐                                    │  │
│  │                    │   FOLLOWER   │◄──────────────────────┐            │  │
│  │                    │              │   AppendEntries from  │            │  │
│  │                    │  term = T    │   valid leader        │            │  │
│  │                    │  votedFor = -│                       │            │  │
│  │                    └──────┬───────┘                       │            │  │
│  │                           │                              │            │  │
│  │              election     │  election                    │            │  │
│  │              timeout      │  timeout                     │            │  │
│  │              (150-300ms)  │  (split vote)               │            │  │
│  │                           │                              │            │  │
│  │                    ┌──────▼───────┐                       │            │  │
│  │                    │  CANDIDATE   │───────┐               │            │  │
│  │                    │              │       │               │            │  │
│  │                    │  term = T+1  │       │  receives     │            │  │
│  │                    │  votedFor =  │       │  AppendEntries│            │  │
│  │                    │   self       │       │  from valid   │            │  │
│  │                    └──────┬───────┘       │  leader       │            │  │
│  │                           │               │               │            │  │
│  │              wins          │  loses        │               │            │  │
│  │              majority      │  election     │               │            │  │
│  │              (N/2+1 votes) │               │               │            │  │
│  │                           │               │               │            │  │
│  │                    ┌──────▼───────┐       │               │            │  │
│  │                    │   LEADER     │───────┘               │            │  │
│  │                    │              │                       │            │  │
│  │                    │  term = T+1  │  detects higher term  │            │  │
│  │                    │  sends       │──────────────────────►│            │  │
│  │                    │  heartbeats  │                       │            │  │
│  │                    └──────────────┘                       │            │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  KEY INVARIANTS                                                          │  │
│  │                                                                         │  │
│  │  1. At most one leader per term                                         │  │
│  │  2. Leader's log is always at least as up-to-date as any follower's    │  │
│  │  3. A node only votes once per term                                    │  │
│  │  4. A candidate increments term before requesting votes                │  │
│  │  5. Heartbeat interval < election timeout                              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  HYDRACACHE SIMPLIFICATIONS vs FULL RAFT                                │  │
│  │                                                                         │  │
│  │  1. No persistent log for config changes (gossip propagates state)    │  │
│  │  2. No log replication (state is in-memory, replicated via gossip)    │  │
│  │  3. Config changes are lightweight (just slot ownership changes)      │  │
│  │  4. Leader only used for cluster coordination (failover, rebalance)   │  │
│  │  5. Data writes bypass the leader entirely (direct to slot owner)     │  │
│  │                                                                         │  │
│  │  This gives us leader election benefits without the overhead of        │  │
│  │  full consensus on every write.                                        │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  TIMING CONSTANTS                                                       │  │
│  │                                                                         │  │
│  │  election_timeout_min  = 150ms                                         │  │
│  │  election_timeout_max  = 300ms  (randomized to prevent split votes)    │  │
│  │  heartbeat_interval    = 100ms  (leader sends heartbeats)              │  │
│  │  follower_timeout      = 300ms  (no heartbeat → become candidate)     │  │
│  │                                                                         │  │
│  │  Constraint: heartbeat_interval < election_timeout_min                 │  │
│  │  This ensures followers don't become candidates while leader          │  │
│  │  heartbeats are merely delayed (not lost).                            │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Self-Healing Flow

### 10.1 Complete Failover Sequence

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     COMPLETE SELF-HEALING FLOW                                │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHASE 1: DETECTION (0-5s)                                              │  │
│  │                                                                         │  │
│  │  T=0.0s   Node A crashes (process killed, network partition, etc.)     │  │
│  │  T=0.0s   Gossip tick: Node B tries to send heartbeat to A            │  │
│  │  T=0.1s   Node C also tries to send heartbeat to A                    │  │
│  │  T=0.5s   Phi-accrual detector: phi(A) = 1.2 on Node B                │  │
│  │  T=1.0s   Phi-accrual detector: phi(A) = 2.8 on Node B                │  │
│  │  T=1.5s   Phi-accrual detector: phi(A) = 3.5 on Node B                │  │
│  │  T=2.0s   phi(A) = 4.1 > threshold → Node B marks A as SUSPECTED     │  │
│  │  T=2.0s   Node B broadcasts "A suspected" via gossip                   │  │
│  │  T=2.3s   Node C also detects phi(A) > threshold → confirms           │  │
│  │  T=2.5s   (N/2 + 1) = 2 replicas agree: A is DEAD                    │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHASE 2: FAILOVER DECISION (5-6s)                                      │  │
│  │                                                                         │  │
│  │  T=2.5s   Failover candidate selection:                                │  │
│  │           - Node B: offset=15000, lag=2s, health=95%                   │  │
│  │           - Node C: offset=14800, lag=3s, health=90%                   │  │
│  │           Winner: Node B (highest offset, lowest lag)                  │  │
│  │                                                                         │  │
│  │  T=2.5s   Proposal sent to leader (Node D)                            │  │
│  │  T=2.6s   Leader validates: cross-checks with Nodes C, E              │  │
│  │  T=2.7s   Leader confirms A is unreachable from 3/5 nodes             │  │
│  │  T=2.8s   Leader approves promotion of Node B                         │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHASE 3: PROMOTION (6-8s)                                              │  │
│  │                                                                         │  │
│  │  T=3.0s   Leader sends PROMOTE to Node B                               │  │
│  │  T=3.0s   Node B: REPLICA → PRIMARY transition                         │  │
│  │           - Stops replication from A                                    │  │
│  │           - Starts accepting client writes                             │  │
│  │           - Broadcasts new role via gossip                             │  │
│  │                                                                         │  │
│  │  T=3.1s   Leader broadcasts new slotmap:                               │  │
│  │           slots[0-5460] → Node B (was Node A)                          │  │
│  │                                                                         │  │
│  │  T=3.2s   All nodes receive new slotmap via gossip                     │  │
│  │  T=3.3s   Proxy layer updates hash ring                                │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHASE 4: RECONFIGURATION (8-15s)                                       │  │
│  │                                                                         │  │
│  │  T=3.5s   Node C repoints to Node B (was replica of A, now of B)      │  │
│  │  T=3.5s   Node C: partial resync from B (from last known offset)      │  │
│  │  T=4.0s   Node E joins as new replica of Node B                        │  │
│  │  T=4.0s   Node E: full resync from B (new replica)                    │  │
│  │  T=5.0s   Node C resync complete (caught up)                          │  │
│  │  T=8.0s   Node E resync complete (full sync takes longer)             │  │
│  │                                                                         │  │
│  │  FINAL STATE:                                                           │  │
│  │  - Node B: Primary for slots 0-5460                                    │  │
│  │  - Node C: Replica for Node B                                          │  │
│  │  - Node E: Replica for Node B (new)                                    │  │
│  │  - Node A: DEAD (will be re-added manually or via auto-discovery)     │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  PHASE 5: POST-HEALING (15s+)                                           │  │
│  │                                                                         │  │
│  │  - Cluster operates at R-1 replication factor until new replica joins  │  │
│  │  - Monitoring alerts: "Node A down, failover complete"                 │  │
│  │  - If Node A recovers:                                                │  │
│  │    a. Node A joins as replica of Node B (its successor)                │  │
│  │    b. Full resync from B                                               │  │
│  │    c. Once synced, becomes active replica                             │  │
│  │  - Cluster returns to R replication factor                             │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Tradeoff Analysis

### 11.1 Major Design Decisions

| Decision | Choice | Alternatives Considered | Rationale |
|----------|--------|------------------------|-----------|
| **Data Partitioning** | Consistent hashing with 16384 fixed slots (Redis Cluster model) | Range-based partitioning, random hashing, directory-based | Fixed slots enable O(1) routing without lookups. 16384 slots provides fine granularity for rebalancing. |
| **Replication** | Async replication with configurable R factor | Sync replication, Semi-sync, Quorum writes | Async maximizes write throughput (no waiting for replicas). Tradeoff: potential data loss on primary failure before replication completes. |
| **Failure Detection** | Phi-accrual adaptive detector | Fixed-timeout heartbeat, SWIM protocol, failure detectors with fixed threshold | Phi-accrual adapts to network conditions. False positive rate self-tunes based on observed latency distribution. |
| **Leader Election** | Simplified Raft (no log replication) | Full Raft, Paxos, Bully algorithm, leaderless (gossip-based) | Raft gives strong leader semantics for coordination. Skipping log replication reduces overhead since data is in-memory and replicated via gossip. |
| **Consistency Model** | Eventual consistency with read-repair | Strong consistency (linearizable), Causal consistency, Session consistency | Cache workloads tolerate staleness. Eventual consistency maximizes availability and throughput. |
| **Eviction Policy** | LRU with configurable maxmemory | LFU, Random, TTL-only, ARC, LIRS | LRU is well-understood, good performance, low overhead. LFU can be added as an option. |
| **Memory Management** | Pre-allocated memory pools with jemalloc | Go GC default, mmap-based allocator, arena allocator | jemalloc reduces GC pressure, provides memory profiling, supports defragmentation. |
| **Wire Protocol** | Redis RESP protocol compatibility | Custom binary protocol, gRPC, HTTP/2 | RESP compatibility enables use of existing Redis clients (redis-py, go-redis, Jedis). Massive ecosystem benefit. |
| **Inter-node Communication** | TCP for replication, UDP for gossip | TCP-only, QUIC, multicast | Gossip is fire-and-forget; UDP avoids head-of-line blocking. Replication needs reliability; TCP ensures delivery. |
| **Cluster Topology** | Shared-nothing per-shard | Shared storage (Redis on shared disk), proxy-based sharding | Shared-nothing scales linearly. Each node is independent. No single point of failure. |
| **Configuration** | Embedded Raft for cluster config | External store (etcd, ZooKeeper), gossip-only | Eliminates external dependency. Raft ensures consistent view of cluster topology across all nodes. |
| **Cache Eviction Trigger** | Background sweeper + on-access lazy expiry | Pure lazy expiry, Active expiration thread, No eviction (OOM) | Hybrid approach: lazy expiry catches most cases quickly. Background sweeper handles bulk expiry and memory pressure. |
| **Key Serialization** | msgpack for replication payloads | JSON, Protobuf, gob, custom binary | msgpack is compact, fast, and language-agnostic. Good balance of size and encoding speed. |
| **Network I/O Model** | Goroutine-per-connection (Go runtime) | Event loop (epoll/kqueue), io_uring, actor model | Go's goroutine scheduler handles thousands of connections efficiently. Simpler code than manual event loops. |
| **Slot Migration** | Blocking migration with ASK redirection | Live migration with forwarding, Double-write during migration | Blocking migration is simpler to reason about. ASK redirection keeps the system available during migration. |

### 11.2 Detailed Tradeoff: Sync vs Async Replication

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     SYNC vs ASYNC REPLICATION                                  │
│                                                                                │
│  ASYNC (chosen):                                                               │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  + Write latency ≈ local memory latency (< 1ms)                       │  │
│  │  + Write throughput limited only by local disk (or memory) speed       │  │
│  │  + Replicas can lag without blocking writes                           │  │
│  │  - Up to repl_backlog_size bytes of data loss on primary failure      │  │
│  │  - Replicas may serve stale reads                                     │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  SYNC (rejected):                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  + Zero data loss on primary failure                                  │  │
│  │  + Replicas always up-to-date                                         │  │
│  │  - Write latency = local + network RTT to R replicas (1-5ms)         │  │
│  │  - Write throughput bottlenecked by slowest replica                    │  │
│  │  - System unavailable if any replica is down                          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  VERDICT: For a cache (not a database), async replication is the correct      │
│  choice. Data loss of a few hundred ms of writes is acceptable because        │
│  the data source of truth is the database. The performance benefit of         │
│  async replication is substantial for cache workloads.                        │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 12. Failure Modes

### 12.1 Comprehensive Failure Matrix

| # | Failure Scenario | Detection Method | Recovery Strategy | Data Loss | Downtime |
|---|-----------------|------------------|-------------------|-----------|----------|
| 1 | **Single node crash** | Phi-accrual detector (2-5s) | Automatic failover: promote replica | 0-100ms of unreplicated writes | 2-5s |
| 2 | **Network partition (node isolated)** | Gossip stops, phi increases | Partition: majority side promotes, minority side marks partitioned node as dead | 0-100ms on minority side | 2-5s |
| 3 | **Network partition (split brain)** | Split-brain detection via leader quorum | Leader election resolves: only majority partition has leader | Possible divergent writes on minority | 5-10s |
| 4 | **Disk full (if WAL enabled)** | Disk space monitoring | WAL disabled, fallback to in-memory only. Alert raised. | None (WAL is optional) | None |
| 5 | **OOM (out of memory)** | Memory usage monitoring | LRU eviction kicks in. If maxmemory reached, reject writes with OOM error. | None | None (eviction) |
| 6 | **Replication lag too high** | Lag tracker | If lag > backlog_size, trigger full resync. During resync, replica serves stale data. | None (full resync catches up) | None (read-only) |
| 7 | **Leader election failure** | Election timeout | Retry with randomized backoff. Max retries: 5. After max retries, node stays follower. | None (data writes don't need leader) | 5-15s |
| 8 | **Slot migration failure** | Migration progress monitoring | Abort migration. Return slot to MIGRATING state. Retry with different target node. | None (migration is atomic per slot) | 1-3s |
| 9 | **Gossip protocol partition** | Vector clock divergence | Anti-entropy (Merkle tree comparison) resolves conflicts. | None (last-write-wins) | None |
| 10 | **Client connection storm** | Connection rate monitoring | Circuit breaker on proxy. Rate limit new connections. Backpressure to clients. | None | Degraded (rate limited) |
| 11 | **Rapid node churn (flapping)** | Node join/leave frequency | Debounce: ignore node state changes within 30s of last change. Prevents thrashing. | None | None |
| 12 | **Clock skew** | NTP monitoring, timestamp validation | Use monotonic clocks for internal operations. Wall clock only for TTL. | Possible premature/expired TTL | None |
| 13 | **Corrupted in-memory state** | Checksum validation on replication | Replica requests full resync from primary. | None (primary has clean copy) | None |
| 14 | **All replicas down** | Quorum check | Primary continues serving reads. Writes succeed but are unreplicated. Alert critical. | Writes unreplicated until replica recovers | Read-only degraded |
| 15 | **Majority of nodes down** | Quorum check | Cluster enters read-only mode. No writes accepted. | Writes since last checkpoint lost | Full outage |
| 16 | **Network latency spike** | RTT monitoring, phi-accrual adapts | Phi-accrual detector adjusts threshold. Longer timeout before declaring dead. | None | None (adaptive) |
| 17 | **CPU saturation** | CPU usage monitoring | Backpressure to clients. Reject new commands with LOADING error. | None | Degraded throughput |

---

## 13. Consistency Model

### 13.1 Eventual Consistency Guarantees

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     CONSISTENCY MODEL                                          │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  GUARANTEES                                                              │  │
│  │                                                                         │  │
│  │  1. EVENTUAL CONSISTENCY                                                │  │
│  │     If no new writes are made, all replicas will eventually            │  │
│  │     converge to the same value. Time to convergence:                   │  │
│  │     - Normal operation: < 1 second                                     │  │
│  │     - After failover: < 10 seconds (depends on lag)                   │  │
│  │                                                                         │  │
│  │  2. MONOTONIC READS (within session)                                   │  │
│  │     A client that reads key K will never see a value older            │  │
│  │     than what it previously read for K (via read-repair).            │  │
│  │                                                                         │  │
│  │  3. READ-YOUR-WRITES (within same node)                               │  │
│  │     A write to key K followed by a read from the same node            │  │
│  │     will return the written value.                                     │  │
│  │                                                                         │  │
│  │  4. NO BIZARRE INVARIANTS                                              │  │
│  │     Values don't "go backwards" — if K=5 at time T, it will           │  │
│  │     never be K=3 at time T+1 (assuming CAS operations).              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  NOT GUARANTEED                                                          │  │
│  │                                                                         │  │
│  │  - Linearizability (strong consistency)                                │  │
│  │  - Sequential consistency across nodes                                 │  │
│  │  - Consistent prefix reads (reading from replicas may skip writes)    │  │
│  │  - Distributed transactions                                             │  │
│  │  - Compare-and-swap across nodes (only atomic on single node)         │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  CONFLICT RESOLUTION                                                     │  │
│  │                                                                         │  │
│  │  When two concurrent writes to the same key arrive at different        │  │
│  │  replicas before replication completes:                                │  │
│  │                                                                         │  │
│  │  Resolution strategy: LAST-WRITE-WINS (LWW)                           │  │
│  │  - Each write is timestamped (hybrid logical clock)                   │  │
│  │  - On conflict, the write with the later timestamp wins               │  │
│  │  - Ties broken by nodeID comparison (deterministic)                   │  │
│  │                                                                         │  │
│  │  This is the same strategy used by Cassandra and DynamoDB.            │  │
│  │  For a cache, LWW is acceptable because:                              │  │
│  │  - Cache data is derived from a database (source of truth)           │  │
│  │  - Concurrent writes to the same key are rare                        │  │
│  │  - Slightly stale data is acceptable for cache workloads             │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  CONSISTENCY WINDOW                                                      │  │
│  │                                                                         │  │
│  │  The "consistency window" is the maximum time a read from a replica   │  │
│  │  may be stale. This equals the maximum replication lag:               │  │
│  │                                                                         │  │
│  │  consistency_window = max(repl_lag across all replicas)               │  │
│  │                                                                         │  │
│  │  Typical values:                                                       │  │
│  │  - Healthy cluster: < 100ms                                           │  │
│  │  - Under load: < 1 second                                              │  │
│  │  - After failover: < 5 seconds (until replica catches up)            │  │
│  │                                                                         │  │
│  │  The consistency window is bounded by:                                │  │
│  │  repl_backlog_size / write_throughput                                  │  │
│  │  For default 1MB backlog at 100MB/s write rate: 10ms window          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 14. Memory Model

### 14.1 Memory Architecture

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     MEMORY MANAGEMENT                                          │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  MEMORY LAYOUT (per node)                                               │  │
│  │                                                                         │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    jemalloc allocator                            │   │  │
│  │  │                                                                  │   │  │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │   │  │
│  │  │  │  Hash Table   │  │  LRU List    │  │  Expire Heap         │   │   │  │
│  │  │  │  (dict)       │  │  (doubly     │  │  (min-heap by        │   │   │  │
│  │  │  │              │  │   linked     │  │   expiry time)        │   │   │  │
│  │  │  │  Key → Value │  │   list)      │  │                      │   │   │  │
│  │  │  │  mapping     │  │              │  │  Fast lookup for      │   │   │  │
│  │  │  │              │  │  O(1) insert │  │  bulk expiry          │   │   │  │
│  │  │  │  O(1) avg    │  │  O(1) remove │  │  operations           │   │   │  │
│  │  │  │  lookup      │  │  O(1) access │  │                      │   │   │  │
│  │  │  └──────────────┘  └──────────────┘  └──────────────────────┘   │   │  │
│  │  │                                                                  │   │  │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │   │  │
│  │  │  │  Replication  │  │  Client      │  │  Metadata            │   │   │  │
│  │  │  │  Backlog      │  │  Output      │  │  (cluster state,     │   │   │  │
│  │  │  │  (ring buffer)│  │  Buffers     │  │   slot map, etc.)    │   │   │  │
│  │  │  │              │  │  (per-client)│  │                      │   │   │  │
│  │  │  │  Fixed 1MB   │  │  Dynamic     │  │  ~1KB per node       │   │   │  │
│  │  │  │  (config)    │  │  (max 1MB/   │  │                      │   │   │  │
│  │  │  │              │  │   client)    │  │                      │   │   │  │
│  │  │  └──────────────┘  └──────────────┘  └──────────────────────┘   │   │  │
│  │  └──────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  MEMORY ACCOUNTING                                                       │  │
│  │                                                                         │  │
│  │  used_memory = hash_table_size + lru_overhead + expire_heap_size       │  │
│  │               + replication_backlog + client_buffers + metadata        │  │
│  │               + jemalloc_overhead                                      │  │
│  │                                                                         │  │
│  │  maxmemory = configurable (default: 8GB)                               │  │
│  │  eviction_policy = "allkeys-lru" (default)                             │  │
│  │                                                                         │  │
│  │  Memory check on every write:                                          │  │
│  │  if used_memory + new_entry_size > maxmemory:                          │  │
│  │      evict_keysUntilFree(new_entry_size)                               │  │
│  │      if still not enough: REJECT with OOM error                        │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  EVICTION STRATEGIES                                                    │  │
│  │                                                                         │  │
│  │  ┌─────────────────┬──────────────────────────────────────────────────┐ │  │
│  │  │ Strategy        │ Description                                      │ │  │
│  │  │─────────────────┼──────────────────────────────────────────────────│ │  │
│  │  │ allkeys-lru     │ Evict least recently used key from entire       │ │  │
│  │  │                 │ keyspace. Best for general-purpose caching.     │ │  │
│  │  │─────────────────┼──────────────────────────────────────────────────│ │  │
│  │  │ volatile-lru    │ Evict LRU key that has an expiry set.           │ │  │
│  │  │                 │ Keys without TTL are never evicted.             │ │  │
│  │  │─────────────────┼──────────────────────────────────────────────────│ │  │
│  │  │ allkeys-random  │ Evict a random key. Fast, no LRU tracking.     │ │  │
│  │  │─────────────────┼──────────────────────────────────────────────────│ │  │
│  │  │ volatile-ttl    │ Evict key with shortest remaining TTL.          │ │  │
│  │  │                 │ Good for time-series data.                      │ │  │
│  │  │─────────────────┼──────────────────────────────────────────────────│ │  │
│  │  │ noeviction      │ Return OOM error when memory is full.           │ │  │
│  │  │                 │ Use when eviction is unacceptable.              │ │  │
│  │  └─────────────────┴──────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  MEMORY OPTIMIZATIONS                                                   │  │
│  │                                                                         │  │
│  │  1. SHARDED LRU: Divide LRU list into N shards. Eviction scans a     │  │
│  │     random shard instead of the full list. Reduces lock contention.   │  │
│  │                                                                         │  │
│  │  2. LAZY DELETION: DEL marks keys as deleted (tombstone). Actual     │  │
│  │     memory reclaim happens in background compaction cycle.            │  │
│  │                                                                         │  │
│  │  3. STRING INTEGER CACHE: Small integers (0-9999) are interned.      │  │
│  │     Saves memory for counter/histogram workloads.                     │  │
│  │                                                                         │  │
│  │  4. COMPACT VALUES: Values < 64 bytes use a compact encoding that   │  │
│  │     avoids pointer overhead (similar to Redis ziplist).              │  │
│  │                                                                         │  │
│  │  5. jemalloc THP: Transparent huge pages disabled to avoid           │  │
│  │     latency spikes from THP compaction.                              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 15. Wire Protocol

### 15.1 Redis RESP Protocol Compatibility

HydraCache implements the **Redis Serialization Protocol (RESP)** for client compatibility.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     RESP PROTOCOL                                              │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  RESP DATA TYPES                                                         │  │
│  │                                                                         │  │
│  │  Simple Strings: +OK\r\n                                               │  │
│  │  Errors:         -ERR unknown command\r\n                              │  │
│  │  Integers:       :1000\r\n                                             │  │
│  │  Bulk Strings:   $6\r\nfoobar\r\n                                      │  │
│  │  Arrays:         *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n                     │  │
│  │  Null:           $-1\r\n                                               │  │
│  │  Null Array:     *-1\r\n                                               │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  CLUSTER-SPECIFIC RESPONSES                                             │  │
│  │                                                                         │  │
│  │  MOVED <slot> <host>:<port>                                            │  │
│  │  Returned when a command targets a slot owned by another node.         │  │
│  │  Client should reconnect to the specified node and retry.             │  │
│  │                                                                         │  │
│  │  ASK <slot> <host>:<port>                                              │  │
│  │  Returned during slot migration. Client should send ASKING command    │  │
│  │  first, then retry on the target node.                                │  │
│  │                                                                         │  │
│  │  Examples:                                                             │  │
│  │  -MOVED 3999 127.0.0.1:7002                                           │  │
│  │  -ASK 3999 127.0.0.1:7003                                             │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  COMMAND PARSING                                                         │  │
│  │                                                                         │  │
│  │  Client sends:                                                         │  │
│  │  *3\r\n                                                                │  │
│  │  $3\r\n                                                                │  │
│  │  SET\r\n                                                               │  │
│  │  $5\r\n                                                                │  │
│  │  mykey\r\n                                                             │  │
│  │  $7\r\n                                                                │  │
│  │  myvalue\r\n                                                           │  │
│  │                                                                         │  │
│  │  HydraCache processes:                                                 │  │
│  │  1. Parse RESP array → [SET, mykey, myvalue]                          │  │
│  │  2. Command router: SET → write command                               │  │
│  │  3. Slot lookup: CRC16("mykey") % 16384 = slot 3999                  │  │
│  │  4. Route to slot owner                                               │  │
│  │  5. Execute SET on local store                                         │  │
│  │  6. Return: +OK\r\n                                                   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  CLUSTER BUS PROTOCOL (internal)                                        │  │
│  │                                                                         │  │
│  │  For inter-node communication, HydraCache uses a binary protocol       │  │
│  │  (not RESP) for efficiency:                                            │  │
│  │                                                                         │  │
│  │  ┌────────┬────────┬────────┬──────────┬────────────────┐              │  │
│  │  │ Magic  │ Version│ Type   │ Length   │ Payload        │              │  │
│  │  │ 2 bytes│ 1 byte │ 1 byte │ 4 bytes  │ N bytes        │              │  │
│  │  └────────┴────────┴────────┴──────────┴────────────────┘              │  │
│  │                                                                         │  │
│  │  Message Types:                                                        │  │
│  │  0x01 = PING (heartbeat)                                              │  │
│  │  0x02 = PONG (heartbeat response)                                     │  │
│  │  0x03 = MEET (node join request)                                      │  │
│  │  0x04 = FAIL (node failure announcement)                              │  │
│  │  0x05 = SLOTMAP (slot ownership update)                               │  │
│  │  0x06 = REPLICATE (full/partial resync request)                       │  │
│  │  0x07 = DATA (replication data transfer)                              │  │
│  │  0x08 = VOTE (leader election)                                        │  │
│  │  0x09 = APPEND (leader heartbeat / log entry)                         │  │
│  │  0x0A = GOSSIP (gossip message)                                       │  │
│  │  0x0B = MIGRATE (slot migration control)                              │  │
│  │  0x0C = INFO (node info request/response)                             │  │
│  │                                                                         │  │
│  │  Port assignments:                                                     │  │
│  │  Client port: 6379 (standard Redis port)                              │  │
│  │  Cluster bus: 16379 (client_port + 10000, same as Redis Cluster)      │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  SUPPORTED COMMANDS (subset)                                             │  │
│  │                                                                         │  │
│  │  String:  GET, SET, SETNX, SETEX, MGET, MSET, INCR, DECR, APPEND     │  │
│  │  Key:     DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, TYPE, KEYS        │  │
│  │  Server:  PING, INFO, DBSIZE, FLUSHDB, FLUSHALL                      │  │
│  │  Cluster: CLUSTER INFO, CLUSTER NODES, CLUSTER SLOTS                 │  │
│  │           CLUSTER MYID, CLUSTER KEYSLOT, CLUSTER MEET                │  │
│  │           CLUSTER FORGET, CLUSTER REPLICATE, CLUSTER RESET           │  │
│  │  Admin:   MIGRATE (for slot transfer), ASKING (during migration)     │  │
│  │                                                                         │  │
│  │  Non-cluster commands that operate on single keys work normally.      │  │
│  │  Multi-key commands (SORT, SUNION, etc.) require all keys in same    │  │
│  │  slot (use hash tags: {tag}key forces slot = CRC16(tag) % 16384).   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Appendix A: Configuration Reference

```yaml
# hydracache.conf
server:
  port: 6379
  bind: 0.0.0.0
  maxclients: 10000
  timeout: 0          # client idle timeout (0 = no timeout)

cluster:
  enabled: true
  port: 16379          # cluster bus port (server.port + 10000)
  require-full-coverage: false
  migration-timeout: 15000  # ms to wait for slot migration

replication:
  repl-backlog-size: 1048576    # 1MB
  repl-backlog-ttl: 3600        # seconds to release backlog if no replicas
  min-replicas-to-write: 0      # 0 = writes allowed even with no replicas
  min-replicas-max-lag: 10      # seconds

failure-detection:
  heartbeat-interval: 1000      # ms between gossip ticks
  phi-threshold: 4.0            # phi value to declare node dead
  phi-window-size: 100          # number of samples for phi calculation

leader-election:
  election-timeout-min: 150     # ms
  election-timeout-max: 300     # ms
  heartbeat-interval: 100       # ms

memory:
  maxmemory: 8589934592         # 8GB in bytes
  maxmemory-policy: allkeys-lru
  maxmemory-samples: 5          # LRU samples per eviction
  jemalloc-bg-thread: true      # background jemalloc thread

migration:
  batch-size: 100               # keys per MIGRATE batch
  pipeline-size: 10             # concurrent MIGRATE pipelines
  blocking-timeout: 5000        # ms to block client during migration
```

## Appendix B: Metrics Reference

```
# Prometheus metrics exposed at /metrics

# Cluster
hydracache_cluster_nodes_total          gauge    Total nodes in cluster
hydracache_cluster_slots_assigned       gauge    Slots assigned to this node
hydracache_cluster_slots_migrating      gauge    Slots in MIGRATING state
hydracache_cluster_slots_importing      gauge    Slots in IMPORTING state

# Replication
hydracache_replication_offset           gauge    Current replication offset
hydracache_replication_lag_bytes        gauge    Bytes behind primary
hydracache_replication_lag_seconds      gauge    Estimated seconds behind
hydracache_replication_full_resyncs     counter  Total full resyncs

# Failure Detection
hydracache_failure_detector_phi         gauge    Current phi value per node
hydracache_failure_detector_suspected   gauge    Number of suspected nodes

# Leader Election
hydracache_leader_term                  gauge    Current Raft term
hydracache_leader_is_leader             gauge    1 if this node is leader

# Memory
hydracache_memory_used_bytes            gauge    Current memory usage
hydracache_memory_max_bytes             gauge    Maximum memory limit
hydracache_memory_evicted_keys          counter  Total keys evicted

# Operations
hydracache_ops_total{cmd,set,get,del}  counter  Total operations
hydracache_ops_latency_seconds{p50,p99}histogram Operation latency
hydracache_connections_total            gauge    Current connections
hydracache_keys_total                   gauge    Total keys in local store
hydracache_keyspace_hits                counter  Cache hits
hydracache_keyspace_misses              counter  Cache misses
```

---

*End of Architecture Design Document*
