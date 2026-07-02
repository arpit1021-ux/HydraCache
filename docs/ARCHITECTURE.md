# Architecture

## System Overview

HydraCache is a distributed in-memory cache that provides high availability through replication, automatic failover, and self-healing capabilities.

## Components

```
┌─────────────────────────────────────────────────────────┐
│                      Client Layer                        │
│   redis-cli / hc CLI / Application (any RESP client)    │
└──────────────────────────┬──────────────────────────────┘
                           │ RESP Protocol
┌──────────────────────────▼──────────────────────────────┐
│                      Network Layer                       │
│   TCP Server → Parser → Command Router → Handler         │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      Cache Engine                        │
│   Store (RWMutex) → Eviction (LRU/LFU) → Bloom Filter   │
│   TTL Engine (lazy + active expiration)                  │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                    Distributed Layer                     │
│   Hash Ring → Replication → Heartbeat → Election         │
│   WAL → Snapshots → Recovery                             │
└─────────────────────────────────────────────────────────┘
```

## Request Flow

### SET Command

```
Client ──SET key val──► TCP Server
                           │
                           ▼
                     Parse RESP
                           │
                           ▼
                     Route to Handler
                           │
                           ▼
                     Apply to Local Cache
                           │
                           ▼
                     Append to WAL
                           │
                           ▼
                     Async Replicate to Peers
                           │
                           ▼
                     Return +OK to Client
```

### GET Command

```
Client ──GET key──► TCP Server
                      │
                      ▼
                Parse RESP
                      │
                      ▼
                Check Local Cache
                      │
                ┌─────┴─────┐
                │ Hit        │ Miss
                ▼            ▼
          Return Value   Return $-1
```

## Data Distribution

Keys are distributed across nodes using consistent hashing:

1. Hash the key using FNV-1a → position on ring
2. Walk clockwise → first physical node owns the key
3. Continue clockwise for N-1 more distinct nodes → replicas

## Failure Recovery

```
Normal:       Node A ──► Node B ──► Node C    (all healthy)
                    │
Failure:      Node A dies (detected by phi-accrual)
                    │
Detection:    Phi value > threshold (8.0) for > 5s
                    │
Promotion:    Replica B promoted to primary (highest seq)
                    │
Recovery:     Hash ring updated, traffic redirected
                    │
Rejoin:       If Node A returns, rejoins as replica
```

## Consistency Model

HydraCache provides **eventual consistency**:
- Writes are applied to primary immediately
- Replication is asynchronous
- Reads from primary are consistent
- Reads from replicas may be stale (bounded by replication lag)

## Memory Model

- Each entry: Key (string) + Value ([]byte) + Expiry (int64) + Metadata
- RWMutex protects the store map
- Active expiration samples 10 random keys per sweep
- LRU eviction via doubly-linked list + map (O(1))

## Wire Protocol

Full RESP (REdis Serialization Protocol) compatibility:
- Simple strings: `+OK\r\n`
- Errors: `-ERR message\r\n`
- Integers: `:42\r\n`
- Bulk strings: `$5\r\nhello\r\n`
- Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

> See [API Reference](API.md) for endpoint details.
