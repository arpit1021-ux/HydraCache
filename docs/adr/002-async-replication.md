# ADR-002: Async Replication Over Sync Replication

## Status

Accepted

## Context

Replication ensures data durability when nodes fail. The choice is between synchronous and asynchronous replication.

## Decision

We will use asynchronous replication by default, with optional synchronous mode.

## Consequences

### Positive
- Client receives response immediately after primary applies write
- Write latency = local memory write (~2μs) instead of round-trip to replicas (~1ms+)
- Replication lag is bounded by network latency, not write latency

### Negative
- On primary failure, recent writes (within replication lag window) may be lost
- Eventual consistency — replicas may serve stale reads briefly

### Consistency Guarantees
- Strong consistency for single-key reads from primary
- Eventual consistency for cross-key operations
- Write durability depends on replication lag (typically <1ms in same datacenter)

### Alternatives Considered
- Sync replication: Zero data loss, but 2x+ write latency. Too slow for most cache use cases.
- Quorum writes: Balance between consistency and latency. Complex implementation.
