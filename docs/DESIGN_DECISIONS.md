# Design Decisions

## Protocol: RESP (Redis)

**Decision**: Use Redis RESP protocol instead of custom binary or gRPC.

**Why**: Every existing Redis client works out of the box. Battle-tested by millions of production systems. Telnet debugging works immediately.

**Tradeoff**: Limited to Redis-style command model. No built-in support for complex types.

## Replication: Async

**Decision**: Async replication by default, with optional sync mode.

**Why**: Client receives response immediately after primary applies write. Write latency = local memory write (~2μs) instead of round-trip to replicas (~1ms+).

**Tradeoff**: On primary failure, recent writes (within replication lag window) may be lost. This is the standard AP choice.

## Failure Detection: Phi-Accrual

**Decision**: Phi-accrual failure detection instead of fixed timeouts.

**Why**: Fixed timeouts fail under load. Phi-accrual adapts to network conditions by computing suspicion level from heartbeat history.

**Tradeoff**: More complex than fixed timeout. Requires tuning the phi threshold (default: 8.0).

## Consensus: Simplified Raft

**Decision**: Simplified Raft for leader election without log replication.

**Why**: Provides leader uniqueness per term (prevents split-brain), quorum-based, ~700 lines vs ~2500 for full Raft.

**Tradeoff**: No linearizable reads/writes through leader. Data operations remain eventually consistent.

## Eviction: LRU + LFU

**Decision**: LRU and LFU eviction policies, configurable per instance.

**Why**: LRU is simplest, LFU better for skewed access. Both are well-understood and sufficient for most workloads.

**Tradeoff**: More sophisticated policies (ARC, W-TinyLFU) exist but add complexity without proportional benefit.

## Storage: In-Memory Map

**Decision**: Go map with RWMutex instead of skip list or B-tree.

**Why**: O(1) operations, simple implementation, RWMutex allows concurrent reads.

**Tradeoff**: Global lock contention at very high throughput. Migration path to sharded maps documented.

## Hashing: FNV-1a

**Decision**: FNV-1a hash function for consistent hashing ring.

**Why**: Fast with excellent distribution. Simpler than MurmurHash3, sufficient for our use case.

**Tradeoff**: Not cryptographically secure (not needed for cache).

## Virtual Nodes: 150

**Decision**: 150 virtual nodes per physical node.

**Why**: Gives <5% standard deviation in key distribution. Redis uses 16384 slots because it needs slot-based routing for multi-key operations. We don't have that constraint.

**Tradeoff**: More virtual nodes = better distribution but more memory. 150 is the sweet spot.
