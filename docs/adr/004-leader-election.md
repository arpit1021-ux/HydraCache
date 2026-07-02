# ADR-004: Simplified Raft for Leader Election

## Status

Accepted

## Context

The cluster needs a leader for coordinating topology changes, rebalancing, and other cluster-wide operations. Full Raft consensus provides linearizability but adds significant complexity.

## Decision

We will implement a simplified Raft-inspired leader election without log replication.

## Consequences

### Positive
- Provides leader uniqueness per term (prevents split-brain)
- Quorum-based (survives minority failures)
- Term numbers prevent stale leaders from acting
- ~700 lines vs ~2500 for full Raft

### Negative
- No linearizable reads/writes through leader
- Data operations remain eventually consistent (AP)
- Cannot use leader for write ordering

### What We Keep from Raft
- Term numbers
- Vote requests with "vote for me if your term is lower"
- Heartbeat from leader to reset follower election timers
- Randomized election timeout to prevent split votes

### What We Omit
- Log replication
- Log compaction
- Membership changes through Raft
- Safety proofs

### Alternatives Considered
- Full Raft: Complete consensus, but 3.5x more code and risk of subtle bugs
- Bully algorithm: Simpler, but O(N²) message complexity
- Paxos: More complex than Raft for little benefit
