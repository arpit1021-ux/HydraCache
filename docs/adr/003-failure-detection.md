# ADR-003: Phi-Accrual Failure Detection

## Status

Accepted

## Context

The cluster needs to detect node failures. Fixed timeout approaches (e.g., "3 missed heartbeats = dead") are fragile under variable load.

## Decision

We will implement phi-accrual failure detection (inspired by Cassandra).

## Consequences

### Positive
- Adapts to network conditions — threshold adjusts based on heartbeat history
- Reduces false positives during high load periods
- Provides a suspicion level (0-∞) instead of binary alive/dead
- Production-proven by Cassandra, Amazon, and other distributed systems

### Negative
- More complex than fixed timeout
- Requires tuning the phi threshold (default: 8.0)
- Needs sufficient heartbeat samples for accurate statistics

### How It Works
1. Track heartbeat arrival intervals
2. Compute mean and standard deviation of intervals
3. Given elapsed time since last heartbeat, compute phi = -log10(P_later)
4. When phi > threshold, mark as suspect
5. After suspect duration expires, declare dead

### Alternatives Considered
- Fixed timeout: Simpler, but fragile under load
- SWIM: More complex, gossip-based failure detection
- Heartbeat with ACK: Requires bidirectional communication
