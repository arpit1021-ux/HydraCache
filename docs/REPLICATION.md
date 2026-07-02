# Replication

## Overview

HydraCache uses asynchronous replication. The primary applies writes immediately and replicates to followers in the background.

## Write Path

```
Client → Primary: SET key val
Primary: 1. Apply to local cache
         2. Append to replication stream
         3. Return OK to client
         4. Async: send to each replica
```

## Replication Stream

- Bounded ring buffer (capacity: 10,000 operations)
- Each entry: `{seq, cmd, args, timestamp}`
- New replicas catch up by replaying from buffer
- When buffer is full, oldest entries are dropped

## Lag Tracking

Replication lag = `primary_seq - replica_seq`

- Tracked per replica
- Exposed as Prometheus metric
- If lag > 100, replica is marked as "lagging"
- Dashboard shows real-time lag per node

## Replica Promotion

When the primary fails:

1. Heartbeat detector marks primary as dead
2. Select replica with highest replication sequence
3. Promote to primary
4. Update hash ring
5. Redirect client requests

## Configuration

- **Replication factor**: Default 2 (1 primary + 1 replica)
- **Buffer size**: 10,000 operations
- **Sync mode**: Async (default), Sync (optional)

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| Primary dies | Best replica promoted |
| Replica dies | Replaced from remaining replicas |
| Network partition | Phi-accrual detects, promotes available replica |
| All replicas die | Primary continues (data loss on next primary failure) |
