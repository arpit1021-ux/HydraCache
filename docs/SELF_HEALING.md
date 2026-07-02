# Self-Healing

## Overview

HydraCache detects node failures automatically and recovers without client downtime or manual intervention.

## Detection

### Gossip Protocol

- Each node sends heartbeat to random subset of peers every 100ms
- Heartbeat payload: `{nodeID, epoch, seqNum, load, memoryUsage}`
- Propagates across cluster in O(log N) rounds

### Phi-Accrual Failure Detector

Instead of binary alive/dead, computes a suspicion level:

1. Track heartbeat arrival intervals
2. Compute mean and standard deviation
3. Given elapsed time since last heartbeat, compute phi = -log10(P_later)
4. When phi > threshold (default: 8.0), mark as suspect
5. After suspect timeout (default: 5s), declare dead

## Recovery Sequence

```
t=0:  Primary node dies
t=1:  Heartbeat detector notices missing heartbeat
t=2:  Phi value rises above threshold
t=3:  Node marked as suspect
t=4:  After 5s suspect timeout, declared dead
t=5:  Best replica selected (highest replication seq)
t=6:  Replica promoted to primary
t=7:  Hash ring updated
t=8:  Client requests redirected to new primary
t=9:  (Optional) Dead node rejoins as replica
```

## Split-Brain Prevention

- Only one leader per term (Raft invariant)
- Leaders send heartbeats to reset follower election timers
- Election requires majority quorum
- Two candidates with the same term cannot both win

## Simulation

Use the built-in simulator to test self-healing:

```go
sim := simulator.New()
sim.KillNode("node-1")      // Kill a node
sim.AddDelay("node-2", 5s)  // Add network delay
sim.Chaos(30s, 5s)          // Random failures for 30s
```

## Dashboard Visualization

The React dashboard shows:
- Node health status (green/yellow/red)
- Real-time failure detection events
- Replica promotion timeline
- Recovery animation
