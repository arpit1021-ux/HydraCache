# Persistence

## Overview

HydraCache provides durability through a Write-Ahead Log (WAL) and periodic snapshots.

## Write-Ahead Log (WAL)

### Structure

Every write is appended to the WAL before applying to the in-memory cache:

```
[4-byte CRC32][4-byte length][entry data]
```

Entry data:
```
[8-byte seq][1-byte cmd_len][cmd][4-byte key_len][key][4-byte val_len][val][8-byte ttl][8-byte timestamp]
```

### Sync Modes

| Mode | Behavior | Durability | Performance |
|------|----------|-----------|-------------|
| EveryWrite | fsync after each write | Maximum | Slowest |
| Batch | fsync every 100 writes | Good | Balanced |
| Async | fsync periodically | Lower | Fastest |

### Crash Recovery

1. On startup, load latest snapshot
2. Replay WAL entries with sequence > snapshot sequence
3. Validate CRC32 on each entry
4. Skip corrupted entries (don't crash on single bad entry)

## Snapshots

### Structure

```json
{
  "entries": {"key1": {"key":"key1", "value":"...", "expires_at":...}},
  "seq": 42,
  "timestamp": "2024-01-01T00:00:00Z",
  "node_id": "abc123"
}
```

### Atomic Writes

1. Write to temporary file
2. `os.Rename` to final path (atomic on POSIX)
3. No partial snapshots

### Configuration

```yaml
wal:
  enabled: true
  dir: ./data/wal
  max_size: 104857600  # 100MB
  sync_mode: batch
  snapshot:
    enabled: true
    interval: 60s
```

## Tradeoffs

| Choice | Pros | Cons |
|--------|------|------|
| WAL enabled | Crash recovery, durability | Disk I/O on every write |
| WAL disabled | Maximum performance | Data loss on crash |
| Batch sync | Balanced | May lose up to 100 writes |
| Frequent snapshots | Faster recovery | More disk I/O |
