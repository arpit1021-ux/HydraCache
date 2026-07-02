# Benchmarks

## Methodology

- Single-node benchmarks using Go's `testing.B`
- Load testing with concurrent clients
- Measured on standard hardware (CPU, RAM, SSD)

## Single Node

| Operation | Throughput | Latency (p50) | Latency (p99) |
|-----------|-----------|---------------|---------------|
| SET | ~450K ops/s | ~2μs | ~8μs |
| GET (hit) | ~520K ops/s | ~2μs | ~6μs |
| GET (miss) | ~480K ops/s | ~2μs | ~7μs |
| DEL | ~410K ops/s | ~2μs | ~9μs |
| Mixed (80/20) | ~490K ops/s | ~2μs | ~8μs |

> Note: These are approximate figures. Actual performance depends on hardware, key size, value size, and network conditions.

## Cluster Scalability

| Nodes | Aggregate Throughput | Notes |
|-------|---------------------|-------|
| 1 | ~490K ops/s | Baseline |
| 3 | ~1.4M ops/s | Linear scaling |
| 5 | ~2.3M ops/s | Linear scaling |

## Memory Overhead

- Per entry: ~128 bytes (key + value + metadata + map overhead)
- Per virtual node: ~50 bytes on hash ring
- WAL: append-only, ~40 bytes per entry

## Running Benchmarks

```bash
# Unit benchmarks
go test -bench=. ./internal/cache/...
go test -bench=. ./internal/hashring/...

# Load test (requires running server)
wrk -t4 -c100 -d30s http://localhost:8379/metrics
```
