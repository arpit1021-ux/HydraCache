# ADR-001: Use RESP Protocol Instead of Custom Protocol

## Status

Accepted

## Context

HydraCache needs a wire protocol for client-server communication. The options are:
1. Implement a custom binary protocol
2. Use Redis RESP (REdis Serialization Protocol)

## Decision

We will implement RESP protocol compatibility.

## Consequences

### Positive
- Every existing Redis client (redis-cli, Jedis, go-redis, ioredis) works out of the box
- Well-documented specification
- Battle-tested by millions of production systems
- Telnet/netcat debugging works immediately

### Negative
- RESP has some quirks (inline commands, CRLF termination)
- Limited to Redis-style command model
- No built-in support for complex types (但我们用 JSON 解决)

### Alternatives Considered
- gRPC: Strongly typed, but adds protobuf dependency and complexity
- Custom binary: More efficient, but zero ecosystem compatibility
- HTTP: Universally compatible, but verbose for cache operations
