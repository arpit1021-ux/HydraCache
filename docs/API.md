# API Reference

## TCP Protocol (RESP)

HydraCache implements the Redis RESP protocol. Connect with any Redis client.

### Connection

```
telnet localhost 7379
redis-cli -p 7379
```

### Commands

#### PING

```
PING
+PONG

PING hello
+hello
```

#### SET

```
SET key value
+OK

SET key value EX 3600
+OK

SET key value PX 1000
+OK
```

#### GET

```
GET key
$5
hello

GET nonexistent
$-1
```

#### DEL

```
DEL key
:(integer) 1

DEL key1 key2 key3
:(integer) 2
```

#### EXISTS

```
EXISTS key
:(integer) 1

EXISTS nonexistent
:(integer) 0
```

#### TTL

```
TTL key
:(integer) -1    (no expiry)

TTL key
:(integer) 3598  (seconds remaining)

TTL nonexistent
:(integer) -2    (key doesn't exist)
```

#### INFO

```
INFO
$47
keys:100
hits:5000
misses:200
hit_rate:0.9615
```

#### DBSIZE

```
DBSIZE
:(integer) 100
```

#### FLUSHALL

```
FLUSHALL
+OK
```

#### KEYS

```
KEYS *
*3
$4
user
$5
order
$7
session

KEYS user:*
*1
$9
user:123
```

## HTTP API

### Health Check

```
GET /health
200 OK
```

### Cluster Info

```
GET /api/cluster
Content-Type: application/json

{
  "nodes": [...],
  "epoch": 42,
  "updated_at": "2024-01-01T00:00:00Z"
}
```

### Cache Stats

```
GET /api/stats
Content-Type: application/json

{
  "keys": 100,
  "hits": 5000,
  "misses": 200,
  "hit_rate": 0.9615
}
```

### Prometheus Metrics

```
GET /metrics
Content-Type: text/plain

# HELP hydracache_requests_total Total requests
# TYPE hydracache_requests_total counter
hydracache_requests_total 5200

# HELP hydracache_hits_total Cache hits
# TYPE hydracache_hits_total counter
hydracache_hits_total 5000
```
