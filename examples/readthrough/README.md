# HydraCache reference application: read-through cache over Postgres

A small product-catalog HTTP API that uses HydraCache as a cache-aside
(read-through) layer in front of a real Postgres database, with an
optional live "kill a node" control for demonstrating that the app keeps
serving correct data — just slower — while HydraCache is degraded.

This is a demo of the pattern, not a template for a production
deployment. It's a separate Go module (its own `go.mod`) so a Postgres
driver and go-redis don't end up as dependencies of the core HydraCache
engine, which needs neither.

## What this actually proves, and what it doesn't

- **Proves**: a real HTTP service can use HydraCache exactly as it would
  use Redis (via `go-redis/v9`, the same client library and the same
  RESP2-only connection settings documented in
  [COMMANDS.md](../../COMMANDS.md)) to build a working cache-aside layer,
  and that layer degrades to the database — not to an error — when the
  cache is unreachable. `service_test.go` proves this with a fake store
  and a fake in-memory Redis client, no live cluster required.
- **Does not prove**: multi-node client-side routing or failover across
  the HydraCache cluster. This demo's cache client connects to **one**
  HydraCache node address (`-cache-addr`, default `localhost:7379`), the
  same way `cmd/cli` does. If you kill that specific node, the demo falls
  back to Postgres for every read until you revive it or point
  `-cache-addr` at a different node — it does not automatically retry
  other nodes in the cluster. That's an honest limitation of this demo
  client, not a claim about HydraCache itself.

## Running it

1. Start Postgres and the HydraCache cluster (from the repo root):

   ```bash
   docker compose -f deploy/docker-compose.yml up -d postgres cache-node-1 cache-node-2 cache-node-3 cache-node-4 cache-node-5
   ```

   `postgres` is seeded automatically from [schema.sql](schema.sql) on
   first startup.

2. Run the demo app on the host (see "Why not containerize this" below):

   ```bash
   cd examples/readthrough
   go run . -cache-addr localhost:7379 -postgres-dsn "postgres://postgres:postgres@localhost:5432/readthrough?sslmode=disable"
   ```

3. Open <http://localhost:9000> for the demo UI, or drive it directly:

   ```bash
   curl http://localhost:9000/products/1          # first read: source=database
   curl http://localhost:9000/products/1          # second read: source=cache
   ```

## Live kill-a-node control

Start the app with `-enable-chaos-controls` to expose:

- `POST /admin/nodes/{id}/kill` — runs `docker stop hydracache-node-{id}`
- `POST /admin/nodes/{id}/revive` — runs `docker start hydracache-node-{id}`
- `GET /admin/nodes` — container status for each configured node

This flag is **off by default** and must never be turned on outside a
local demo — it gives any caller of this HTTP API the ability to run
`docker stop`/`docker start` on named containers. It reuses the exact two
commands `internal/chaostest`'s harness already runs against the same
container names (see `chaos.go`); nothing new is being trusted here that
wasn't already load-bearing in this repo's own chaos-testing tool.

Kill the node the demo is actually connected to (`-cache-addr`) and watch
`source` in the UI switch from `cache` to `database` on every read —
that's the whole point of the demo. Killing a *different* node
demonstrates HydraCache's own cluster self-healing instead, which is
better observed through the existing dashboard / `GET /api/cluster` than
through this app.

## Why not containerize this

`ChaosController` shells out to the `docker` CLI on whatever host the
demo process runs on. Running the demo inside its own container would
mean either mounting the Docker socket into that container (a real
privilege-escalation surface, not something to normalize for a demo) or
building a second, different chaos mechanism just for the containerized
case. Running it directly on the host — where `docker` and the socket
are already there — avoids both. A `Dockerfile` is included for the
non-chaos-enabled case (e.g. deploying just the read-through API without
live node control), but it is not wired into `deploy/docker-compose.yml`.

## Testing

```bash
go test -race ./...
```

Everything in `service_test.go` runs against a fake `Store` and a fake
`redisCommands` implementation — no live Postgres or HydraCache required.
The wiring to a real go-redis client against a real HydraCache server is
proven separately, in the main module's
`internal/network/goredis_compat_test.go`. The wiring to real Postgres
(`PostgresStore` in `store.go`) is exercised by running the app for real
against the `postgres` compose service above; it is not covered by an
automated test in this repo.
