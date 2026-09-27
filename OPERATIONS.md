# Operations Guide

A runbook for actually running HydraCache — deploying it, watching it,
changing its membership, backing it up, and diagnosing it when something
looks wrong. Everything here is checked against the real code (file paths
included); where something is a known limitation rather than a missing
doc, it's said plainly rather than glossed over — see the "Known
limitations that affect operations" section at the end before you rely on
anything here in a way its gaps would surprise you.

## Table of contents

- Deploying
- Configuration reference
- Health checks and readiness
- Monitoring
- Node lifecycle: adding, removing, restarting
- Backup and restore
- Security operations
- Troubleshooting
- Known limitations that affect operations

## Deploying

### Docker Compose (5-node cluster, for development or a small deployment)

```bash
docker compose -f deploy/docker-compose.yml up -d
```

Brings up 5 cache nodes (`hydracache-node-1..5`), Prometheus, Grafana, and
(if you're also using the reference app) Postgres. Ports on the host:
`7379-7383` (RESP), `8379-8383` (HTTP/metrics), `9090` (Prometheus), `3001`
(Grafana), `5432` (Postgres).

### Kubernetes (`deploy/k8s`)

```bash
docker build -t hydracache:latest .   # or push to a registry and reference it
kubectl apply -k deploy/k8s
kubectl -n hydracache get pods -w
```

See `deploy/k8s/README.md` for the StatefulSet's join-ordering design and
exactly what was and wasn't verified about these manifests before they were
committed (schema-validated offline; never applied to a live cluster in
this environment).

### From source, single node

```bash
go build -o hydracache ./cmd/server
go build -o hc ./cmd/cli
./hydracache -addr :7379 -http :8379 -data-dir ./data/wal
```

## Configuration reference

Two layers, and they compose: command-line flags, and an optional YAML file
(`-config path/to/config.yaml`) loaded by `internal/config.LoadConfig`,
which starts from `internal/config.DefaultConfig()` and overlays only the
keys you actually set — an empty or partial config file is safe; it does
not zero out anything you didn't mention.

**Flags** (`cmd/server/main.go`):

| Flag | Default | Meaning |
|---|---|---|
| `-config` | *(none)* | Path to a YAML config file |
| `-addr` | `:7379` | RESP/TCP listen address |
| `-advertise` | *(same as -addr)* | Address advertised to other nodes — set this explicitly whenever `-addr` isn't directly reachable by peers under that same address (containers, NAT, Kubernetes) |
| `-http` | `:8379` | HTTP API (health, metrics, cluster/stats JSON) listen address |
| `-data-dir` | *(from config)* | Directory for the WAL, snapshot, and election term store — overrides whatever the config file says |
| `-id` | *(auto-generated)* | This node's ID. Set it explicitly in any deployment where you need a *stable* identity across restarts (Kubernetes StatefulSet pods do this via their own pod name — see `deploy/k8s/statefulset.yaml`) |
| `-join` | *(none)* | Address of an existing node to join. Comma-separated for multiple seeds. Omit this only for the very first node in a brand-new cluster |

**Config file** (`internal/config.Config`, see `internal/config/config.go`
for the full struct and every default):

```yaml
server:
  max_conns: 10000
  read_timeout: 30s
  write_timeout: 30s
cache:
  eviction_policy: lru        # or lfu
  max_memory_bytes: 268435456 # 256MB
  replication_factor: 2
  replication_mode: async     # or sync
wal:
  enabled: true
  sync_mode: everysec         # always | everysec | never — see below
auth:
  enabled: false
  users:
    - username: default
      password: "sha256:<hex>"   # or a plaintext password, not recommended
      commands: ["*"]
      key_patterns: ["*"]
tls:
  enabled: false
  cert_file: /path/to/cert.pem
  key_file: /path/to/key.pem
  require_client_cert: false     # true = mutual TLS
```

An unrecognized `eviction_policy` or `wal.sync_mode` value is a **hard
startup error**, not a silent fallback to a default — a config typo should
fail loudly at boot, not silently run with different semantics than you
asked for.

### Choosing a WAL sync mode

- `always`: fsync after every write. Maximum durability, lowest write
  throughput. Use this if losing even the last write on a hard crash is
  unacceptable.
- `everysec` (default): fsync at most once per second, decoupled from
  write volume. You can lose up to ~1 second of the most recent writes on
  a hard crash or power loss (not a clean process crash — the OS page
  cache already has those). The right default for most deployments.
- `never`: no explicit fsync. Highest throughput, durability is whatever
  the OS's own write-back policy provides. Only reasonable if this cache
  is fully, cheaply re-derivable from a system of record (see
  `examples/readthrough` for exactly that pattern) and losing its contents
  outright is a non-event.

## Health checks and readiness

- `GET http://<node>:<http-port>/health` — returns `200 OK` if this node's
  process is up and its TCP server is running. This does **not** mean the
  node has finished joining the cluster or is caught up on replication —
  see "Known limitations" below. It's what `deploy/k8s/statefulset.yaml`'s
  readiness/liveness probes use, and what the Docker Compose healthchecks
  poll.
- `GET http://<node>:<http-port>/api/cluster` — JSON: every node this node
  currently knows about, each with `role` (leader/replica/peer), `health`
  (alive/suspect/dead/left), `replication_lag`, `last_seen`, `joined_at`.
  This is the real, live membership view — check it first when something
  seems wrong with the cluster.
- `CLUSTER INFO` / `CLUSTER MYID` over the RESP protocol (`hc cluster info`)
  — a quick from-the-client-side view of the same facts.

## Monitoring

- `GET http://<node>:<http-port>/metrics` — Prometheus text format:
  request/hit/miss/eviction counters, memory/connection/node gauges,
  per-replica replication lag, and a real cumulative latency histogram
  (`hydracache_request_duration_seconds_bucket`, usable directly with
  PromQL's `histogram_quantile`).
- `GET http://<node>:<http-port>/api/stats` — the same core numbers as
  JSON, plus the latency histogram as a flat array — this is what the
  React dashboard polls.
- **The React dashboard** (`dashboard/`, served by the same HTTP server if
  built into `dashboard/dist` next to the binary, or run separately with
  `npm run dev`) — cluster topology, replication lag, request/hit-rate
  charts, the latency histogram. When it can't reach a backend, it shows
  a clearly labeled "Demo Data — Disconnected" banner rather than silently
  displaying stale or fabricated numbers — if you see that banner, the
  dashboard itself is telling you where to look, not the cluster.
- **Grafana + Prometheus** (`deploy/docker-compose.yml`, `deploy/prometheus/`)
  — for anything beyond point-in-time numbers: trends, alerting, retention.

**No authentication exists on the metrics/admin HTTP server** — put it
behind a network boundary you control (a private subnet, a reverse proxy
with its own auth, a `NetworkPolicy` in Kubernetes), the same way you'd
treat any unauthenticated `/metrics` endpoint. This is a real, current gap,
not a design choice — see COMMANDS.md and INTERVIEW_PREP.md Part 7 for the
full list of gaps like this one.

## Node lifecycle

### Adding a node

Start it with `-join <any-existing-node-address>` (or several, comma-
separated, as seeds). It gossips its way into the membership view of the
whole cluster from there — you don't need to tell every existing node
about the new one individually. Once it's visible in `/api/cluster` with
`health: alive`, the hash ring's key ownership will have shifted to include
it, and key migration for its newly-owned range begins automatically
(`internal/hashring/rebalance.go`).

**Verify a join actually completed** — don't just trust that the process
started: poll `/api/cluster` from an *existing* node (not the new one) and
confirm the new node's ID shows up with `health: alive`. `/health` on the
new node itself only tells you its own process is up, not that gossip has
actually propagated its membership.

### Removing a node (planned maintenance)

There is no explicit "leave" command — the clean way to remove a node
today is: stop sending it traffic, let its keys' ownership settle onto its
replicas (already true continuously, since replicas already hold the
data), then stop the process. The failure detector will mark it dead after
`SuspectTimeout` (default 5s) once its heartbeats stop, and the rest of the
cluster reconverges via gossip the same way it would for an unplanned
failure — see "Troubleshooting" below for what that looks like in
`/api/cluster` while it's happening.

### Rolling restart

Restart nodes **one at a time**, waiting for the previous one to show
`health: alive` in `/api/cluster` (from a *different* node) before moving
to the next. This is exactly what `internal/chaostest`'s Rolling Restart
scenario exercises end-to-end (write keys, restart every node in turn,
verify every key is still correctly readable after each one) — the
procedure it tests is the procedure to actually follow.

If this node currently holds cluster-coordinator (leader) status, a clean
shutdown resigns that role explicitly before stopping (`elect.Resign()` in
`cmd/server/main.go`'s shutdown sequence) — a new coordinator gets elected
immediately rather than the rest of the cluster having to wait out a lease
timeout to notice.

## Backup and restore

Everything durable a node holds lives under its `-data-dir` (default
`./data/wal`, or whatever `wal.dir` says in config):

| File | Contents |
|---|---|
| `wal.log` | Append-only log of every write since the last snapshot |
| `snapshot.json` | Full cache state as of the last snapshot interval |
| `election/election-state.json` | This node's persisted election term/vote |

**Backup**: copy the `-data-dir` directory while the process is running.
The WAL is append-only and the snapshot is written via a temp-file-then-
rename (atomic) pattern, so a copy taken mid-write either sees the old
snapshot or the new one, never a half-written one — but a live copy can
still be a moment behind the absolute latest write; for a point-in-time
guarantee, stop the node first.

**Restore**: put the backed-up files back at the same `-data-dir` path and
start the node normally. Recovery (`internal/persistence/recovery.go`)
loads the snapshot, then replays every WAL entry after it, validating each
entry's checksum and cleanly truncating a torn tail (an incomplete final
entry from a crash mid-write) rather than failing the whole recovery.

**This is single-node backup/restore.** There is no cluster-wide
coordinated backup mechanism — for a full cluster restore, restore every
node's own data directory and let gossip/replication reconverge normally
once they're all back up.

## Security operations

- **Rotating an AUTH password**: edit the config file's `auth.users`
  entry and restart the node. There is no hot-reload — a config change
  requires a restart to take effect, on this node specifically (other
  nodes' own AUTH state is independent per-node config, not something this
  one node's restart affects).
- **Rotating a TLS certificate**: same story — replace the cert/key files
  and restart. There is no `SIGHUP`-triggered reload; this is a known,
  explicitly disclosed gap (see INTERVIEW_PREP.md Part 7), not an oversight
  you need to go looking for.
- **`AUTH`/ACL and clustering**: inter-node RPCs (`GOSSIP`, `REPLICATE`,
  `ELECTION_*`) are exempt from client ACL by design — enabling `auth` in
  config does not require any peer-to-peer credential, and does not by
  itself secure node-to-node traffic. If you need that, put TLS (ideally
  mutual TLS, `tls.require_client_cert: true`) on the same listener; there
  is currently no separate check of *which* certificate a peer presents
  against an expected peer identity (see COMMANDS.md).

## Troubleshooting

**A node won't join.** Check `/api/cluster` on the seed node it's joining
— if the new node never appears at all, confirm the `-join` address is
actually reachable from the new node's network position (a common
Kubernetes/Docker mistake: `-advertise` set to something only reachable
*inside* the cluster's own network, while `-join` on a new node points at
an address that's not resolvable from wherever it's actually running).

**Replication lag keeps growing on a replica.** Check `/api/cluster`'s
`replication_lag` for the affected node. A replica that's fallen far enough
behind triggers gap catch-up (`REPLICA_SYNC`) automatically — if lag is
growing *and* catch-up doesn't seem to be resolving it, check that node's
own logs for `[replication] gap catch-up ... failed` — a common cause is
the gap having exceeded the primary's retention buffer, which requires a
full resync (not automated today — see COMMANDS.md's replication section).

**A node is flapping between alive and suspect.** This is exactly what
phi-accrual is designed to avoid on a *normal* network — if you're seeing
it anyway, it usually means either genuinely unstable network conditions
between that node and the rest of the cluster, or `cluster.phi_threshold`/
`cluster.suspect_timeout` tuned too aggressively for your actual network's
jitter. Check the node's own heartbeat timing pattern before assuming it's
a config problem — INTERVIEW_PREP.md Part 4.6 explains exactly what the
detector is measuring and why.

**Cluster shows two leaders (or none).** During an active network
partition this is *expected*, not a bug — see INTERVIEW_PREP.md Part 4.7
and Part 8 for why quorum-based election tolerates (and is designed
around) exactly this during a partition, and reconverges to one leader
once the partition heals. If you see it with **no** partition and it
doesn't resolve within a few election timeouts (hundreds of milliseconds,
by default), that's the actual anomaly worth investigating — check every
node's own view of `/api/cluster` for disagreement about who else is
alive.

**A client's write seems to have vanished.** Confirm the client actually
connected to the key's real primary — see "Known limitations" immediately
below. A write to a non-primary node succeeds locally but is not
replicated anywhere via that path today, so it will not be visible from
any other node, and if that specific node is later removed, the write is
gone.

## Known limitations that affect operations

These are documented in depth in `COMMANDS.md` and `INTERVIEW_PREP.md` Part
7 — repeated here specifically because they change how you should operate
this system, not just what commands it accepts:

- **Writes to a non-primary node are not replicated.** Point every client
  at a specific, known node per key's ownership if you need this to be
  reliable across node loss — or accept that a client using one fixed
  address (like `cmd/cli` and `examples/readthrough` both do) is only
  fully durable for the subset of keys that node happens to be primary
  for.
- **No Redis Cluster protocol.** A generic Redis-Cluster-aware client
  cannot route directly to shards against this server — point any client
  at one address (or a plain load-balancing Service/Service mesh in front
  of the cluster, as `deploy/k8s/service-client.yaml` does), not at a
  Cluster-protocol-aware connection pool expecting `MOVED`/`ASK`.
- **No authentication on the metrics/admin HTTP port.** Network-isolate it.
- **`/health` reflects process liveness, not cluster convergence.** Don't
  gate "safe to route traffic here" purely on this passing — for a
  brand-new node, also check its visibility in `/api/cluster` from an
  existing node first.
- **No hot-reload of AUTH/TLS config.** Rotating credentials or
  certificates requires a restart of that node.
