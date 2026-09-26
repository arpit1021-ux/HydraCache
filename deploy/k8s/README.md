# HydraCache on Kubernetes

A 5-node StatefulSet deployment: one seed pod (`hydracache-0`), four pods
that join it, a headless Service for stable per-pod DNS (peer discovery),
a client-facing Service, a PodDisruptionBudget protecting election quorum,
and a ConfigMap for cluster-wide (non-per-pod) settings.

## Deploying

```bash
# Build the image first (from the repo root) if you're not pulling one
# from a registry:
docker build -t hydracache:latest .

kubectl apply -k deploy/k8s
kubectl -n hydracache get pods -w
```

Pods come up in order (`hydracache-0`, then `hydracache-1`, ...) because
StatefulSets default to `podManagementPolicy: OrderedReady` — each pod must
pass its readiness probe before the next one is even created. This is what
makes the join logic in `statefulset.yaml` safe: every pod except
`hydracache-0` joins `hydracache-0` specifically, and by the time any
joining pod starts, `hydracache-0` is guaranteed to already be up.

## What's real here, and what isn't

- **The manifests themselves are schema-validated**, not just "written to
  look right" — see the verification note at the bottom of this file for
  exactly what was checked and how.
- **What is *not* verified**: an actual live deployment to a real cluster.
  This was written and validated in a development environment with no
  running Kubernetes cluster available (no `kind`/`minikube`, no live
  Docker daemon at the time of writing — see the main repo's session notes
  for the same limitation affecting `internal/chaostest`'s live scenarios).
  If you deploy this for real and something doesn't come up as described,
  that's genuinely useful information — nothing here should be taken as
  "tested end-to-end in a live cluster."
- **The Redis-Cluster-routing caveat applies here too.** `hydracache-client`
  is an ordinary Kubernetes `Service` — it load-balances across whichever
  pods are `Ready`, but HydraCache doesn't speak the Redis Cluster wire
  protocol (see `COMMANDS.md`), so a client connecting through this Service
  can land on a node that isn't the primary for the key it's about to use.
  This Service doesn't add sharding-aware routing that doesn't exist in the
  server itself — see `examples/readthrough/README.md` for the same
  limitation discussed from the client side.

## Sizing

The container's memory limit (512Mi) is deliberately set above the cache
engine's own default `max_memory_bytes` (256MB, from
`internal/config.DefaultConfig`) — the gap is headroom for WAL buffers,
goroutine stacks, and gossip/replication buffers that aren't counted in the
cache's own memory accounting (see `internal/cache/entry.go`'s
`estimatedSize`, which is a real but approximate accounting, not exact heap
measurement). If you change `cache.max_memory_bytes` in `configmap.yaml`,
raise the container memory limit to comfortably exceed it, not just match
it.

## Why `fsGroup: 1000` matters here specifically

The Dockerfile creates its runtime user with a pinned UID/GID (1000), not
whatever `adduser` would pick by default, *specifically* so this manifest
can state that number as a fact instead of a guess. A `chown` baked into a
Docker image layer only affects that layer — once a `PersistentVolumeClaim`
is mounted over `/data` at runtime, the volume's actual ownership (normally
root:root) is what the container sees, not whatever the image's build step
set. `securityContext.fsGroup: 1000` is what makes the mounted volume
actually writable by the non-root `hydracache` user the container runs as.

## Verification performed on these manifests

```bash
kubectl kustomize deploy/k8s > /tmp/hydracache-rendered.yaml
kubeconform -kubernetes-version 1.29.0 -summary -output text \
  /tmp/hydracache-rendered.yaml
# Summary: 6 resources found in 1 file - Valid: 6, Invalid: 0, Errors: 0, Skipped: 0
```

- `kubectl kustomize deploy/k8s` renders without error — valid YAML and
  valid Kustomize resource references.
- [`kubeconform`](https://github.com/yannh/kubeconform) validated all 6
  rendered resources (Namespace, ConfigMap, 2 Services, StatefulSet,
  PodDisruptionBudget) against the *real* Kubernetes 1.29 OpenAPI schema —
  this genuinely catches wrong field names, wrong types, and missing
  required fields, the same class of error `kubectl apply --dry-run=client`
  would catch given a live cluster to talk to. `kustomization.yaml` itself
  has no Kubernetes schema (it's Kustomize's own format, not a Kubernetes
  API resource) and is excluded from this check for that reason, not
  because it wasn't checked at all — `kubectl kustomize` already proved it
  parses and resolves correctly.
- **Not performed**: `kubectl apply --dry-run=server` (needs a live API
  server to talk to), and no live pod was ever actually scheduled or
  started — no Kubernetes cluster was available in this development
  environment. Schema-valid is not the same claim as "deploys successfully";
  don't read this section as more than it says.
