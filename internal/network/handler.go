package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/election"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/metrics"
	"github.com/hydracache/hydracache/internal/persistence"
	"github.com/hydracache/hydracache/internal/protocol"
	"github.com/hydracache/hydracache/internal/replication"
)

// Replication modes, set via SetReplicationMode. ModeAsync (the default)
// returns to the client as soon as the local write is durable, fanning
// out to replicas in the background. ModeSync blocks the client response
// until at least AckCount replicas confirm the write or SyncTimeout
// elapses — matching Redis's WAIT semantics: the local write has already
// happened either way, sync mode only reports whether it was
// sufficiently replicated, since there is no safe way to "undo" a write
// concurrent readers may have already observed.
const (
	ReplicationModeAsync = "async"
	ReplicationModeSync  = "sync"
)

type Response struct {
	data []byte
	err  error
}

func (r *Response) WriteTo(encoder *protocol.Encoder) error {
	if r.err != nil {
		return encoder.WriteError(r.err.Error())
	}
	return encoder.WriteRaw(r.data)
}

type Handler struct {
	cache    cache.Cache
	wal      *persistence.WAL
	gossip   GossipHandler
	nodeID   string
	registry *replication.ReplicaRegistry
	locator  *hashring.Locator
	election *election.Election
	// epochFn returns the current topology epoch, stamped onto outgoing
	// replication ops as the fencing token: a replica rejects an
	// incoming op whose epoch is behind the highest it has already seen
	// for that shard, since a strictly-increasing topology epoch means a
	// membership/ownership change happened after the sender last knew
	// about it (see ReplicaSet.CheckAndAdvanceEpoch).
	epochFn func() uint64

	replicationMode  string // ReplicationModeAsync (default) or ReplicationModeSync
	ackCount         int
	syncTimeout      time.Duration
	metricsCollector *metrics.Collector
	migration        MigrationChecker
}

// GossipHandler processes GOSSIP commands. Set via SetGossip after construction.
type GossipHandler interface {
	HandleGossip(payload string) (string, error)
}

// MigrationChecker reports whether a key was migrated away from this node
// to another one recently enough that a local miss should be redirected
// there instead of reported as a genuine miss — protecting a client whose
// routing view hasn't caught up with the latest topology change yet.
type MigrationChecker interface {
	RedirectTarget(key string) (addr string, ok bool)
}

func NewHandler(c cache.Cache) *Handler {
	return &Handler{cache: c}
}

func NewHandlerWithWAL(c cache.Cache, wal *persistence.WAL) *Handler {
	return &Handler{cache: c, wal: wal}
}

// SetGossip wires the gossip handler into the command dispatch.
// Must be called before the server starts accepting connections.
func (h *Handler) SetGossip(g GossipHandler) {
	h.gossip = g
}

// SetReplication wires replication into the command dispatch.
// Must be called before the server starts accepting connections.
func (h *Handler) SetReplication(nodeID string, registry *replication.ReplicaRegistry, locator *hashring.Locator) {
	h.nodeID = nodeID
	h.registry = registry
	h.locator = locator
}

// SetElection wires the node's Election into the command dispatch so peers
// can send it ELECTION_VOTE / ELECTION_HEARTBEAT RPCs. Must be called
// before the server starts accepting connections.
func (h *Handler) SetElection(e *election.Election) {
	h.election = e
}

// SetEpochSource wires a function returning the current topology epoch,
// used to stamp and validate the per-shard fencing token on replicated
// writes. Must be called before the server starts accepting connections.
func (h *Handler) SetEpochSource(fn func() uint64) {
	h.epochFn = fn
}

// SetReplicationMode configures whether writes wait for replica
// acknowledgment. mode must be ReplicationModeAsync (default if never
// called) or ReplicationModeSync; ackCount is how many replicas must ack
// in sync mode (capped to however many active replicas actually exist);
// syncTimeout bounds how long a sync write waits before failing.
func (h *Handler) SetReplicationMode(mode string, ackCount int, syncTimeout time.Duration) {
	h.replicationMode = mode
	h.ackCount = ackCount
	h.syncTimeout = syncTimeout
}

// SetMetricsCollector wires a metrics.Collector so real replication lag
// observed from replica acks is exported on /metrics instead of the
// counter sitting permanently at zero.
func (h *Handler) SetMetricsCollector(c *metrics.Collector) {
	h.metricsCollector = c
}

// SetMigrationChecker wires in-flight-migration redirect support: a local
// GET miss for a key this node recently migrated away is forwarded to its
// new owner instead of being reported as a genuine miss.
func (h *Handler) SetMigrationChecker(mc MigrationChecker) {
	h.migration = mc
}

func (h *Handler) Handle(cmd *protocol.Command) *Response {
	if err := protocol.ValidateCommand(cmd); err != nil {
		return &Response{err: err}
	}

	switch cmd.Name {
	case "PING":
		return h.handlePing(cmd)
	case "SET":
		return h.handleSet(cmd)
	case "GET":
		return h.handleGet(cmd)
	case "DEL":
		return h.handleDel(cmd)
	case "EXISTS":
		return h.handleExists(cmd)
	case "TTL":
		return h.handleTTL(cmd)
	case "PTTL":
		return h.handlePTTL(cmd)
	case "EXPIRE":
		return h.handleExpire(cmd)
	case "PERSIST":
		return h.handlePersist(cmd)
	case "KEYS":
		return h.handleKeys(cmd)
	case "DBSIZE":
		return h.handleDBSize(cmd)
	case "FLUSHALL":
		return h.handleFlushAll(cmd)
	case "INFO":
		return h.handleInfo(cmd)
	case "GOSSIP":
		return h.handleGossip(cmd)
	case "REPLICATE":
		return h.handleReplicate(cmd)
	case "REPLICA_SYNC":
		return h.handleReplicaSync(cmd)
	case "ELECTION_VOTE":
		return h.handleElectionVote(cmd)
	case "ELECTION_HEARTBEAT":
		return h.handleElectionHeartbeat(cmd)
	default:
		return &Response{err: fmt.Errorf("unknown command '%s'", cmd.Name)}
	}
}

func (h *Handler) handlePing(cmd *protocol.Command) *Response {
	if len(cmd.Args) == 0 {
		return &Response{data: []byte("+PONG\r\n")}
	}
	return &Response{data: fmt.Appendf(nil, "+%s\r\n", cmd.Args[0])}
}

func (h *Handler) handleSet(cmd *protocol.Command) *Response {
	value, ttlNano, flags, err := protocol.ParseSetFlags(cmd.Args)
	if err != nil {
		return &Response{err: err}
	}

	var ttl time.Duration
	if ttlNano > 0 {
		ttl = time.Duration(ttlNano)
	}

	hasNX := false
	hasXX := false
	for _, f := range flags {
		switch f {
		case "NX":
			hasNX = true
		case "XX":
			hasXX = true
		}
	}

	key := cmd.Args[0]
	val := []byte(value)

	// --- Conditional SET (NX/XX): mutate first, WAL after, rollback on failure ---
	if hasNX && hasXX {
		return &Response{data: []byte("$-1\r\n")}
	}

	if hasNX {
		// SetNX is atomic: if it succeeds, the key was absent at the moment
		// of the write. No prior state to capture — rollback on WAL failure
		// is always a plain Delete.
		if !h.cache.SetNX(key, val, ttl) {
			return &Response{data: []byte("$-1\r\n")}
		}
		if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
			_, _ = h.cache.Delete(key)
			return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
		}
		if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
			return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
		}
		return &Response{data: []byte("+OK\r\n")}
	}

	if hasXX {
		// XX path: rollback restores prior state because the key existed
		// before we overwrote it. Known limitation: a concurrent write
		// between our Get() and SetXX() could land between the mutation
		// and the rollback, and the rollback would clobber it. Full
		// correctness requires CAS-with-version, out of scope here.
		prior, priorErr := h.cache.Get(key)
		priorExists := priorErr == nil
		if !h.cache.SetXX(key, val, ttl) {
			return &Response{data: []byte("$-1\r\n")}
		}
		if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
			if priorExists {
				_ = h.cache.Set(key, prior, 0)
			} else {
				_, _ = h.cache.Delete(key)
			}
			return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
		}
		if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
			return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
		}
		return &Response{data: []byte("+OK\r\n")}
	}

	// --- Unconditional SET: WAL first, then mutate. Cache untouched if WAL fails. ---
	if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	if err := h.cache.Set(key, val, ttl); err != nil {
		return &Response{err: err}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: []byte("+OK\r\n")}
}

func (h *Handler) handleGet(cmd *protocol.Command) *Response {
	key := cmd.Args[0]
	val, err := h.cache.Get(key)
	if err == nil {
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val)}
	}

	// Local miss — if this key was migrated away to another node recently,
	// forward the request there instead of reporting a false miss for
	// data that has simply moved.
	if h.migration != nil {
		if addr, ok := h.migration.RedirectTarget(key); ok {
			if fv, ferr := h.forwardGet(addr, key); ferr == nil {
				return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(fv), fv)}
			}
		}
	}
	return &Response{data: []byte("$-1\r\n")}
}

// forwardGet issues a GET against another node on behalf of a client that
// reached this node for a key that has since moved elsewhere. Bounded by
// a short timeout so a redirect never turns into an unbounded hang.
//
// Known limitation: Client.Send cannot distinguish a genuine nil (RESP
// "$-1") from an empty-string value — both come back as "". A forwarded
// GET for a key whose real value happens to be empty will therefore be
// reported as a miss rather than an empty string. This pre-existing
// ambiguity in Client's reply parsing is not introduced by forwarding;
// it's accepted here rather than reworked, since the case it affects
// (an empty-string cached value, during the redirect TTL window after a
// migration) is narrow.
func (h *Handler) forwardGet(addr, key string) ([]byte, error) {
	const timeout = 500 * time.Millisecond
	client := NewClientWithTimeout(addr, timeout)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set deadline for %s: %w", addr, err)
	}
	resp, err := client.Send("GET", key)
	if err != nil {
		return nil, fmt.Errorf("forwarded GET to %s: %w", addr, err)
	}
	if resp == "" {
		return nil, fmt.Errorf("forwarded GET to %s: empty/miss response", addr)
	}
	return []byte(resp), nil
}

func (h *Handler) handleDel(cmd *protocol.Command) *Response {
	// WAL first for the unconditional DEL command. If WAL fails, the
	// keys remain in the cache — no mutation has occurred yet.
	if err := h.walAppend("DEL", cmd.Args, "", nil, 0); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	count := 0
	for _, key := range cmd.Args {
		deleted, _ := h.cache.Delete(key)
		if deleted {
			count++
		}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: fmt.Appendf(nil, ":%d\r\n", count)}
}

func (h *Handler) handleExists(cmd *protocol.Command) *Response {
	count := 0
	for _, key := range cmd.Args {
		exists, _ := h.cache.Exists(key)
		if exists {
			count++
		}
	}
	return &Response{data: fmt.Appendf(nil, ":%d\r\n", count)}
}

func (h *Handler) handleTTL(cmd *protocol.Command) *Response {
	ttl, err := h.cache.TTL(cmd.Args[0])
	if err != nil {
		return &Response{data: []byte(":-2\r\n")}
	}
	if ttl < 0 {
		return &Response{data: []byte(":-1\r\n")}
	}
	return &Response{data: fmt.Appendf(nil, ":%d\r\n", int(ttl.Seconds()))}
}

func (h *Handler) handlePTTL(cmd *protocol.Command) *Response {
	ttl, err := h.cache.TTL(cmd.Args[0])
	if err != nil {
		return &Response{data: []byte(":-2\r\n")}
	}
	if ttl < 0 {
		return &Response{data: []byte(":-1\r\n")}
	}
	return &Response{data: fmt.Appendf(nil, ":%d\r\n", ttl.Milliseconds())}
}

func (h *Handler) handleExpire(cmd *protocol.Command) *Response {
	var seconds int
	_, err := fmt.Sscanf(cmd.Args[1], "%d", &seconds)
	if err != nil {
		return &Response{err: fmt.Errorf("invalid expire value")}
	}
	// WAL first. Cache is untouched if WAL write fails.
	if err := h.walAppend("EXPIRE", cmd.Args, cmd.Args[0], nil, int64(seconds)*int64(time.Second)); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	if err := h.cache.Expire(cmd.Args[0], time.Duration(seconds)*time.Second); err != nil {
		return &Response{data: []byte(":0\r\n")}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: []byte(":1\r\n")}
}

func (h *Handler) handlePersist(cmd *protocol.Command) *Response {
	// WAL first. Cache is untouched if WAL write fails.
	if err := h.walAppend("PERSIST", cmd.Args, cmd.Args[0], nil, 0); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	if err := h.cache.Persist(cmd.Args[0]); err != nil {
		return &Response{data: []byte(":0\r\n")}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: []byte(":1\r\n")}
}

func (h *Handler) handleKeys(cmd *protocol.Command) *Response {
	keys, _ := h.cache.Keys()
	result := make([]string, 0, len(keys))
	for _, k := range keys {
		if cmd.Args[0] == "*" || strings.Contains(k, cmd.Args[0]) {
			result = append(result, k)
		}
	}
	var buf []byte
	buf = append(buf, fmt.Sprintf("*%d\r\n", len(result))...)
	for _, k := range result {
		buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)...)
	}
	return &Response{data: buf}
}

func (h *Handler) handleDBSize(cmd *protocol.Command) *Response {
	return &Response{data: fmt.Appendf(nil, ":%d\r\n", h.cache.Size())}
}

func (h *Handler) handleFlushAll(cmd *protocol.Command) *Response {
	// WAL first. Cache is untouched if WAL write fails.
	if err := h.walAppend("FLUSHALL", nil, "", nil, 0); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	h.cache.Flush()
	h.replicateWrite(cmd.Name, cmd.Args)
	return &Response{data: []byte("+OK\r\n")}
}

func (h *Handler) handleInfo(cmd *protocol.Command) *Response {
	stats := h.cache.(*cache.LocalCache).Stats()
	info := fmt.Sprintf(
		"keys:%d\r\nhits:%d\r\nmisses:%d\r\nhit_rate:%.4f\r\n",
		stats.Keys, stats.Hits, stats.Misses, stats.HitRate,
	)
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(info), info)}
}

// walAppend writes a WAL entry if a WAL is configured. Returns an error
// if the WAL is enabled but the append or sync failed.
func (h *Handler) walAppend(cmd string, args []string, key string, value []byte, ttl int64) error {
	if h.wal == nil {
		return nil
	}
	return h.wal.Append(persistence.WALEntry{
		Cmd:   cmd,
		Args:  args,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
}

func (h *Handler) handleGossip(cmd *protocol.Command) *Response {
	if h.gossip == nil {
		return &Response{err: fmt.Errorf("gossip not configured")}
	}
	resp, err := h.gossip.HandleGossip(cmd.Args[0])
	if err != nil {
		return &Response{err: err}
	}
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(resp), resp)}
}

// replicateWrite appends an operation to the primary's ReplicationStream
// and fans it out to replicas. In ReplicationModeAsync (default) it
// queues the fan-out and returns nil immediately regardless of outcome —
// the client's write is already durable locally. In ReplicationModeSync
// it blocks (bounded by h.syncTimeout) until at least h.ackCount replicas
// confirm the write, returning an error if that bound isn't met: the
// caller surfaces that as "applied locally but under-replicated" rather
// than a clean OK, since the local mutation has already happened and
// cannot be safely rolled back once other readers may have observed it.
//
// Commands with no key argument (e.g. FLUSHALL) aren't associated with a
// single shard's primary, so they are not replicated by this path; a
// cluster-wide broadcast for such commands is a separate, not-yet-built
// mechanism.
func (h *Handler) replicateWrite(cmd string, args []string) error {
	if h.registry == nil || h.locator == nil || len(args) == 0 {
		return nil
	}

	primary := h.locator.PrimaryNode(args[0])
	if primary != h.nodeID {
		return nil // not the primary for this key
	}

	rs, ok := h.registry.GetReplicaSet(primary)
	if !ok {
		return nil
	}

	streamInfo, ok := rs.GetReplica(primary)
	if !ok || streamInfo == nil || streamInfo.Stream == nil {
		return nil
	}

	var epoch uint64
	if h.epochFn != nil {
		epoch = h.epochFn()
	}
	op := replication.Operation{Command: cmd, Args: args, NodeID: h.nodeID, Epoch: epoch}
	op.Seq = streamInfo.Stream.Append(op)

	targets := rs.ActiveReplicas()

	if h.replicationMode == ReplicationModeSync {
		return h.replicateSync(rs, op, targets, streamInfo.Stream)
	}
	h.replicateAsync(rs, op, targets, streamInfo.Stream)
	return nil
}

// replicateAsync fires the op at every active replica in the background
// and, on a successful ack, records real lag (how far the replica's
// confirmed seq trails the stream's current tip). Every spawned goroutine
// exits on its own within sendReplicateOne's bounded timeout — never
// blocking the caller and never left running indefinitely.
func (h *Handler) replicateAsync(rs *replication.ReplicaSet, op replication.Operation, targets []*replication.ReplicaInfo, stream *replication.ReplicationStream) {
	for _, r := range targets {
		if r.NodeID == h.nodeID || r.Address == "" {
			continue
		}
		r := r
		go func() {
			if err := h.sendReplicateOne(r.Address, op, 5*time.Second); err != nil {
				log.Printf("[replication] async replicate to %s failed: %v", shortAddr(r.Address), err)
				return
			}
			h.recordAck(rs, r.NodeID, stream.LatestSeq()-op.Seq)
		}()
	}
}

// replicateSync fans the op out to every active replica concurrently and
// waits (bounded by h.syncTimeout) for at least h.ackCount of them to
// confirm. ackCount is capped to however many replicas actually exist —
// it can never demand more acks than there are replicas to give them.
func (h *Handler) replicateSync(rs *replication.ReplicaSet, op replication.Operation, targets []*replication.ReplicaInfo, stream *replication.ReplicationStream) error {
	timeout := h.syncTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type result struct {
		err    error
		nodeID string
	}
	results := make(chan result, len(targets))
	spawned := 0
	for _, r := range targets {
		if r.NodeID == h.nodeID || r.Address == "" {
			continue
		}
		spawned++
		r := r
		go func() {
			err := h.sendReplicateOneCtx(ctx, r.Address, op)
			results <- result{err: err, nodeID: r.NodeID}
		}()
	}

	need := h.ackCount
	if need > spawned {
		need = spawned
	}
	if need <= 0 {
		return nil
	}

	acked := 0
	for i := 0; i < spawned; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				log.Printf("[replication] sync replicate to %s failed: %v", shortID(res.nodeID), res.err)
				continue
			}
			acked++
			h.recordAck(rs, res.nodeID, stream.LatestSeq()-op.Seq)
			if acked >= need {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("sync replication timed out after %v: %d/%d required replicas acked", timeout, acked, need)
		}
	}
	return fmt.Errorf("sync replication failed: %d/%d required replicas acked", acked, need)
}

// recordAck updates a replica's tracked lag and, if a metrics collector is
// wired, exports it so it stops sitting permanently at zero on /metrics.
func (h *Handler) recordAck(rs *replication.ReplicaSet, nodeID string, lag int64) {
	if lag < 0 {
		lag = 0
	}
	rs.UpdateLag(nodeID, lag)
	if h.metricsCollector != nil {
		h.metricsCollector.SetReplicationLag(nodeID, lag)
	}
}

// sendReplicateOne sends one REPLICATE command to a replica and waits for
// its reply, bounded by timeout — the previous implementation never read
// the reply at all, so a "successful" replicate was never actually
// confirmed.
func (h *Handler) sendReplicateOne(addr string, op replication.Operation, timeout time.Duration) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshal replication op: %w", err)
	}
	client := NewClientWithTimeout(addr, timeout)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline for %s: %w", addr, err)
	}
	resp, err := client.Send("REPLICATE", string(payload))
	if err != nil {
		return fmt.Errorf("REPLICATE to %s: %w", addr, err)
	}
	if resp != "OK" {
		return fmt.Errorf("unexpected REPLICATE response from %s: %s", addr, resp)
	}
	return nil
}

// sendReplicateOneCtx bounds sendReplicateOne by ctx's deadline instead of
// a fixed timeout, so a synchronous replication round respects the
// overall sync timeout rather than each RPC getting its own separate one.
func (h *Handler) sendReplicateOneCtx(ctx context.Context, addr string, op replication.Operation) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Second)
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return ctx.Err()
	}
	return h.sendReplicateOne(addr, op, timeout)
}

// handleReplicate processes an incoming REPLICATE command from the
// primary. The payload is a JSON-encoded replication.Operation.
//
// op.NodeID identifies the shard (the primary that accepted the write).
// If this node tracks that shard's ReplicaSet, three checks run before
// the op is applied, all serialized under the shard's apply lock so two
// concurrently-arriving ops for the same shard (each opens its own
// connection, so the wire gives no ordering guarantee) can never race:
//  1. Epoch fencing — reject if op.Epoch is behind the highest seen for
//     this shard (see ReplicaSet.CheckAndAdvanceEpoch).
//  2. Staleness — if a later-or-equal seq has already been applied, this
//     op arrived late/out of order; applying it now would risk
//     clobbering a newer value with stale data, so it's dropped as OK.
//  3. Gap detection — if op.Seq skips ahead of what's been applied, pull
//     and apply the missing ops from the primary via REPLICA_SYNC before
//     applying this one.
func (h *Handler) handleReplicate(cmd *protocol.Command) *Response {
	if len(cmd.Args) == 0 {
		return &Response{err: fmt.Errorf("REPLICATE requires a JSON payload")}
	}

	var op replication.Operation
	if err := json.Unmarshal([]byte(cmd.Args[0]), &op); err != nil {
		return &Response{err: fmt.Errorf("REPLICATE payload parse error: %w", err)}
	}

	var rs *replication.ReplicaSet
	if h.registry != nil {
		rs, _ = h.registry.GetReplicaSet(op.NodeID)
	}

	// A shard we don't know about yet, or an unsequenced op (Seq<=0, e.g.
	// from a hand-built test), skips staleness/gap tracking entirely —
	// there's nothing to compare against yet.
	if rs == nil || op.Seq <= 0 {
		if rs != nil && !rs.CheckAndAdvanceEpoch(op.Epoch) {
			return &Response{err: fmt.Errorf("stale epoch %d for shard %s: rejected", op.Epoch, op.NodeID)}
		}
		return h.applyOp(op)
	}

	unlock := rs.LockApply()
	defer unlock()

	if !rs.CheckAndAdvanceEpoch(op.Epoch) {
		return &Response{err: fmt.Errorf("stale epoch %d for shard %s: rejected", op.Epoch, op.NodeID)}
	}

	last := rs.LastAppliedSeq()
	if last > 0 && op.Seq <= last {
		return &Response{data: []byte("+OK\r\n")} // stale/duplicate: already superseded
	}
	if last > 0 && op.Seq > last+1 {
		if err := h.catchUpGapLocked(rs, op.NodeID, last); err != nil {
			log.Printf("[replication] gap catch-up for shard %s failed (have=%d want=%d): %v",
				shortID(op.NodeID), last, op.Seq, err)
		}
	}

	resp := h.applyOp(op)
	if resp.err == nil {
		rs.SetLastAppliedSeq(op.Seq)
	}
	return resp
}

// applyOp applies a single replicated operation to the local cache. It
// does not touch shard bookkeeping (epoch/seq) — callers own that, since
// catch-up ops need to be applied without re-entering handleReplicate's
// (non-reentrant) apply lock.
func (h *Handler) applyOp(op replication.Operation) *Response {
	innerCmd := &protocol.Command{Name: op.Command, Args: op.Args}
	resp := h.Handle(innerCmd)
	if resp.err != nil {
		return resp
	}
	return &Response{data: []byte("+OK\r\n")}
}

// catchUpGapLocked pulls missing ops for a shard from its primary via
// REPLICA_SYNC and applies them in order. The caller must already hold
// rs's apply lock (see ReplicaSet.LockApply), and must NOT route these
// ops back through handleReplicate — that would try to re-acquire the
// same lock and deadlock.
func (h *Handler) catchUpGapLocked(rs *replication.ReplicaSet, primaryID string, lastApplied int64) error {
	primaryInfo, ok := rs.GetReplica(primaryID)
	if !ok || primaryInfo == nil || primaryInfo.Address == "" {
		return fmt.Errorf("no address known for primary %s", primaryID)
	}

	const timeout = 2 * time.Second
	client := NewClientWithTimeout(primaryInfo.Address, timeout)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect to primary %s: %w", shortAddr(primaryInfo.Address), err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	raw, err := client.Send("REPLICA_SYNC", fmt.Sprintf("%d", lastApplied))
	if err != nil {
		return fmt.Errorf("REPLICA_SYNC request: %w", err)
	}

	var result SyncResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return fmt.Errorf("REPLICA_SYNC response parse: %w", err)
	}
	if result.Status == "FULL_SYNC" {
		return fmt.Errorf("gap exceeds retention buffer: full sync required (not performed by gap catch-up)")
	}

	applied := 0
	for _, catchupOp := range result.Ops {
		if catchupOp.Seq <= rs.LastAppliedSeq() {
			continue
		}
		if !rs.CheckAndAdvanceEpoch(catchupOp.Epoch) {
			log.Printf("[replication] catch-up op seq=%d for shard %s rejected: stale epoch %d",
				catchupOp.Seq, shortID(primaryID), catchupOp.Epoch)
			continue
		}
		if resp := h.applyOp(catchupOp); resp.err != nil {
			return fmt.Errorf("apply catch-up op seq=%d: %w", catchupOp.Seq, resp.err)
		}
		rs.SetLastAppliedSeq(catchupOp.Seq)
		applied++
	}
	log.Printf("[replication] shard %s caught up %d/%d gap op(s), now at seq %d",
		shortID(primaryID), applied, len(result.Ops), rs.LastAppliedSeq())
	return nil
}

// SyncResult is returned by handleReplicaSync.
type SyncResult struct {
	Status  string                  `json:"status"`
	Ops     []replication.Operation `json:"ops,omitempty"`
	LastSeq int64                   `json:"last_seq"`
}

// handleReplicaSync processes a REPLICA_SYNC command from a reconnecting
// replica. The replica sends its last known seq; the primary responds with
// the gap operations or a FULL_SYNC signal.
//
// KNOWN SCOPE BOUNDARY — PULL PATH UNREACHABLE:
// This handler implements the PRIMARY-SIDE of the REPLICA_SYNC pull protocol
// (replica → primary). However, NO client-side caller exists yet: no
// reconnecting replica ever sends REPLICA_SYNC to a primary. The initial
// sync for newly-added replicas is handled by the PRIMARY-PUSH path in
// Manager.initiateReplicaSync (which uses REPLICATE/SET commands directly).
// The REPLICA_SYNC pull path is intentionally preserved for a future
// reconnecting-replica feature but is currently dead code from the caller's
// perspective. If you see this handler executing, something external is
// sending REPLICA_SYNC commands — that path is not wired by this codebase.
func (h *Handler) handleReplicaSync(cmd *protocol.Command) *Response {
	if len(cmd.Args) == 0 {
		return &Response{err: fmt.Errorf("REPLICA_SYNC requires lastKnownSeq")}
	}

	var lastSeq int64
	if _, err := fmt.Sscanf(cmd.Args[0], "%d", &lastSeq); err != nil {
		return &Response{err: fmt.Errorf("invalid lastKnownSeq: %w", err)}
	}

	if h.registry == nil || h.locator == nil {
		return &Response{err: fmt.Errorf("replication not configured")}
	}

	// Find the replica set for this node as primary.
	rs, ok := h.registry.GetReplicaSet(h.nodeID)
	if !ok {
		return &Response{err: fmt.Errorf("no replica set for this node")}
	}

	replicaInfo, ok := rs.GetReplica(h.nodeID)
	if !ok || replicaInfo == nil || replicaInfo.Stream == nil {
		return &Response{err: fmt.Errorf("no replication stream")}
	}

	stream := replicaInfo.Stream
	latestSeq := stream.LatestSeq()

	// Check if the gap is within the ring buffer.
	ops := stream.GetSince(lastSeq)
	if ops == nil && lastSeq < latestSeq {
		// Gap exceeds the buffer — trigger full sync.
		result := SyncResult{Status: "FULL_SYNC", LastSeq: latestSeq}
		data, _ := json.Marshal(result)
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(data), data)}
	}

	result := SyncResult{
		Status:  "OK",
		Ops:     ops,
		LastSeq: latestSeq,
	}
	data, _ := json.Marshal(result)
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(data), data)}
}

// handleElectionVote processes an incoming ELECTION_VOTE RPC (pre-vote or
// binding vote request) from a peer's Election instance.
func (h *Handler) handleElectionVote(cmd *protocol.Command) *Response {
	if h.election == nil {
		return &Response{err: fmt.Errorf("election not configured")}
	}
	var req election.VoteRequest
	if err := json.Unmarshal([]byte(cmd.Args[0]), &req); err != nil {
		return &Response{err: fmt.Errorf("ELECTION_VOTE payload parse error: %w", err)}
	}
	resp := h.election.HandleVoteRequest(req)
	data, err := json.Marshal(resp)
	if err != nil {
		return &Response{err: fmt.Errorf("ELECTION_VOTE marshal response: %w", err)}
	}
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(data), data)}
}

// handleElectionHeartbeat processes an incoming ELECTION_HEARTBEAT RPC
// from the current (claimed) leader.
func (h *Handler) handleElectionHeartbeat(cmd *protocol.Command) *Response {
	if h.election == nil {
		return &Response{err: fmt.Errorf("election not configured")}
	}
	var req election.HeartbeatRequest
	if err := json.Unmarshal([]byte(cmd.Args[0]), &req); err != nil {
		return &Response{err: fmt.Errorf("ELECTION_HEARTBEAT payload parse error: %w", err)}
	}
	resp := h.election.HandleHeartbeat(req)
	data, err := json.Marshal(resp)
	if err != nil {
		return &Response{err: fmt.Errorf("ELECTION_HEARTBEAT marshal response: %w", err)}
	}
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(data), data)}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func shortAddr(addr string) string {
	if idx := strings.Index(addr, ":"); idx > 0 {
		return addr[:idx]
	}
	return addr
}
