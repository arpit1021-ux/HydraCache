package network

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/persistence"
	"github.com/hydracache/hydracache/internal/protocol"
	"github.com/hydracache/hydracache/internal/replication"
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
}

// GossipHandler processes GOSSIP commands. Set via SetGossip after construction.
type GossipHandler interface {
	HandleGossip(payload string) (string, error)
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
		h.replicateWrite(cmd.Name, cmd.Args)
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
		h.replicateWrite(cmd.Name, cmd.Args)
		return &Response{data: []byte("+OK\r\n")}
	}

	// --- Unconditional SET: WAL first, then mutate. Cache untouched if WAL fails. ---
	if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	if err := h.cache.Set(key, val, ttl); err != nil {
		return &Response{err: err}
	}
	h.replicateWrite(cmd.Name, cmd.Args)
	return &Response{data: []byte("+OK\r\n")}
}

func (h *Handler) handleGet(cmd *protocol.Command) *Response {
	val, err := h.cache.Get(cmd.Args[0])
	if err != nil {
		return &Response{data: []byte("$-1\r\n")}
	}
	return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val)}
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
	h.replicateWrite(cmd.Name, cmd.Args)
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
	h.replicateWrite(cmd.Name, cmd.Args)
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
	h.replicateWrite(cmd.Name, cmd.Args)
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

// replicateWrite appends an operation to the primary's ReplicationStream and
// fires async REPLICATE commands to all replica nodes. Called after a
// successful write (WAL append + cache mutation). Best-effort: replication
// failures are logged but do not affect the client response.
func (h *Handler) replicateWrite(cmd string, args []string) {
	if h.registry == nil || h.locator == nil {
		return
	}

	primary := h.locator.PrimaryNode(args[0])
	if primary != h.nodeID {
		return // not the primary for this key
	}

	rs, ok := h.registry.GetReplicaSet(primary)
	if !ok {
		return
	}

	streamInfo, ok := rs.GetReplica(primary)
	if !ok || streamInfo == nil || streamInfo.Stream == nil {
		return
	}

	op := replication.Operation{
		Command: cmd,
		Args:    args,
		NodeID:  h.nodeID,
	}
	streamInfo.Stream.Append(op)

	// Fan out to replicas asynchronously.
	for _, replica := range rs.ActiveReplicas() {
		if replica.NodeID == h.nodeID {
			continue
		}
		addr := replica.Address
		if addr == "" {
			continue
		}
		go h.sendReplicate(addr, op)
	}
}

// sendReplicate sends a REPLICATE command to a single replica over TCP.
// Best-effort: errors are logged but not propagated.
func (h *Handler) sendReplicate(addr string, op replication.Operation) {
	payload, err := json.Marshal(op)
	if err != nil {
		log.Printf("[replication] marshal error: %v", err)
		return
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		log.Printf("[replication] connect to %s failed: %v", shortAddr(addr), err)
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	w := bufio.NewWriter(conn)
	encoder := protocol.NewEncoder(w)
	if err := encoder.WriteArrayLen(2); err != nil {
		log.Printf("[replication] send to %s failed: %v", shortAddr(addr), err)
		return
	}
	if err := encoder.WriteBulkStringRaw("REPLICATE"); err != nil {
		log.Printf("[replication] send to %s failed: %v", shortAddr(addr), err)
		return
	}
	if err := encoder.WriteBulkStringRaw(string(payload)); err != nil {
		log.Printf("[replication] send to %s failed: %v", shortAddr(addr), err)
		return
	}
	_ = w.Flush()
}

// handleReplicate processes an incoming REPLICATE command from the primary.
// The payload is a JSON-encoded replication.Operation.
func (h *Handler) handleReplicate(cmd *protocol.Command) *Response {
	if len(cmd.Args) == 0 {
		return &Response{err: fmt.Errorf("REPLICATE requires a JSON payload")}
	}

	var op replication.Operation
	if err := json.Unmarshal([]byte(cmd.Args[0]), &op); err != nil {
		return &Response{err: fmt.Errorf("REPLICATE payload parse error: %w", err)}
	}

	// Apply the command to the local cache.
	innerCmd := &protocol.Command{Name: op.Command, Args: op.Args}
	resp := h.Handle(innerCmd)
	if resp.err != nil {
		return resp
	}
	return &Response{data: []byte("+OK\r\n")}
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

func shortAddr(addr string) string {
	if idx := strings.Index(addr, ":"); idx > 0 {
		return addr[:idx]
	}
	return addr
}
