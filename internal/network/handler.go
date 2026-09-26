package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/auth"
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
	acl              *auth.ACL
}

// Session holds per-connection state that must not be shared across
// connections — currently just AUTH status. The Server creates one per
// accepted connection and threads it through HandleAuthenticated for
// every command read on that connection.
type Session struct {
	authenticated bool
	username      string

	// id and addr are stamped by the Server when the connection is
	// accepted and never change for the connection's lifetime. name,
	// libName and libVer are set by the client via CLIENT SETNAME /
	// CLIENT SETINFO / HELLO SETNAME and default to "" until then.
	id      uint64
	addr    string
	name    string
	libName string
	libVer  string
}

const defaultAuthUsername = "default"

// hydraCacheVersion is reported by HELLO and INFO. It identifies this
// server as HydraCache, not Redis — HELLO's "server" field is honest
// about what actually answered the connection.
const hydraCacheVersion = "1.0.0"

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

// SetAuth enables AUTH/ACL enforcement for client connections. Must be
// called before the server starts accepting connections. When never
// called, HandleAuthenticated behaves exactly like Handle (no auth
// gate) — existing deployments and every test that predates auth are
// unaffected.
func (h *Handler) SetAuth(acl *auth.ACL) {
	h.acl = acl
}

// HandleAuthenticated is the entry point for commands read off a client
// connection (as opposed to internal dispatch — replicated-op application,
// election/gossip RPCs — which call Handle directly and are never subject
// to client ACL checks, since those are inter-node, not client-facing).
// AUTH, HELLO and CLIENT are always handled here directly (never reach
// Handle's switch) because they need access to the per-connection Session;
// when no ACL is configured (SetAuth never called) every other command
// passes straight through to Handle unchecked.
func (h *Handler) HandleAuthenticated(cmd *protocol.Command, sess *Session) *Response {
	if isInterNodeCommand(cmd.Name) {
		// Inter-node RPCs (gossip, replication, election) arrive over the
		// same listener as client connections, but a peer node never
		// sends AUTH — client ACL is the wrong control for them. Node-to-
		// node trust belongs to mutual TLS (not implemented yet), not to
		// the per-connection client session gate: exempting them here is
		// what keeps clustering working at all once AUTH is enabled,
		// rather than gating them on a session no peer will ever
		// authenticate.
		return h.Handle(cmd)
	}

	// AUTH and HELLO (which can carry an inline AUTH clause) must be
	// reachable before the session is authenticated — that's the whole
	// point of a login command — regardless of whether ACL is configured
	// at all, so a client always gets an honest reply instead of falling
	// through to "unknown command".
	switch cmd.Name {
	case "AUTH":
		return h.handleAuth(cmd, sess)
	case "HELLO":
		return h.handleHello(cmd, sess)
	}

	if h.acl == nil {
		if cmd.Name == "CLIENT" {
			return h.handleClient(cmd, sess)
		}
		return h.Handle(cmd)
	}

	if !sess.authenticated {
		return &Response{err: fmt.Errorf("NOAUTH Authentication required")}
	}

	if cmd.Name == "CLIENT" {
		return h.handleClient(cmd, sess)
	}

	keys := commandKeys(cmd)
	if len(keys) == 0 {
		if !h.acl.Allowed(sess.username, cmd.Name, "") {
			return &Response{err: fmt.Errorf("NOPERM this user has no permissions to run the '%s' command", strings.ToLower(cmd.Name))}
		}
	} else {
		for _, key := range keys {
			if !h.acl.Allowed(sess.username, cmd.Name, key) {
				return &Response{err: fmt.Errorf("NOPERM this user has no permissions to access one or more keys used as arguments for '%s'", strings.ToLower(cmd.Name))}
			}
		}
	}
	return h.Handle(cmd)
}

func (h *Handler) handleAuth(cmd *protocol.Command, sess *Session) *Response {
	if h.acl == nil {
		return &Response{err: fmt.Errorf("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")}
	}

	var username, password string
	switch len(cmd.Args) {
	case 1:
		username, password = defaultAuthUsername, cmd.Args[0]
	case 2:
		username, password = cmd.Args[0], cmd.Args[1]
	default:
		return &Response{err: fmt.Errorf("ERR wrong number of arguments for 'auth' command")}
	}

	if !h.acl.Authenticate(username, password) {
		sess.authenticated = false
		sess.username = ""
		return &Response{err: fmt.Errorf("WRONGPASS invalid username-password pair or user is disabled")}
	}
	sess.authenticated = true
	sess.username = username
	return &Response{data: []byte("+OK\r\n")}
}

// isHelloKeyword reports whether s is a HELLO sub-option keyword rather
// than a protocol version, so HELLO AUTH ... (protover omitted, meaning
// "keep whatever's negotiated, just authenticate") can be told apart from
// HELLO 2 AUTH ....
func isHelloKeyword(s string) bool {
	return strings.EqualFold(s, "AUTH") || strings.EqualFold(s, "SETNAME")
}

// handleHello implements HELLO [protover [AUTH username password]
// [SETNAME clientname]], matching real Redis's syntax and error strings so
// clients that probe capability on connect (go-redis, redis-cli) degrade
// gracefully. HydraCache only ever speaks RESP2 on the wire — nulls,
// booleans, maps, doubles and the other RESP3-only types are not
// implemented — so protover 3 is honestly rejected with the same NOPROTO
// error real Redis returns for a version it doesn't support, rather than
// claiming RESP3 and then encoding replies incorrectly.
func (h *Handler) handleHello(cmd *protocol.Command, sess *Session) *Response {
	args := cmd.Args
	i := 0
	if len(args) > 0 && !isHelloKeyword(args[0]) {
		switch args[0] {
		case "3":
			return &Response{err: fmt.Errorf("NOPROTO unsupported protocol version")}
		case "2":
		default:
			return &Response{err: fmt.Errorf("NOPROTO unsupported protocol version")}
		}
		i = 1
	}

	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "AUTH":
			if i+2 >= len(args) {
				return &Response{err: fmt.Errorf("ERR syntax error in HELLO")}
			}
			if h.acl != nil {
				username, password := args[i+1], args[i+2]
				if !h.acl.Authenticate(username, password) {
					return &Response{err: fmt.Errorf("WRONGPASS invalid username-password pair or user is disabled")}
				}
				sess.authenticated = true
				sess.username = username
			}
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				return &Response{err: fmt.Errorf("ERR syntax error in HELLO")}
			}
			if strings.ContainsAny(args[i+1], " \n") {
				return &Response{err: fmt.Errorf("ERR Client names cannot contain spaces, newlines or special characters.")}
			}
			sess.name = args[i+1]
			i += 2
		default:
			return &Response{err: fmt.Errorf("ERR syntax error in HELLO")}
		}
	}

	if h.acl != nil && !sess.authenticated {
		return &Response{err: fmt.Errorf("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")}
	}

	role := "master"
	if h.election != nil && !h.election.IsLeader() {
		role = "replica"
	}

	fields := [][2]string{
		{"server", "hydracache"},
		{"version", hydraCacheVersion},
		{"mode", "standalone"},
		{"role", role},
	}
	// 2 array elements per string field, plus proto (2), id (2) and
	// modules (2: the key plus an empty array) — we ship no modules.
	elemCount := len(fields)*2 + 6
	buf := fmt.Appendf(nil, "*%d\r\n", elemCount)
	for _, kv := range fields {
		buf = fmt.Appendf(buf, "$%d\r\n%s\r\n$%d\r\n%s\r\n", len(kv[0]), kv[0], len(kv[1]), kv[1])
	}
	buf = fmt.Appendf(buf, "$5\r\nproto\r\n:2\r\n")
	buf = fmt.Appendf(buf, "$2\r\nid\r\n:%d\r\n", sess.id)
	buf = fmt.Appendf(buf, "$7\r\nmodules\r\n*0\r\n")
	return &Response{data: buf}
}

// handleClient implements the subset of CLIENT that a per-connection
// Session can honestly answer: identity (GETNAME/SETNAME/ID/INFO) and the
// SETINFO calls go-redis and other modern clients send unconditionally
// on connect. Subcommands with no real backing capability here — LIST,
// KILL, PAUSE, UNPAUSE, NO-EVICT, NO-TOUCH, REPLY — are deliberately not
// implemented as no-ops (a no-op claiming success for something the
// server doesn't actually track would be exactly the kind of untested,
// fabricated capability this codebase avoids) and fall through to the
// unknown-subcommand error instead, which every client we've checked
// tolerates on optional startup commands.
func (h *Handler) handleClient(cmd *protocol.Command, sess *Session) *Response {
	if len(cmd.Args) == 0 {
		return &Response{err: fmt.Errorf("ERR wrong number of arguments for 'client' command")}
	}
	sub := strings.ToUpper(cmd.Args[0])
	switch sub {
	case "GETNAME":
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(sess.name), sess.name)}
	case "SETNAME":
		if len(cmd.Args) != 2 {
			return &Response{err: fmt.Errorf("ERR wrong number of arguments for 'client|setname' command")}
		}
		if strings.ContainsAny(cmd.Args[1], " \n") {
			return &Response{err: fmt.Errorf("ERR Client names cannot contain spaces, newlines or special characters.")}
		}
		sess.name = cmd.Args[1]
		return &Response{data: []byte("+OK\r\n")}
	case "ID":
		return &Response{data: fmt.Appendf(nil, ":%d\r\n", sess.id)}
	case "SETINFO":
		if len(cmd.Args) != 3 {
			return &Response{err: fmt.Errorf("ERR wrong number of arguments for 'client|setinfo' command")}
		}
		switch strings.ToLower(cmd.Args[1]) {
		case "lib-name":
			sess.libName = cmd.Args[2]
		case "lib-ver":
			sess.libVer = cmd.Args[2]
		default:
			return &Response{err: fmt.Errorf("ERR Unrecognized option '%s'", cmd.Args[1])}
		}
		return &Response{data: []byte("+OK\r\n")}
	case "INFO":
		info := fmt.Sprintf("id=%d addr=%s name=%s lib-name=%s lib-ver=%s resp=2",
			sess.id, sess.addr, sess.name, sess.libName, sess.libVer)
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(info), info)}
	default:
		return &Response{err: fmt.Errorf("ERR unknown subcommand or wrong number of arguments for '%s'. Try CLIENT HELP.", cmd.Args[0])}
	}
}

// isInterNodeCommand reports whether name is a server-to-server RPC
// (gossip, replication, election) rather than a client-facing command.
func isInterNodeCommand(name string) bool {
	switch name {
	case "GOSSIP", "REPLICATE", "REPLICA_SYNC", "ELECTION_VOTE", "ELECTION_HEARTBEAT":
		return true
	default:
		return false
	}
}

// commandKeys returns the key arguments an ACL key-pattern check should
// apply to. Commands not listed here have no natural single key argument
// and are gated by command permission alone.
func commandKeys(cmd *protocol.Command) []string {
	switch cmd.Name {
	case "GET", "TTL", "PTTL", "PERSIST", "EXPIRE", "SET", "SETNX":
		if len(cmd.Args) > 0 {
			return cmd.Args[:1]
		}
	case "DEL", "EXISTS":
		return cmd.Args
	}
	return nil
}

func (h *Handler) Handle(cmd *protocol.Command) *Response {
	if err := protocol.ValidateCommand(cmd); err != nil {
		return &Response{err: err}
	}
	if h.metricsCollector != nil {
		h.metricsCollector.IncrRequests()
		start := time.Now()
		defer func() {
			// defer runs after whichever case below returns, regardless of
			// which one fired, so this measures the real end-to-end
			// dispatch time for every command without threading timing
			// through each individual handleX function.
			h.metricsCollector.RecordLatency(cmd.Name, time.Since(start))
		}()
	}

	switch cmd.Name {
	case "PING":
		return h.handlePing(cmd)
	case "SET":
		return h.handleSet(cmd)
	case "SETNX":
		return h.handleSetNX(cmd)
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
	case "CLUSTER":
		return h.handleCluster(cmd)
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
		return &Response{err: fmt.Errorf("ERR unknown command '%s'", cmd.Name)}
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
			return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
		}
		if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
			return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
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
			return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
		}
		if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
			return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
		}
		return &Response{data: []byte("+OK\r\n")}
	}

	// --- Unconditional SET: WAL first, then mutate. Cache untouched if WAL fails. ---
	if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
		return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
	}
	if err := h.cache.Set(key, val, ttl); err != nil {
		return &Response{err: err}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: []byte("+OK\r\n")}
}

// handleSetNX implements the classic two-argument Redis SETNX command
// (distinct from SET key value NX): "set key to value only if key doesn't
// already exist," replying with an integer (1 set / 0 not set) rather than
// SET's simple-string-or-nil reply. It's a thin wrapper around handleSet's
// existing NX path — reusing its WAL/rollback/replication logic exactly —
// that only translates the reply encoding, so there's no duplicated
// conditional-write logic to keep in sync.
func (h *Handler) handleSetNX(cmd *protocol.Command) *Response {
	resp := h.handleSet(&protocol.Command{Name: "SET", Args: []string{cmd.Args[0], cmd.Args[1], "NX"}})
	if resp.err != nil {
		return resp
	}
	if string(resp.data) == "$-1\r\n" {
		return &Response{data: []byte(":0\r\n")}
	}
	return &Response{data: []byte(":1\r\n")}
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
		return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
	}
	count := 0
	for _, key := range cmd.Args {
		deleted, _ := h.cache.Delete(key)
		if deleted {
			count++
		}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
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
		return &Response{err: fmt.Errorf("ERR value is not an integer or out of range")}
	}
	// WAL first. Cache is untouched if WAL write fails.
	if err := h.walAppend("EXPIRE", cmd.Args, cmd.Args[0], nil, int64(seconds)*int64(time.Second)); err != nil {
		return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
	}
	if err := h.cache.Expire(cmd.Args[0], time.Duration(seconds)*time.Second); err != nil {
		return &Response{data: []byte(":0\r\n")}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
	}
	return &Response{data: []byte(":1\r\n")}
}

func (h *Handler) handlePersist(cmd *protocol.Command) *Response {
	// WAL first. Cache is untouched if WAL write fails.
	if err := h.walAppend("PERSIST", cmd.Args, cmd.Args[0], nil, 0); err != nil {
		return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
	}
	if err := h.cache.Persist(cmd.Args[0]); err != nil {
		return &Response{data: []byte(":0\r\n")}
	}
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
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
		return &Response{err: fmt.Errorf("ERR WAL write failed: %w", err)}
	}
	h.cache.Flush()
	if err := h.replicateWrite(cmd.Name, cmd.Args); err != nil {
		return &Response{err: fmt.Errorf("ERR write applied locally but under-replicated: %w", err)}
	}
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

// handleCluster implements CLUSTER INFO and CLUSTER MYID only.
// HydraCache shards via consistent hashing and routes every key
// server-side; it does not speak the Redis Cluster wire protocol (no
// CRC16 slot assignment, no MOVED/ASK redirects). Reporting
// cluster_enabled:1 or implementing CLUSTER NODES/SLOTS in Redis
// Cluster's own format would tell a cluster-aware client it can compute
// slots and route directly to nodes itself — which would silently break,
// since our routing algorithm isn't CRC16-mod-16384. Every other
// subcommand is refused with an explicit explanation instead of a
// fabricated reply.
func (h *Handler) handleCluster(cmd *protocol.Command) *Response {
	sub := strings.ToUpper(cmd.Args[0])
	switch sub {
	case "INFO":
		sharded := 0
		nodes := 1
		if h.locator != nil {
			nodes = h.locator.NodeCount()
			if nodes > 1 {
				sharded = 1
			}
		}
		info := fmt.Sprintf(
			"cluster_enabled:0\r\nhydracache_sharded:%d\r\nhydracache_node_id:%s\r\nhydracache_known_nodes:%d\r\n",
			sharded, h.nodeID, nodes,
		)
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(info), info)}
	case "MYID":
		return &Response{data: fmt.Appendf(nil, "$%d\r\n%s\r\n", len(h.nodeID), h.nodeID)}
	default:
		return &Response{err: fmt.Errorf("ERR CLUSTER %s is not supported: HydraCache shards transparently server-side and does not speak the Redis Cluster protocol. Supported: CLUSTER INFO, CLUSTER MYID", cmd.Args[0])}
	}
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
		return &Response{err: fmt.Errorf("ERR gossip not configured")}
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
	if err = client.Connect(); err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer client.Close()
	if err = client.SetDeadline(time.Now().Add(timeout)); err != nil {
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
