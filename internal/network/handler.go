package network

import (
	"fmt"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/persistence"
	"github.com/hydracache/hydracache/internal/protocol"
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
	cache  cache.Cache
	wal    *persistence.WAL
	gossip GossipHandler
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
		return &Response{data: []byte("+OK\r\n")}
	}

	// --- Unconditional SET: WAL first, then mutate. Cache untouched if WAL fails. ---
	if err := h.walAppend("SET", cmd.Args, key, val, ttlNano); err != nil {
		return &Response{err: fmt.Errorf("WAL write failed: %w", err)}
	}
	if err := h.cache.Set(key, val, ttl); err != nil {
		return &Response{err: err}
	}
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
