package network

import (
	"fmt"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
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
	cache cache.Cache
}

func NewHandler(c cache.Cache) *Handler {
	return &Handler{cache: c}
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
	value, ttlNano, _, err := protocol.ParseSetFlags(cmd.Args)
	if err != nil {
		return &Response{err: err}
	}

	var ttl time.Duration
	if ttlNano > 0 {
		ttl = time.Duration(ttlNano)
	}

	if err := h.cache.Set(cmd.Args[0], []byte(value), ttl); err != nil {
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
	if err := h.cache.Expire(cmd.Args[0], time.Duration(seconds)*time.Second); err != nil {
		return &Response{data: []byte(":0\r\n")}
	}
	return &Response{data: []byte(":1\r\n")}
}

func (h *Handler) handlePersist(cmd *protocol.Command) *Response {
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
