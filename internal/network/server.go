package network

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
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

type Server struct {
	addr         string
	tlsConfig    *tls.Config
	listener     net.Listener
	cache        cache.Cache
	maxConns     int
	sem          chan struct{}
	wg           sync.WaitGroup
	connCount    atomic.Int64
	handler      *Handler
	quit         chan struct{}
	shutdownOnce sync.Once
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxBulkBytes int

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

type ServerConfig struct {
	Addr     string
	MaxConns int
	// TLSConfig, if set, terminates TLS on this listener — every
	// connection, client or peer node, must speak TLS to connect at all
	// (see the AUTH increment's finding: client and inter-node traffic
	// share one listener in this architecture, so there's one TLS
	// decision for both, not a separate one per traffic type).
	TLSConfig *tls.Config
	// ReadTimeout/WriteTimeout bound how long a connection may sit idle
	// waiting to send a command or to have its response written,
	// respectively — a slow or stalled client eventually gets
	// disconnected rather than holding a connection (and a MaxConns
	// slot) forever. Default 30s each if unset.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// MaxBulkBytes bounds a single RESP bulk string's length. Default
	// protocol.DefaultMaxBulkLen if unset — see NewParserWithLimits for
	// why this is load-bearing, not just a resource-usage knob.
	MaxBulkBytes int
}

func applyServerDefaults(cfg *ServerConfig) {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 10000
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.MaxBulkBytes <= 0 {
		cfg.MaxBulkBytes = protocol.DefaultMaxBulkLen
	}
}

func NewServer(cfg ServerConfig, c cache.Cache) *Server {
	applyServerDefaults(&cfg)
	return &Server{
		addr:         cfg.Addr,
		tlsConfig:    cfg.TLSConfig,
		cache:        c,
		maxConns:     cfg.MaxConns,
		sem:          make(chan struct{}, cfg.MaxConns),
		quit:         make(chan struct{}),
		handler:      NewHandler(c),
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
		maxBulkBytes: cfg.MaxBulkBytes,
	}
}

func NewServerWithWAL(cfg ServerConfig, c cache.Cache, wal *persistence.WAL) *Server {
	applyServerDefaults(&cfg)
	return &Server{
		addr:         cfg.Addr,
		tlsConfig:    cfg.TLSConfig,
		cache:        c,
		maxConns:     cfg.MaxConns,
		sem:          make(chan struct{}, cfg.MaxConns),
		quit:         make(chan struct{}),
		handler:      NewHandlerWithWAL(c, wal),
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
		maxBulkBytes: cfg.MaxBulkBytes,
	}
}

func (s *Server) Start(ctx context.Context) error {
	var err error
	if s.tlsConfig != nil {
		s.listener, err = tls.Listen("tcp", s.addr, s.tlsConfig)
	} else {
		s.listener, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return nil
}

// acceptLoop reserves a semaphore slot BEFORE calling Accept, not after —
// this is what makes MaxConns an actual admission-control bound rather
// than a formality. The previous version accepted every inbound TCP
// connection unconditionally (spawning a goroutine and consuming a file
// descriptor for each) and only checked the semaphore inside
// handleConnection; past MaxConns, excess connections were still fully
// accepted and just sat blocked forever, so an attacker opening far more
// connections than MaxConns could still exhaust file descriptors and
// goroutine memory — the exact resource-exhaustion MaxConns exists to
// prevent. Reserving the slot first means Accept is only called once
// capacity is actually available; anything beyond that queues in the
// kernel's listen backlog (bounded, OS-managed) instead of user space.
func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case s.sem <- struct{}{}:
		case <-s.quit:
			return
		}

		conn, err := s.listener.Accept()
		if err != nil {
			<-s.sem
			select {
			case <-s.quit:
				return
			default:
			}
			continue
		}
		s.wg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer func() { <-s.sem }() // release the slot acceptLoop reserved

	// A panic anywhere in this connection's request handling (parser,
	// dispatch, a bug in a command handler) must never take down the
	// whole process over one client's bad input or an edge case this
	// code didn't anticipate — it drops this one connection instead.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[network] recovered from panic handling connection from %s: %v", conn.RemoteAddr(), r)
		}
	}()

	s.trackConn(conn)
	defer s.untrackConn(conn)

	s.connCount.Add(1)
	defer s.connCount.Add(-1)

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	parser := protocol.NewParserWithLimits(reader, s.maxBulkBytes, protocol.DefaultMaxArrayLen)
	encoder := protocol.NewEncoder(writer)
	sess := &Session{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		cmd, err := parser.ReadCommand()
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
				_ = encoder.WriteError(err.Error())
				_ = writer.Flush()
			}
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		response := s.handler.HandleAuthenticated(cmd, sess)
		if err := response.WriteTo(encoder); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// Shutdown stops accepting connections and waits for in-flight ones to
// finish. Safe to call more than once (e.g. an explicit Shutdown in a
// test followed by a deferred/cleanup one) — later calls are no-ops.
func (s *Server) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.quit)
		if s.listener != nil {
			s.listener.Close()
		}
	})
	s.wg.Wait()
}

func (s *Server) trackConn(conn net.Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[conn] = struct{}{}
}

func (s *Server) untrackConn(conn net.Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	delete(s.conns, conn)
}

// CloseAllConnections abruptly closes every currently-connected client
// connection without waiting for in-flight requests to finish, unlike
// Shutdown (which only stops accepting new connections and lets existing
// ones end naturally). This simulates a hard process crash rather than a
// graceful stop — primarily for fault-injection tests that need a
// deterministic "the peer died mid-request" rather than a timing-dependent
// one.
func (s *Server) CloseAllConnections() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for conn := range s.conns {
		conn.Close()
	}
}

func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) ConnectionCount() int64 {
	return s.connCount.Load()
}

// SetGossip wires gossip into the command handler. Must be called
// before the server starts accepting connections.
func (s *Server) SetGossip(g GossipHandler) {
	s.handler.SetGossip(g)
}

// SetReplication wires replication into the command handler. Must be called
// before the server starts accepting connections.
func (s *Server) SetReplication(nodeID string, registry *replication.ReplicaRegistry, locator *hashring.Locator) {
	s.handler.SetReplication(nodeID, registry, locator)
}

// SetElection wires election RPC dispatch into the command handler. Must
// be called before the server starts accepting connections.
func (s *Server) SetElection(e *election.Election) {
	s.handler.SetElection(e)
}

// SetEpochSource wires the topology-epoch fencing source into the command
// handler. Must be called before the server starts accepting connections.
func (s *Server) SetEpochSource(fn func() uint64) {
	s.handler.SetEpochSource(fn)
}

// SetReplicationMode configures async/sync replication. Must be called
// before the server starts accepting connections.
func (s *Server) SetReplicationMode(mode string, ackCount int, syncTimeout time.Duration) {
	s.handler.SetReplicationMode(mode, ackCount, syncTimeout)
}

// SetMetricsCollector wires a metrics.Collector so replication lag
// observed from real replica acks is exported on /metrics.
func (s *Server) SetMetricsCollector(c *metrics.Collector) {
	s.handler.SetMetricsCollector(c)
}

// SetMigrationChecker wires in-flight-migration redirect support into the
// command handler.
func (s *Server) SetMigrationChecker(mc MigrationChecker) {
	s.handler.SetMigrationChecker(mc)
}

// SetAuth enables AUTH/ACL enforcement for client connections. Must be
// called before the server starts accepting connections.
func (s *Server) SetAuth(acl *auth.ACL) {
	s.handler.SetAuth(acl)
}
