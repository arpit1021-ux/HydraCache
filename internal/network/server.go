package network

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
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
}

func NewServer(cfg ServerConfig, c cache.Cache) *Server {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 10000
	}
	return &Server{
		addr:      cfg.Addr,
		tlsConfig: cfg.TLSConfig,
		cache:     c,
		maxConns:  cfg.MaxConns,
		sem:       make(chan struct{}, cfg.MaxConns),
		quit:      make(chan struct{}),
		handler:   NewHandler(c),
	}
}

func NewServerWithWAL(cfg ServerConfig, c cache.Cache, wal *persistence.WAL) *Server {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 10000
	}
	return &Server{
		addr:      cfg.Addr,
		tlsConfig: cfg.TLSConfig,
		cache:     c,
		maxConns:  cfg.MaxConns,
		sem:       make(chan struct{}, cfg.MaxConns),
		quit:      make(chan struct{}),
		handler:   NewHandlerWithWAL(c, wal),
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

func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-s.quit:
			return
		default:
		}
		conn, err := s.listener.Accept()
		if err != nil {
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

	s.trackConn(conn)
	defer s.untrackConn(conn)

	s.connCount.Add(1)
	defer s.connCount.Add(-1)

	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	parser := protocol.NewParser(reader)
	encoder := protocol.NewEncoder(writer)
	sess := &Session{}

	_ = conn.SetDeadline(time.Now().Add(30 * time.Minute))

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
		cmd, err := parser.ReadCommand()
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				_ = encoder.WriteError(err.Error())
				_ = writer.Flush()
			}
			return
		}

		_ = conn.SetDeadline(time.Now().Add(30 * time.Minute))
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
