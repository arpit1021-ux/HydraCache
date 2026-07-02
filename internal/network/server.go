package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/protocol"
)

type Server struct {
	addr       string
	listener   net.Listener
	cache      cache.Cache
	maxConns   int
	sem        chan struct{}
	wg         sync.WaitGroup
	connCount  atomic.Int64
	handler    *Handler
	quit       chan struct{}
}

type ServerConfig struct {
	Addr     string
	MaxConns int
}

func NewServer(cfg ServerConfig, c cache.Cache) *Server {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 10000
	}
	return &Server{
		addr:     cfg.Addr,
		cache:    c,
		maxConns: cfg.MaxConns,
		sem:      make(chan struct{}, cfg.MaxConns),
		quit:     make(chan struct{}),
		handler:  NewHandler(c),
	}
}

func (s *Server) Start(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	go s.acceptLoop(ctx)
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) {
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

	s.connCount.Add(1)
	defer s.connCount.Add(-1)

	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	parser := protocol.NewParser(reader)
	encoder := protocol.NewEncoder(writer)

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
		response := s.handler.Handle(cmd)
		if err := response.WriteTo(encoder); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) Shutdown() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
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
