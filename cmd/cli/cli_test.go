package main

import (
	"context"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/network"
)

func startServer(t *testing.T) (string, func()) {
	t.Helper()
	c := cache.New(nil)
	srv := network.NewServer(network.ServerConfig{
		Addr:     "127.0.0.1:0",
		MaxConns: 100,
	}, c)

	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	addr := srv.Addr().String()
	cleanup := func() {
		srv.Shutdown()
		c.Shutdown()
		cancel()
	}
	return addr, cleanup
}

func TestConfigAddress(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: 7379}
	if cfg.Address() != "localhost:7379" {
		t.Errorf("expected 'localhost:7379', got '%s'", cfg.Address())
	}
}

func TestClientConnectionRefused(t *testing.T) {
	c, err := NewClient("127.0.0.1:59999")
	if err == nil {
		c.Close()
		t.Fatal("expected connection error")
	}
}

func TestClientPing(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	resp, err := c.Send("PING")
	if err != nil {
		t.Fatalf("failed to send PING: %v", err)
	}
	if resp.Type != 0 || resp.Str != "PONG" {
		t.Errorf("expected PONG, got %v", resp)
	}
}

func TestClientSetGet(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	// SET
	resp, err := c.Send("SET", "hello", "world")
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if resp.Str != "OK" {
		t.Errorf("expected OK, got %v", resp)
	}

	// GET
	resp, err = c.Send("GET", "hello")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if string(resp.Data) != "world" {
		t.Errorf("expected 'world', got '%s'", string(resp.Data))
	}
}

func TestClientDel(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	c.Send("SET", "key1", "val1")
	c.Send("SET", "key2", "val2")

	resp, err := c.Send("DEL", "key1")
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if resp.Integer != 1 {
		t.Errorf("expected 1, got %d", resp.Integer)
	}

	resp, err = c.Send("GET", "key1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.Data != nil {
		t.Errorf("expected nil, got %v", resp.Data)
	}
}

func TestClientExists(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	c.Send("SET", "exists_key", "val")

	resp, err := c.Send("EXISTS", "exists_key")
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if resp.Integer != 1 {
		t.Errorf("expected 1, got %d", resp.Integer)
	}

	resp, err = c.Send("EXISTS", "no_such_key")
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if resp.Integer != 0 {
		t.Errorf("expected 0, got %d", resp.Integer)
	}
}

func TestClientTTL(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	// TTL on non-existent key
	resp, err := c.Send("TTL", "no_key")
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if resp.Integer != -2 {
		t.Errorf("expected -2 for non-existent key, got %d", resp.Integer)
	}

	// SET with no TTL
	c.Send("SET", "no_ttl_key", "val")
	resp, _ = c.Send("TTL", "no_ttl_key")
	if resp.Integer != -1 {
		t.Errorf("expected -1 for key without TTL, got %d", resp.Integer)
	}

	// SET with TTL
	c.Send("SET", "ttl_key", "val", "EX", "10")
	resp, _ = c.Send("TTL", "ttl_key")
	if resp.Integer < 0 || resp.Integer > 10 {
		t.Errorf("expected TTL between 0-10, got %d", resp.Integer)
	}
}

func TestClientDBSize(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	c.Send("SET", "a", "1")
	c.Send("SET", "b", "2")

	resp, err := c.Send("DBSIZE")
	if err != nil {
		t.Fatalf("DBSIZE failed: %v", err)
	}
	if resp.Integer != 2 {
		t.Errorf("expected 2, got %d", resp.Integer)
	}
}

func TestClientFlushAll(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	c.Send("SET", "a", "1")
	c.Send("SET", "b", "2")

	resp, err := c.Send("FLUSHALL")
	if err != nil {
		t.Fatalf("FLUSHALL failed: %v", err)
	}
	if resp.Str != "OK" {
		t.Errorf("expected OK, got %v", resp)
	}

	resp, _ = c.Send("DBSIZE")
	if resp.Integer != 0 {
		t.Errorf("expected 0 after flush, got %d", resp.Integer)
	}
}

func TestClientError(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	// Unknown command
	resp, err := c.Send("BADCMD")
	if err != nil {
		t.Fatalf("unexpected connection error: %v", err)
	}
	if resp.Type != 1 { // ResponseError
		t.Errorf("expected error response, got type %d", resp.Type)
	}
}

func TestClientMultipleCommands(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer c.Close()

	// Multiple SETs
	for i := 0; i < 100; i++ {
		resp, err := c.Send("SET", "key", "val")
		if err != nil {
			t.Fatalf("SET %d failed: %v", i, err)
		}
		if resp.Str != "OK" {
			t.Errorf("SET %d: expected OK, got %v", i, resp)
		}
	}

	// Final GET
	resp, err := c.Send("GET", "key")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if string(resp.Data) != "val" {
		t.Errorf("expected 'val', got '%s'", string(resp.Data))
	}
}

func TestClientTimeout(t *testing.T) {
	// Connect to a non-routable address to trigger timeout
	c, err := NewClient("192.0.2.1:80")
	if err == nil {
		c.Close()
		t.Skip("connection succeeded unexpectedly")
	}
	_ = time.Second // ensure time package is used
}
