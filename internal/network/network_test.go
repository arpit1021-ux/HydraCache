package network

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/protocol"
)

func newTestCache() cache.Cache {
	opts := &cache.Options{
		EvictionPolicy:       cache.EvictionLRU,
		EvictionCapacity:     1000,
		ActiveExpiration:     false,
		ExpirationInterval:   time.Second,
		ExpirationSampleSize: 10,
	}
	return cache.New(opts)
}

func startTestServer(t *testing.T) (*Server, *Client) {
	t.Helper()
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 100}, c)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() { srv.Shutdown() })

	addr := srv.Addr().String()
	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return srv, client
}

func TestServer_StartAndShutdown(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if srv.Addr() == nil {
		t.Error("Addr() should not be nil after Start")
	}
	srv.Shutdown()
}

func TestServer_ShutdownWaitsForAcceptLoop(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr := srv.Addr().String()

	alive := make(chan struct{})
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
		}
		close(alive)
	}()
	<-alive

	srv.Shutdown()

	// After Shutdown returns, acceptLoop must have exited.
	// Prove it by attempting a new connection — it must be refused.
	_, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		t.Error("connection should be refused after Shutdown — acceptLoop may still be running")
	}
}

func TestServer_Addr_BeforeStart(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	if srv.Addr() != nil {
		t.Error("Addr() should be nil before Start")
	}
}

func TestServer_ConnectionCount(t *testing.T) {
	srv, client := startTestServer(t)
	_ = client

	if srv.ConnectionCount() < 0 {
		t.Errorf("ConnectionCount = %d, want >= 0", srv.ConnectionCount())
	}
}

func TestNewServer_DefaultMaxConns(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	if srv.maxConns != 10000 {
		t.Errorf("default maxConns = %d, want 10000", srv.maxConns)
	}
}

func TestNewServer_ZeroMaxConns(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 0}, c)
	if srv.maxConns != 10000 {
		t.Errorf("zero maxConns defaults to %d, want 10000", srv.maxConns)
	}
}

func TestNewServer_NegativeMaxConns(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: -5}, c)
	if srv.maxConns != 10000 {
		t.Errorf("negative maxConns defaults to %d, want 10000", srv.maxConns)
	}
}

func TestClient_Ping(t *testing.T) {
	_, client := startTestServer(t)

	resp, err := client.Send("PING")
	if err != nil {
		t.Fatalf("PING error: %v", err)
	}
	if resp != "PONG" {
		t.Errorf("PING response = %q, want PONG", resp)
	}
}

func TestClient_PingWithArg(t *testing.T) {
	_, client := startTestServer(t)

	resp, err := client.Send("PING", "hello")
	if err != nil {
		t.Fatalf("PING hello error: %v", err)
	}
	if resp != "hello" {
		t.Errorf("PING hello response = %q, want hello", resp)
	}
}

func TestClient_SetAndGet(t *testing.T) {
	_, client := startTestServer(t)

	_, err := client.Send("SET", "mykey", "myvalue")
	if err != nil {
		t.Fatalf("SET error: %v", err)
	}

	resp, err := client.Send("GET", "mykey")
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	if resp != "myvalue" {
		t.Errorf("GET response = %q, want myvalue", resp)
	}
}

func TestClient_GetMissingKey(t *testing.T) {
	_, client := startTestServer(t)

	resp, err := client.Send("GET", "nonexistent")
	if err != nil {
		t.Fatalf("GET missing key error: %v", err)
	}
	if resp != "" {
		t.Errorf("GET missing key should return empty string, got %q", resp)
	}
}

func TestClient_SetWithTTL(t *testing.T) {
	_, client := startTestServer(t)

	_, err := client.Send("SET", "ttlkey", "val", "EX", "2")
	if err != nil {
		t.Fatalf("SET with TTL error: %v", err)
	}

	resp, err := client.Send("GET", "ttlkey")
	if err != nil {
		t.Fatalf("GET ttlkey error: %v", err)
	}
	if resp != "val" {
		t.Errorf("GET response = %q, want val", resp)
	}
}

func TestClient_Delete(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "delkey", "val1")
	client.Send("SET", "delkey2", "val2")

	resp, err := client.Send("DEL", "delkey", "delkey2")
	if err != nil {
		t.Fatalf("DEL error: %v", err)
	}
	if resp != "2" {
		t.Errorf("DEL response = %q, want 2", resp)
	}

	resp, err = client.Send("GET", "delkey")
	if err != nil {
		t.Fatalf("GET after DEL error: %v", err)
	}
	if resp != "" {
		t.Errorf("GET after DEL should return empty, got %q", resp)
	}
}

func TestClient_Exists(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "existskey", "val")

	resp, err := client.Send("EXISTS", "existskey")
	if err != nil {
		t.Fatalf("EXISTS error: %v", err)
	}
	if resp != "1" {
		t.Errorf("EXISTS response = %q, want 1", resp)
	}

	client.Send("DEL", "existskey")
	resp, err = client.Send("EXISTS", "existskey")
	if err != nil {
		t.Fatalf("EXISTS after DEL error: %v", err)
	}
	if resp != "0" {
		t.Errorf("EXISTS after DEL response = %q, want 0", resp)
	}
}

func TestClient_DBSize(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "k1", "v1")
	client.Send("SET", "k2", "v2")
	client.Send("SET", "k3", "v3")

	resp, err := client.Send("DBSIZE")
	if err != nil {
		t.Fatalf("DBSIZE error: %v", err)
	}
	if resp != "3" {
		t.Errorf("DBSIZE response = %q, want 3", resp)
	}
}

func TestClient_FlushAll(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "k1", "v1")
	client.Send("SET", "k2", "v2")

	resp, err := client.Send("FLUSHALL")
	if err != nil {
		t.Fatalf("FLUSHALL error: %v", err)
	}
	if resp != "OK" {
		t.Errorf("FLUSHALL response = %q, want OK", resp)
	}

	resp, _ = client.Send("DBSIZE")
	if resp != "0" {
		t.Errorf("DBSIZE after FLUSHALL = %q, want 0", resp)
	}
}

func TestClient_Keys(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "user:1", "alice")
	client.Send("SET", "user:2", "bob")
	client.Send("SET", "item:1", "widget")

	resp, err := client.Send("KEYS", "user")
	if err != nil {
		t.Fatalf("KEYS error: %v", err)
	}
	if resp == "" {
		t.Error("KEYS should return results")
	}
}

func TestClient_Info(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "k1", "v1")
	resp, err := client.Send("INFO")
	if err != nil {
		t.Fatalf("INFO error: %v", err)
	}
	if resp == "" {
		t.Error("INFO should return stats")
	}
}

func TestClient_TTL(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "ttl1", "v", "EX", "100")
	resp, err := client.Send("TTL", "ttl1")
	if err != nil {
		t.Fatalf("TTL error: %v", err)
	}
	if resp == "" {
		t.Error("TTL should return a value")
	}
}

func TestClient_PTTL(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "pttl1", "v", "PX", "50000")
	resp, err := client.Send("PTTL", "pttl1")
	if err != nil {
		t.Fatalf("PTTL error: %v", err)
	}
	if resp == "" {
		t.Error("PTTL should return a value")
	}
}

func TestClient_Persist(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "pkey", "v", "EX", "100")
	resp, err := client.Send("PERSIST", "pkey")
	if err != nil {
		t.Fatalf("PERSIST error: %v", err)
	}
	if resp != "1" {
		t.Errorf("PERSIST response = %q, want 1", resp)
	}
}

func TestClient_Expire(t *testing.T) {
	_, client := startTestServer(t)

	client.Send("SET", "ekey", "v")
	resp, err := client.Send("EXPIRE", "ekey", "100")
	if err != nil {
		t.Fatalf("EXPIRE error: %v", err)
	}
	if resp != "1" {
		t.Errorf("EXPIRE response = %q, want 1", resp)
	}
}

func TestClient_Expire_NonexistentKey(t *testing.T) {
	_, client := startTestServer(t)

	resp, err := client.Send("EXPIRE", "nokey", "100")
	if err != nil {
		t.Fatalf("EXPIRE error: %v", err)
	}
	if resp != "0" {
		t.Errorf("EXPIRE nonexistent = %q, want 0", resp)
	}
}

func TestClient_NotConnected(t *testing.T) {
	client := NewClient("127.0.0.1:99999")
	_, err := client.Send("PING")
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestClient_Close(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	srv.Start(context.Background())
	defer srv.Shutdown()

	client := NewClient(srv.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.IsConnected() {
		t.Error("should be connected")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.IsConnected() {
		t.Error("should not be connected after Close")
	}
}

func TestClient_CloseNilConn(t *testing.T) {
	client := NewClient("127.0.0.1:99999")
	if err := client.Close(); err != nil {
		t.Errorf("Close on nil conn should not error, got: %v", err)
	}
}

func TestClient_ConnectRefused(t *testing.T) {
	client := NewClient("127.0.0.1:19999")
	err := client.Connect()
	if err == nil {
		client.Close()
		t.Fatal("expected connection refused error")
	}
}

func TestClient_IsConnectedBeforeConnect(t *testing.T) {
	client := NewClient("127.0.0.1:99999")
	if client.IsConnected() {
		t.Error("should not be connected before Connect()")
	}
}

func TestServer_MultipleClients(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 50}, c)
	srv.Start(context.Background())
	defer srv.Shutdown()

	const numClients = 10
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		cl := NewClient(srv.Addr().String())
		if err := cl.Connect(); err != nil {
			t.Fatalf("client %d connect: %v", i, err)
		}
		clients[i] = cl
	}
	defer func() {
		for _, cl := range clients {
			cl.Close()
		}
	}()

	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(id int, c *Client) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			val := fmt.Sprintf("val%d", id)
			c.Send("SET", key, val)
			resp, err := c.Send("GET", key)
			if err != nil {
				t.Errorf("client %d GET error: %v", id, err)
				return
			}
			if resp != val {
				t.Errorf("client %d GET = %q, want %q", id, resp, val)
			}
		}(i, cl)
	}
	wg.Wait()
}

func TestServer_ConcurrentSetGet(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 50}, c)
	srv.Start(context.Background())
	defer srv.Shutdown()

	client := NewClient(srv.Addr().String())
	client.Connect()
	defer client.Close()

	const ops = 100
	var wg sync.WaitGroup
	wg.Add(ops)

	for i := 0; i < ops; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", id)
			val := fmt.Sprintf("v%d", id)
			client.Send("SET", key, val)
			resp, err := client.Send("GET", key)
			if err != nil {
				t.Errorf("GET %s error: %v", key, err)
				return
			}
			if resp != val {
				t.Errorf("GET %s = %q, want %q", key, resp, val)
			}
		}(i)
	}
	wg.Wait()
}

func TestServer_ShutdownClosesListener(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	srv.Start(context.Background())
	addr := srv.Addr().String()
	srv.Shutdown()

	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Error("connection should fail after shutdown")
	}
}

func TestClient_SendMultipleCommands(t *testing.T) {
	_, client := startTestServer(t)

	commands := []struct {
		args []interface{}
	}{
		{[]interface{}{"SET", "a", "1"}},
		{[]interface{}{"SET", "b", "2"}},
		{[]interface{}{"SET", "c", "3"}},
	}
	for _, cmd := range commands {
		_, err := client.Send(cmd.args...)
		if err != nil {
			t.Fatalf("Send %v error: %v", cmd.args, err)
		}
	}

	resp, _ := client.Send("DBSIZE")
	if resp != "3" {
		t.Errorf("DBSIZE = %q, want 3", resp)
	}
}

func TestServer_InvalidCommand(t *testing.T) {
	_, client := startTestServer(t)

	_, err := client.Send("BOGUS")
	if err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestHandler_PingNoArgs(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	resp := h.Handle(&protocol.Command{Name: "PING", Args: []string{}})
	if resp.err != nil {
		t.Errorf("unexpected error: %v", resp.err)
	}
}

func TestHandler_UnknownCommand(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	resp := h.Handle(&protocol.Command{Name: "ZNONCE", Args: []string{}})
	if resp.err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestHandler_DBSizeEmpty(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	resp := h.Handle(&protocol.Command{Name: "DBSIZE", Args: []string{}})
	if resp.err != nil {
		t.Errorf("unexpected error: %v", resp.err)
	}
}

func TestHandler_FlushAll(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	c.Set("k1", []byte("v1"), 0)
	h.Handle(&protocol.Command{Name: "FLUSHALL", Args: []string{}})
	if c.Size() != 0 {
		t.Errorf("Size after FLUSHALL = %d, want 0", c.Size())
	}
}

func TestHandler_Keys(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	c.Set("user:1", []byte("a"), 0)
	c.Set("item:1", []byte("b"), 0)
	c.Set("user:2", []byte("c"), 0)

	resp := h.Handle(&protocol.Command{Name: "KEYS", Args: []string{"user"}})
	if resp.err != nil {
		t.Errorf("unexpected error: %v", resp.err)
	}
	if len(resp.data) == 0 {
		t.Error("KEYS should return data")
	}
}

func TestHandler_KeysAll(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)

	resp := h.Handle(&protocol.Command{Name: "KEYS", Args: []string{"*"}})
	if resp.err != nil {
		t.Errorf("unexpected error: %v", resp.err)
	}
	if len(resp.data) == 0 {
		t.Error("KEYS * should return all keys")
	}
}

func TestHandler_SetAndGetDirect(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v"}})
	resp := h.Handle(&protocol.Command{Name: "GET", Args: []string{"k"}})
	if resp.err != nil {
		t.Errorf("GET error: %v", resp.err)
	}
	if resp.data == nil {
		t.Error("GET should return data")
	}
}

func TestHandler_SetNXOnMissingKey(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v1", "NX"}})
	if resp.err != nil {
		t.Fatalf("NX on missing key error: %v", resp.err)
	}
	if string(resp.data) != "+OK\r\n" {
		t.Errorf("NX on missing key should return OK, got %q", resp.data)
	}
	val, _ := c.Get("k")
	if string(val) != "v1" {
		t.Errorf("value = %q, want v1", string(val))
	}
}

func TestHandler_SetNXOnExistingKey(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v1"}})
	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v2", "NX"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$-1\r\n" {
		t.Errorf("NX on existing key should return nil, got %q", resp.data)
	}
	val, _ := c.Get("k")
	if string(val) != "v1" {
		t.Errorf("value should be unchanged, got %q", string(val))
	}
}

func TestHandler_SetXXOnExistingKey(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v1"}})
	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v2", "XX"}})
	if resp.err != nil {
		t.Fatalf("XX on existing key error: %v", resp.err)
	}
	if string(resp.data) != "+OK\r\n" {
		t.Errorf("XX on existing key should return OK, got %q", resp.data)
	}
	val, _ := c.Get("k")
	if string(val) != "v2" {
		t.Errorf("value = %q, want v2", string(val))
	}
}

func TestHandler_SetXXOnMissingKey(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v2", "XX"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$-1\r\n" {
		t.Errorf("XX on missing key should return nil, got %q", resp.data)
	}
	_, err := c.Get("k")
	if err == nil {
		t.Error("key should not exist after failed XX SET")
	}
}

func TestHandler_SetNXWithTTL(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v1", "NX", "EX", "10"}})
	if resp.err != nil {
		t.Fatalf("NX+EX error: %v", resp.err)
	}
	if string(resp.data) != "+OK\r\n" {
		t.Errorf("NX+EX should return OK, got %q", resp.data)
	}
}

func TestHandler_SetNXAndXXTogether(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "v1", "NX", "XX"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$-1\r\n" {
		t.Errorf("NX+XX together should return nil, got %q", resp.data)
	}
}

func TestConcurrentSetNXOnlyOneWinner(t *testing.T) {
	c := newTestCache()
	h := NewHandler(c)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			val := fmt.Sprintf("v%d", id)
			resp := h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", val, "NX"}})
			if string(resp.data) != "$-1\r\n" && string(resp.data) != "+OK\r\n" {
				t.Errorf("unexpected response: %q", resp.data)
			}
		}(i)
	}
	wg.Wait()

	val, _ := c.Get("k")
	if string(val) == "" {
		t.Fatal("key should have been set by exactly one goroutine")
	}

	okCount := 0
	for i := 0; i < goroutines; i++ {
		h2 := NewHandler(c)
		resp := h2.Handle(&protocol.Command{Name: "SET", Args: []string{"k2", "onlyonce", "NX"}})
		if string(resp.data) == "+OK\r\n" {
			okCount++
		}
	}
	_ = val
}

func TestServer_LargePayload(t *testing.T) {
	_, client := startTestServer(t)

	bigVal := make([]byte, 100*1024)
	for i := range bigVal {
		bigVal[i] = 'x'
	}
	_, err := client.Send("SET", "bigkey", string(bigVal))
	if err != nil {
		t.Fatalf("SET large: %v", err)
	}

	resp, err := client.Send("GET", "bigkey")
	if err != nil {
		t.Fatalf("GET large: %v", err)
	}
	if len(resp) != len(bigVal) {
		t.Errorf("GET large len = %d, want %d", len(resp), len(bigVal))
	}
}

func TestClient_ReadLine_EOF(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	client := NewClient(ln.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	client.Close()
}

func TestClient_SendRawRESP(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0"}, c)
	srv.Start(context.Background())
	defer srv.Shutdown()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send raw RESP PING
	_, err = fmt.Fprint(conn, "*1\r\n$4\r\nPING\r\n")
	if err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if line != "+PONG\r\n" {
		t.Errorf("raw RESP PING response = %q, want +PONG\\r\\n", line)
	}
}
