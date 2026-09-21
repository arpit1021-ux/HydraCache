package network

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// TestServer_MalformedNegativeBulkLengthDoesNotCrashServer is the
// end-to-end regression test for the parser crash bug: this exact
// payload used to reach make([]byte, strLen+2) with a negative length,
// which panics in Go and, before the server-level recover() and parser
// bounds were added, could take down the entire process from a single
// malformed request on any connection (parsing happens before the AUTH
// gate). The real proof isn't that this connection gets an error — it's
// that the SERVER is still alive and serving other connections
// afterward.
func TestServer_MalformedNegativeBulkLengthDoesNotCrashServer(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	addr := srv.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Write([]byte("*1\r\n$-5\r\n")); err != nil {
		t.Fatalf("write malformed request: %v", err)
	}
	buf := make([]byte, 256)
	_, _ = conn.Read(buf) // an error reply or EOF is fine; a hung/crashed process is not
	conn.Close()

	client := NewClient(addr)
	if err = client.Connect(); err != nil {
		t.Fatalf("server appears to have crashed: cannot connect after malformed request: %v", err)
	}
	defer client.Close()
	resp, err := client.Send("PING")
	if err != nil || resp != "PONG" {
		t.Fatalf("server appears unhealthy after malformed request: resp=%q err=%v", resp, err)
	}
}

func TestServer_MaxBulkBytesRejectsOversizedValue(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10, MaxBulkBytes: 16}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	addr := srv.Addr().String()

	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := client.Send("SET", "k", "this-value-is-longer-than-sixteen-bytes"); err == nil {
		t.Error("expected an oversized bulk string to be rejected")
	}
	client.Close()

	// The server itself must still be healthy — a fresh connection works.
	client2 := NewClient(addr)
	if err := client2.Connect(); err != nil {
		t.Fatalf("connect2: %v", err)
	}
	defer client2.Close()
	if resp, err := client2.Send("PING"); err != nil || resp != "PONG" {
		t.Fatalf("server unhealthy after oversized-bulk rejection: resp=%q err=%v", resp, err)
	}
}

func TestServer_ReadTimeoutDisconnectsIdleClient(t *testing.T) {
	srv := NewServer(ServerConfig{
		Addr:         "127.0.0.1:0",
		MaxConns:     10,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send nothing; the server's ReadTimeout must eventually close the
	// connection rather than holding it (and its MaxConns slot) open
	// indefinitely. The server writes an error response before closing
	// (a read-timeout error isn't io.EOF, so it takes the "write an
	// error, then close" path), so the first Read can legitimately
	// succeed with that message — what actually proves the connection
	// was closed is a subsequent Read failing.
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 256)
	closed := false
	for i := 0; i < 5; i++ {
		if _, err := conn.Read(buf); err != nil {
			closed = true
			break
		}
	}
	if !closed {
		t.Fatal("expected the idle connection to eventually be closed by the server's read timeout")
	}
}

// TestServer_MaxConnsThrottlesAcceptance proves MaxConns bounds actual
// admission, not just a post-accept formality: with MaxConns=1, a second
// TCP connection may still complete its handshake (the kernel's listen
// backlog does that independent of the application calling Accept), but
// the server's accept loop must not service it — no response to
// anything it sends — until the first connection's slot is freed.
func TestServer_MaxConnsThrottlesAcceptance(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 1}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	addr := srv.Addr().String()

	connA, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer connA.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv.ConnectionCount() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.ConnectionCount() != 1 {
		t.Fatalf("expected ConnectionCount()==1 once connA is accepted, got %d", srv.ConnectionCount())
	}

	connB, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer connB.Close()

	_ = connB.SetDeadline(time.Now().Add(200 * time.Millisecond))
	if _, werr := connB.Write([]byte("*1\r\n$4\r\nPING\r\n")); werr == nil {
		buf := make([]byte, 8)
		if _, rerr := connB.Read(buf); rerr == nil {
			t.Fatal("expected connB to get no response while the only MaxConns slot is held by connA")
		}
	}

	// Free the slot — connB must now be served.
	connA.Close()

	_ = connB.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := connB.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("write to connB after freeing the slot: %v", err)
	}
	buf := make([]byte, 7)
	if _, err := io.ReadFull(connB, buf); err != nil {
		t.Fatalf("expected connB to be served once connA's slot freed up: %v", err)
	}
	if string(buf) != "+PONG\r\n" {
		t.Errorf("connB response = %q, want +PONG\\r\\n", buf)
	}
}

// TestServer_ShutdownWithTimeout_ForceClosesStuckIdleConnection proves
// Shutdown is bounded: an idle connection sitting in a blocking read
// (with a long ReadTimeout, simulating a real deployment) must not make
// Shutdown wait for that timeout to elapse — it gets force-closed once
// the drain grace period passes, so shutdown's own duration is
// predictable regardless of what a connection happens to be doing.
func TestServer_ShutdownWithTimeout_ForceClosesStuckIdleConnection(t *testing.T) {
	srv := NewServer(ServerConfig{
		Addr:         "127.0.0.1:0",
		MaxConns:     10,
		ReadTimeout:  time.Minute, // deliberately much longer than the drain timeout below
		WriteTimeout: time.Minute,
	}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv.ConnectionCount() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.ConnectionCount() != 1 {
		t.Fatalf("expected the idle connection to be accepted, ConnectionCount()=%d", srv.ConnectionCount())
	}

	start := time.Now()
	srv.ShutdownWithTimeout(200 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %v — expected it to be bounded by the drain timeout, not the connection's 1-minute ReadTimeout", elapsed)
	}
}

// TestServer_ShutdownWithTimeout_ReturnsPromptlyWithNoStuckConnections is
// a regression guard: when every connection is already idle-but-closable
// (or there are none), Shutdown must return quickly rather than always
// waiting out the full drain timeout.
func TestServer_ShutdownWithTimeout_ReturnsPromptlyWithNoStuckConnections(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	start := time.Now()
	srv.ShutdownWithTimeout(5 * time.Second)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Errorf("Shutdown with no connections took %v — expected it to return promptly, not wait out the drain timeout", elapsed)
	}
}
