package network

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// startGoRedisTestServer starts a real HydraCache TCP server and returns a
// real go-redis client connected to it — proving compatibility against an
// actual, independent Redis client implementation rather than against
// HydraCache's own hand-rolled test Client, which necessarily only
// exercises whatever subset of RESP it already knows how to parse.
func startGoRedisTestServer(t *testing.T) *redis.Client {
	t.Helper()
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 100}, c)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	client := redis.NewClient(&redis.Options{
		Addr:        srv.Addr().String(),
		Protocol:    2, // HydraCache only speaks RESP2 — see COMMANDS.md.
		DialTimeout: 2 * time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestGoRedisCompat_Ping(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
}

func TestGoRedisCompat_SetGet(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	val, err := client.Get(ctx, "greeting").Result()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if val != "hello" {
		t.Errorf("GET = %q, want %q", val, "hello")
	}
}

func TestGoRedisCompat_GetMissReturnsRedisNil(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	_, err := client.Get(ctx, "does-not-exist").Result()
	// go-redis represents a RESP2 nil bulk string as the sentinel
	// redis.Nil error, not an empty string — this is the exact behavior
	// every go-redis-based application's cache-miss branch depends on.
	if err != redis.Nil {
		t.Errorf("GET miss error = %v, want redis.Nil", err)
	}
}

func TestGoRedisCompat_SetWithExpiry(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	if err := client.Set(ctx, "session", "token", 30*time.Second).Err(); err != nil {
		t.Fatalf("SET with TTL: %v", err)
	}
	ttl, err := client.TTL(ctx, "session").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Errorf("TTL = %v, want a positive duration <= 30s", ttl)
	}
}

func TestGoRedisCompat_SetNX(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	first, err := client.SetNX(ctx, "lock", "holder-1", 0).Result()
	if err != nil {
		t.Fatalf("first SetNX: %v", err)
	}
	if !first {
		t.Fatal("expected the first SETNX on an absent key to succeed")
	}

	second, err := client.SetNX(ctx, "lock", "holder-2", 0).Result()
	if err != nil {
		t.Fatalf("second SetNX: %v", err)
	}
	if second {
		t.Fatal("expected SETNX against an already-held key to fail")
	}

	val, err := client.Get(ctx, "lock").Result()
	if err != nil || val != "holder-1" {
		t.Errorf("lock value = %q, err=%v, want %q held by the first writer", val, err, "holder-1")
	}
}

func TestGoRedisCompat_SetXX(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	onMissing, err := client.SetXX(ctx, "absent", "v", 0).Result()
	if err != nil {
		t.Fatalf("SetXX on absent key: %v", err)
	}
	if onMissing {
		t.Fatal("expected SETXX against a missing key to fail")
	}

	if err = client.Set(ctx, "present", "old", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}
	onPresent, err := client.SetXX(ctx, "present", "new", 0).Result()
	if err != nil {
		t.Fatalf("SetXX on present key: %v", err)
	}
	if !onPresent {
		t.Fatal("expected SETXX against an existing key to succeed")
	}
}

func TestGoRedisCompat_Del(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	client.Set(ctx, "a", "1", 0)
	client.Set(ctx, "b", "2", 0)

	n, err := client.Del(ctx, "a", "b", "nonexistent").Result()
	if err != nil {
		t.Fatalf("DEL: %v", err)
	}
	if n != 2 {
		t.Errorf("DEL deleted count = %d, want 2", n)
	}
}

func TestGoRedisCompat_Exists(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	client.Set(ctx, "here", "v", 0)
	n, err := client.Exists(ctx, "here", "not-here").Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if n != 1 {
		t.Errorf("EXISTS count = %d, want 1", n)
	}
}

func TestGoRedisCompat_Expire(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	client.Set(ctx, "k", "v", 0)
	ok, err := client.Expire(ctx, "k", 60*time.Second).Result()
	if err != nil {
		t.Fatalf("EXPIRE: %v", err)
	}
	if !ok {
		t.Fatal("expected EXPIRE on an existing key to succeed")
	}
}

// TestGoRedisCompat_UnknownCommandError proves go-redis can parse
// HydraCache's error replies without choking — it surfaces them as a Go
// error from the command's Err() rather than panicking or hanging, which
// only works because our errors are well-formed RESP error replies
// (leading '-', no embedded CR/LF).
func TestGoRedisCompat_UnknownCommandError(t *testing.T) {
	client := startGoRedisTestServer(t)
	ctx := context.Background()

	err := client.Do(ctx, "NOSUCHCOMMAND").Err()
	if err == nil {
		t.Fatal("expected an error for an unknown command")
	}
}

// TestGoRedisCompat_ConnectsWithDefaultProtocolNegotiation proves the
// documented behavior in COMMANDS.md: a go-redis client left on its
// default settings (which requests RESP3 via HELLO) still connects and
// works, because go-redis falls back gracefully to legacy AUTH/RESP2 when
// HELLO fails — HydraCache doesn't need to implement RESP3 for this to
// work, it only needs to fail HELLO 3 honestly instead of hanging or
// corrupting the connection.
func TestGoRedisCompat_ConnectsWithDefaultProtocolNegotiation(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 100}, c)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	// Deliberately NOT setting Protocol: 2 here — this is go-redis's
	// out-of-the-box default (RESP3), which HydraCache's HELLO refuses.
	client := redis.NewClient(&redis.Options{
		Addr:        srv.Addr().String(),
		DialTimeout: 2 * time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET with default (RESP3-requesting) go-redis client: %v", err)
	}
	val, err := client.Get(ctx, "k").Result()
	if err != nil || val != "v" {
		t.Fatalf("GET after fallback: val=%q err=%v", val, err)
	}
}
