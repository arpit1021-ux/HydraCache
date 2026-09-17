package network

import (
	"context"
	"testing"

	"github.com/hydracache/hydracache/internal/protocol"
)

// staticMigrationChecker is a minimal MigrationChecker for testing the
// handler's redirect behavior in isolation from cluster.Manager.
type staticMigrationChecker struct {
	redirects map[string]string
}

func (c *staticMigrationChecker) RedirectTarget(key string) (string, bool) {
	addr, ok := c.redirects[key]
	return addr, ok
}

func TestHandler_GetRedirectsMigratedKeyToNewOwner(t *testing.T) {
	newOwnerCache := newTestCache()
	newOwnerCache.Set("moved", []byte("value-on-new-owner"), 0)

	newOwnerSrv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, newOwnerCache)
	if err := newOwnerSrv.Start(context.Background()); err != nil {
		t.Fatalf("new owner server start: %v", err)
	}
	t.Cleanup(newOwnerSrv.Shutdown)

	oldOwnerCache := newTestCache() // does NOT have "moved" — it was migrated away
	h := NewHandler(oldOwnerCache)
	h.SetMigrationChecker(&staticMigrationChecker{
		redirects: map[string]string{"moved": newOwnerSrv.Addr().String()},
	})

	resp := h.Handle(&protocol.Command{Name: "GET", Args: []string{"moved"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$18\r\nvalue-on-new-owner\r\n" {
		t.Errorf("response = %q, want the redirected value from the new owner", resp.data)
	}
}

func TestHandler_GetFalseMissWithoutMigrationChecker(t *testing.T) {
	// Regression guard: a plain miss (no migration checker configured at
	// all) must behave exactly as before — a normal nil bulk reply, not
	// an error or a panic.
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "GET", Args: []string{"nope"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$-1\r\n" {
		t.Errorf("response = %q, want a plain nil bulk reply", resp.data)
	}
}

func TestHandler_GetRealMissWhenRedirectTargetUnreachable(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetMigrationChecker(&staticMigrationChecker{
		redirects: map[string]string{"moved": "127.0.0.1:1"}, // nothing listens here
	})

	resp := h.Handle(&protocol.Command{Name: "GET", Args: []string{"moved"}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if string(resp.data) != "$-1\r\n" {
		t.Errorf("response = %q, want a fallback miss when the redirect target is unreachable", resp.data)
	}
}
