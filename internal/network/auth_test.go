package network

import (
	"context"
	"testing"

	"github.com/hydracache/hydracache/internal/auth"
	"github.com/hydracache/hydracache/internal/protocol"
)

func newTestACL(t *testing.T, users ...auth.User) *auth.ACL {
	t.Helper()
	acl, err := auth.New(users)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	return acl
}

func TestHandleAuthenticated_NoACLConfiguredBehavesLikeHandle(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "PING"}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
}

func TestHandleAuthenticated_RequiresAuthFirst(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "GET", Args: []string{"k"}}, sess)
	if resp.err == nil {
		t.Fatal("expected NOAUTH before authenticating")
	}
}

func TestHandleAuthenticated_WrongPasswordRejected(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"wrong"}}, sess)
	if resp.err == nil {
		t.Fatal("expected WRONGPASS error")
	}
	if sess.authenticated {
		t.Error("session must not be marked authenticated after a failed AUTH")
	}
}

func TestHandleAuthenticated_CorrectPasswordGrantsAccess(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	authResp := h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"s3cret"}}, sess)
	if authResp.err != nil {
		t.Fatalf("unexpected AUTH error: %v", authResp.err)
	}
	if !sess.authenticated {
		t.Fatal("expected session to be authenticated after correct AUTH")
	}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "SET", Args: []string{"k", "v"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error after auth: %v", resp.err)
	}
}

func TestHandleAuthenticated_TwoArgAuthSelectsUser(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "alice", Password: "p",
		Commands: []string{"GET"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"alice", "p"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if sess.username != "alice" {
		t.Errorf("session username = %q, want alice", sess.username)
	}
}

func TestHandleAuthenticated_CommandNotInACLDenied(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "readonly", Password: "p",
		Commands: []string{"GET"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}
	h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"readonly", "p"}}, sess)

	resp := h.HandleAuthenticated(&protocol.Command{Name: "SET", Args: []string{"k", "v"}}, sess)
	if resp.err == nil {
		t.Fatal("expected NOPERM for a command not in the user's allow-list")
	}
}

func TestHandleAuthenticated_KeyPatternDenied(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "svc", Password: "p",
		Commands: []string{"*"}, KeyPatterns: []string{"session:*"},
	}))
	sess := &Session{}
	h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"svc", "p"}}, sess)

	allowed := h.HandleAuthenticated(&protocol.Command{Name: "GET", Args: []string{"session:1"}}, sess)
	if allowed.err != nil {
		t.Errorf("expected a matching key to be allowed: %v", allowed.err)
	}

	denied := h.HandleAuthenticated(&protocol.Command{Name: "GET", Args: []string{"other:1"}}, sess)
	if denied.err == nil {
		t.Fatal("expected NOPERM for a key outside the user's allowed patterns")
	}
}

func TestHandleAuthenticated_AuthItselfNeverRequiresPriorAuth(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "p",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}
	// AUTH must always be reachable pre-authentication — otherwise no
	// client could ever authenticate at all.
	resp := h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"p"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
}

// TestHandleAuthenticated_InterNodeCommandsExemptFromClientACL proves
// enabling AUTH doesn't break clustering itself: a peer connection never
// calls AUTH, so gossip/replication/election RPCs must keep working on an
// unauthenticated session once an ACL is configured.
func TestHandleAuthenticated_InterNodeCommandsExemptFromClientACL(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{} // never authenticated, exactly like a real peer connection

	for _, cmd := range []string{"GOSSIP", "REPLICATE", "REPLICA_SYNC", "ELECTION_VOTE", "ELECTION_HEARTBEAT"} {
		resp := h.HandleAuthenticated(&protocol.Command{Name: cmd, Args: []string{"{}"}}, sess)
		if resp.err != nil && resp.err.Error() == "NOAUTH Authentication required" {
			t.Errorf("%s must be exempt from client AUTH, got NOAUTH", cmd)
		}
	}
}

// TestServer_AuthEndToEnd exercises the whole path over a real TCP
// connection: unauthenticated commands are rejected, AUTH succeeds, and
// the connection stays authenticated for its lifetime (a NEW connection
// starts unauthenticated again).
func TestServer_AuthEndToEnd(t *testing.T) {
	c := newTestCache()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, c)
	srv.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	addr := srv.Addr().String()

	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if _, err := client.Send("GET", "k"); err == nil {
		t.Fatal("expected NOAUTH before authenticating")
	}

	if resp, err := client.Send("AUTH", "s3cret"); err != nil || resp != "OK" {
		t.Fatalf("AUTH failed: resp=%q err=%v", resp, err)
	}

	if _, err := client.Send("SET", "k", "v"); err != nil {
		t.Fatalf("SET after auth: %v", err)
	}

	// A fresh connection must NOT inherit the first one's auth state.
	client2 := NewClient(addr)
	if err := client2.Connect(); err != nil {
		t.Fatalf("connect2: %v", err)
	}
	defer client2.Close()
	if _, err := client2.Send("GET", "k"); err == nil {
		t.Fatal("expected a new connection to start unauthenticated")
	}
}
