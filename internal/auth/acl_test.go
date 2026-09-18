package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestNew_RequiresAtLeastOneUser(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected an error with zero users")
	}
}

func TestNew_RejectsEmptyUsername(t *testing.T) {
	_, err := New([]User{{Username: "", Password: "p", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err == nil {
		t.Fatal("expected an error for empty username")
	}
}

func TestNew_RejectsDuplicateUsername(t *testing.T) {
	u := User{Username: "alice", Password: "p", Commands: []string{"*"}, KeyPatterns: []string{"*"}}
	_, err := New([]User{u, u})
	if err == nil {
		t.Fatal("expected an error for duplicate username")
	}
}

func TestNew_RejectsNoCommandsOrNoKeyPatterns(t *testing.T) {
	if _, err := New([]User{{Username: "a", Password: "p", KeyPatterns: []string{"*"}}}); err == nil {
		t.Fatal("expected an error when Commands is empty")
	}
	if _, err := New([]User{{Username: "a", Password: "p", Commands: []string{"*"}}}); err == nil {
		t.Fatal("expected an error when KeyPatterns is empty")
	}
}

func TestNew_RejectsInvalidSHA256Hash(t *testing.T) {
	_, err := New([]User{{Username: "a", Password: "sha256:not-hex", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err == nil {
		t.Fatal("expected an error for a malformed sha256 password hash")
	}
}

func TestAuthenticate_PlaintextPassword(t *testing.T) {
	acl, err := New([]User{{Username: "alice", Password: "s3cret", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Authenticate("alice", "s3cret") {
		t.Error("expected correct plaintext password to authenticate")
	}
	if acl.Authenticate("alice", "wrong") {
		t.Error("expected wrong password to be rejected")
	}
	if acl.Authenticate("bob", "s3cret") {
		t.Error("expected unknown user to be rejected")
	}
}

func TestAuthenticate_CaseInsensitiveUsername(t *testing.T) {
	acl, err := New([]User{{Username: "Alice", Password: "s3cret", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Authenticate("alice", "s3cret") {
		t.Error("expected username matching to be case-insensitive")
	}
	if !acl.Authenticate("ALICE", "s3cret") {
		t.Error("expected username matching to be case-insensitive")
	}
}

func TestAuthenticate_SHA256HashedPassword(t *testing.T) {
	sum := sha256.Sum256([]byte("s3cret"))
	hash := "sha256:" + hex.EncodeToString(sum[:])
	acl, err := New([]User{{Username: "alice", Password: hash, Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Authenticate("alice", "s3cret") {
		t.Error("expected the plaintext matching the stored hash to authenticate")
	}
	if acl.Authenticate("alice", "wrong") {
		t.Error("expected a wrong plaintext to be rejected against the hash")
	}
}

func TestAllowed_CommandAllowList(t *testing.T) {
	acl, err := New([]User{{
		Username:    "readonly",
		Password:    "p",
		Commands:    []string{"GET", "EXISTS"},
		KeyPatterns: []string{"*"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Allowed("readonly", "GET", "k") {
		t.Error("expected GET to be allowed")
	}
	if !acl.Allowed("readonly", "get", "k") {
		t.Error("expected command matching to be case-insensitive")
	}
	if acl.Allowed("readonly", "SET", "k") {
		t.Error("expected SET to be denied for a read-only user")
	}
}

func TestAllowed_WildcardCommand(t *testing.T) {
	acl, err := New([]User{{Username: "admin", Password: "p", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Allowed("admin", "FLUSHALL", "") {
		t.Error("expected wildcard command permission to allow everything")
	}
}

func TestAllowed_KeyPatternScoping(t *testing.T) {
	acl, err := New([]User{{
		Username:    "session-svc",
		Password:    "p",
		Commands:    []string{"*"},
		KeyPatterns: []string{"session:*"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Allowed("session-svc", "GET", "session:abc123") {
		t.Error("expected a key matching the pattern to be allowed")
	}
	if acl.Allowed("session-svc", "GET", "other:abc123") {
		t.Error("expected a key NOT matching the pattern to be denied")
	}
}

func TestAllowed_NoKeyArgumentBypassesKeyPatternCheck(t *testing.T) {
	acl, err := New([]User{{
		Username:    "svc",
		Password:    "p",
		Commands:    []string{"DBSIZE"},
		KeyPatterns: []string{"session:*"}, // deliberately narrow
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Allowed("svc", "DBSIZE", "") {
		t.Error("expected a no-key command to be gated by Commands alone, not KeyPatterns")
	}
}

func TestAllowed_UnknownUserDenied(t *testing.T) {
	acl, err := New([]User{{Username: "a", Password: "p", Commands: []string{"*"}, KeyPatterns: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	if acl.Allowed("ghost", "GET", "k") {
		t.Error("expected an unconfigured user to be denied")
	}
}
