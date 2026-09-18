// Package auth implements per-connection AUTH and a flat, user-scoped ACL:
// which commands and key patterns a given user's connection may touch.
// This is deliberately not full Redis ACL syntax (categories, +/- toggles,
// selectors) — a flat allow-list per user is enough until a real
// multi-tenant use case demands more, and a smaller surface is easier to
// audit for a security-relevant subsystem.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"sync"
)

// User is one ACL entry: a set of credentials plus what that identity is
// allowed to do once authenticated.
type User struct {
	Username string
	// Password is either a plaintext secret or, prefixed with "sha256:",
	// a hex-encoded SHA-256 hash of the secret — so an operator who
	// doesn't want a plaintext credential sitting in a config file has
	// an option, at the cost of the same offline-brute-force exposure
	// SHA-256 always has for low-entropy secrets. This mirrors Redis's
	// own ACL password hashing (also plain SHA-256), not a claim that
	// it's suitable for high-value end-user passwords.
	Password string
	// Commands lists allowed command names (case-insensitive), or ["*"]
	// for all commands.
	Commands []string
	// KeyPatterns lists glob patterns (path.Match syntax: *, ?, [...])
	// a command's key argument must match, or ["*"] for all keys.
	// Commands with no natural single key argument (e.g. FLUSHALL,
	// DBSIZE, PING) are gated by Commands alone; KeyPatterns doesn't
	// apply to them.
	KeyPatterns []string
}

// ACL holds every configured user and answers authentication and
// authorization questions for a connection.
type ACL struct {
	mu    sync.RWMutex
	users map[string]resolvedUser
}

type resolvedUser struct {
	username    string
	password    string // raw config value, compared per Authenticate's rule
	hashed      bool
	commands    map[string]bool // lowercase command name -> allowed; "*" key means all
	allowAll    bool
	keyPatterns []string
	allowAllKey bool
}

// New builds an ACL from a set of users. Returns an error if any user has
// an empty username, a duplicate username, or no commands/key patterns
// configured at all (a user that can authenticate but do nothing is
// almost certainly a config mistake, not an intentional lockout — an
// operator who wants that should still list at least one explicit,
// harmless command like PING).
func New(users []User) (*ACL, error) {
	if len(users) == 0 {
		return nil, fmt.Errorf("auth: at least one user is required when auth is enabled")
	}

	resolved := make(map[string]resolvedUser, len(users))
	for _, u := range users {
		if u.Username == "" {
			return nil, fmt.Errorf("auth: user has an empty username")
		}
		key := strings.ToLower(u.Username)
		if _, exists := resolved[key]; exists {
			return nil, fmt.Errorf("auth: duplicate username %q", u.Username)
		}
		if len(u.Commands) == 0 {
			return nil, fmt.Errorf("auth: user %q has no allowed commands configured", u.Username)
		}
		if len(u.KeyPatterns) == 0 {
			return nil, fmt.Errorf("auth: user %q has no allowed key patterns configured", u.Username)
		}

		password := u.Password
		hashed := false
		if strings.HasPrefix(password, "sha256:") {
			hashed = true
			password = strings.ToLower(strings.TrimPrefix(password, "sha256:"))
			if _, err := hex.DecodeString(password); err != nil {
				return nil, fmt.Errorf("auth: user %q has an invalid sha256 password hash: %w", u.Username, err)
			}
		}

		commands := make(map[string]bool, len(u.Commands))
		allowAll := false
		for _, c := range u.Commands {
			if c == "*" {
				allowAll = true
				continue
			}
			commands[strings.ToLower(c)] = true
		}

		allowAllKey := false
		for _, p := range u.KeyPatterns {
			if p == "*" {
				allowAllKey = true
				break
			}
		}

		resolved[key] = resolvedUser{
			username:    u.Username,
			password:    password,
			hashed:      hashed,
			commands:    commands,
			allowAll:    allowAll,
			keyPatterns: u.KeyPatterns,
			allowAllKey: allowAllKey,
		}
	}

	return &ACL{users: resolved}, nil
}

// Authenticate reports whether username/password match a configured user.
// Uses a constant-time comparison so response timing doesn't leak how
// many leading bytes of a guessed password were correct.
func (a *ACL) Authenticate(username, password string) bool {
	a.mu.RLock()
	u, ok := a.users[strings.ToLower(username)]
	a.mu.RUnlock()
	if !ok {
		return false
	}

	candidate := password
	if u.hashed {
		sum := sha256.Sum256([]byte(password))
		candidate = hex.EncodeToString(sum[:])
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(u.password)) == 1
}

// Allowed reports whether username may run command against key. key is
// ignored for commands that don't take one — pass "" and it's treated as
// always matching (command-only gating).
func (a *ACL) Allowed(username, command, key string) bool {
	a.mu.RLock()
	u, ok := a.users[strings.ToLower(username)]
	a.mu.RUnlock()
	if !ok {
		return false
	}

	if !u.allowAll && !u.commands[strings.ToLower(command)] {
		return false
	}
	if key == "" || u.allowAllKey {
		return true
	}
	for _, pattern := range u.keyPatterns {
		if matched, _ := path.Match(pattern, key); matched {
			return true
		}
	}
	return false
}

// HasUser reports whether username is configured at all — used to give a
// clear "unknown user" vs "wrong password" distinction is deliberately
// NOT exposed to clients (that would let an attacker enumerate valid
// usernames), but is useful for startup diagnostics/tests.
func (a *ACL) HasUser(username string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.users[strings.ToLower(username)]
	return ok
}
