package network

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hydracache/hydracache/internal/auth"
	"github.com/hydracache/hydracache/internal/protocol"
)

// decodeResponse parses a successful Response's raw wire bytes the same
// way a real client would, so these tests assert on the actual RESP
// structure rather than trusting the byte-slice the handler built.
func decodeResponse(t *testing.T, resp *Response) *protocol.Response {
	t.Helper()
	if resp.err != nil {
		t.Fatalf("unexpected error response: %v", resp.err)
	}
	decoded, err := protocol.NewDecoder(bytes.NewReader(resp.data)).Decode()
	if err != nil {
		t.Fatalf("response did not decode as valid RESP: %v (raw: %q)", err, resp.data)
	}
	return decoded
}

func TestHandle_SETNX_SetsWhenAbsentAndReturnsIntegerOne(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "SETNX", Args: []string{"k", "v"}})
	decoded := decodeResponse(t, resp)
	if decoded.Type != protocol.ResponseInteger || decoded.Integer != 1 {
		t.Fatalf("SETNX on absent key = %v, want integer 1", decoded)
	}
}

func TestHandle_SETNX_NoOpWhenPresentAndReturnsIntegerZero(t *testing.T) {
	h := NewHandler(newTestCache())
	h.Handle(&protocol.Command{Name: "SET", Args: []string{"k", "first"}})

	resp := h.Handle(&protocol.Command{Name: "SETNX", Args: []string{"k", "second"}})
	decoded := decodeResponse(t, resp)
	if decoded.Type != protocol.ResponseInteger || decoded.Integer != 0 {
		t.Fatalf("SETNX on existing key = %v, want integer 0", decoded)
	}

	getResp := h.Handle(&protocol.Command{Name: "GET", Args: []string{"k"}})
	got := decodeResponse(t, getResp)
	if string(got.Data) != "first" {
		t.Errorf("value after failed SETNX = %q, want unchanged %q", got.Data, "first")
	}
}

func TestValidateCommand_UnknownCommandIsERRPrefixed(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "NOSUCHCOMMAND"})
	if resp.err == nil {
		t.Fatal("expected an error")
	}
	if !strings.HasPrefix(resp.err.Error(), "ERR ") {
		t.Errorf("error = %q, want ERR-prefixed to match Redis convention", resp.err.Error())
	}
}

func TestValidateCommand_WrongArityIsERRPrefixedAndLowercasesCommand(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "GET"}) // GET requires 1 arg
	if resp.err == nil {
		t.Fatal("expected an error")
	}
	want := "ERR wrong number of arguments for 'get' command"
	if resp.err.Error() != want {
		t.Errorf("error = %q, want %q", resp.err.Error(), want)
	}
}

// --- HELLO ---

func TestHandleAuthenticated_HELLO_NoArgsDefaultsToProto2(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{id: 7}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO"}, sess)
	decoded := decodeResponse(t, resp)
	if decoded.Type != protocol.ResponseArray {
		t.Fatalf("expected an array reply, got type %v", decoded.Type)
	}

	fields := map[string]*protocol.Response{}
	for i := 0; i+1 < len(decoded.Items); i += 2 {
		fields[string(decoded.Items[i].Data)] = decoded.Items[i+1]
	}
	if server := fields["server"]; server == nil || string(server.Data) != "hydracache" {
		t.Errorf("server field = %v, want bulk string %q", server, "hydracache")
	}
	if proto := fields["proto"]; proto == nil || proto.Integer != 2 {
		t.Errorf("proto field = %v, want integer 2", proto)
	}
	if id := fields["id"]; id == nil || id.Integer != 7 {
		t.Errorf("id field = %v, want integer 7 (the session's connection id)", id)
	}
}

func TestHandleAuthenticated_HELLO_Proto3IsHonestlyRejected(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"3"}}, sess)
	if resp.err == nil {
		t.Fatal("expected NOPROTO: RESP3 is not implemented, so claiming to support it would be dishonest")
	}
	if !strings.HasPrefix(resp.err.Error(), "NOPROTO") {
		t.Errorf("error = %q, want NOPROTO-prefixed", resp.err.Error())
	}
}

func TestHandleAuthenticated_HELLO_ReachableWithoutACLConfigured(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"2"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
}

func TestHandleAuthenticated_HELLO_RequiresAuthWhenACLConfigured(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"2"}}, sess)
	if resp.err == nil {
		t.Fatal("expected NOAUTH: HELLO without an inline AUTH clause on an unauthenticated session must not silently succeed")
	}
	if !strings.HasPrefix(resp.err.Error(), "NOAUTH") {
		t.Errorf("error = %q, want NOAUTH-prefixed", resp.err.Error())
	}
}

func TestHandleAuthenticated_HELLO_InlineAuthAuthenticatesSession(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"2", "AUTH", "default", "s3cret"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if !sess.authenticated {
		t.Fatal("expected HELLO's inline AUTH clause to authenticate the session")
	}

	// And the session should now be able to run ordinary commands.
	if setResp := h.HandleAuthenticated(&protocol.Command{Name: "SET", Args: []string{"k", "v"}}, sess); setResp.err != nil {
		t.Fatalf("unexpected error after HELLO AUTH: %v", setResp.err)
	}
}

func TestHandleAuthenticated_HELLO_InlineAuthWrongPassword(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}

	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"2", "AUTH", "default", "wrong"}}, sess)
	if resp.err == nil {
		t.Fatal("expected WRONGPASS")
	}
	if !strings.HasPrefix(resp.err.Error(), "WRONGPASS") {
		t.Errorf("error = %q, want WRONGPASS-prefixed", resp.err.Error())
	}
}

func TestHandleAuthenticated_HELLO_SetNameSetsSessionName(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "HELLO", Args: []string{"2", "SETNAME", "myapp"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if sess.name != "myapp" {
		t.Errorf("sess.name = %q, want %q", sess.name, "myapp")
	}
}

// --- AUTH without ACL configured ---

func TestHandleAuthenticated_AUTHWithoutACLReturnsHonestError(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "AUTH", Args: []string{"anything"}}, sess)
	if resp.err == nil {
		t.Fatal("expected an error: AUTH with no password configured must not silently succeed")
	}
	if !strings.Contains(resp.err.Error(), "no password is set") {
		t.Errorf("error = %q, want the real Redis 'no password is set' message", resp.err.Error())
	}
}

// --- CLIENT ---

func TestHandleAuthenticated_CLIENT_GetSetName(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}

	if resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"SETNAME", "worker-1"}}, sess); resp.err != nil {
		t.Fatalf("SETNAME: unexpected error: %v", resp.err)
	}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"GETNAME"}}, sess)
	decoded := decodeResponse(t, resp)
	if string(decoded.Data) != "worker-1" {
		t.Errorf("GETNAME = %q, want %q", decoded.Data, "worker-1")
	}
}

func TestHandleAuthenticated_CLIENT_SetNameRejectsSpaces(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"SETNAME", "has space"}}, sess)
	if resp.err == nil {
		t.Fatal("expected an error for a client name containing spaces")
	}
}

func TestHandleAuthenticated_CLIENT_ID(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{id: 42}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"ID"}}, sess)
	decoded := decodeResponse(t, resp)
	if decoded.Integer != 42 {
		t.Errorf("CLIENT ID = %d, want 42", decoded.Integer)
	}
}

func TestHandleAuthenticated_CLIENT_SetInfo(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"SETINFO", "lib-name", "go-redis"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if sess.libName != "go-redis" {
		t.Errorf("sess.libName = %q, want %q", sess.libName, "go-redis")
	}
}

func TestHandleAuthenticated_CLIENT_SetInfoRejectsUnknownAttribute(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"SETINFO", "bogus-attr", "x"}}, sess)
	if resp.err == nil {
		t.Fatal("expected an error for an unrecognized CLIENT SETINFO attribute")
	}
}

func TestHandleAuthenticated_CLIENT_UnknownSubcommandIsHonestError(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{}
	// CLIENT LIST/KILL/PAUSE etc. are deliberately not implemented as
	// fake no-ops — they must fail loudly, not silently pretend to work.
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"LIST"}}, sess)
	if resp.err == nil {
		t.Fatal("expected an error for an unimplemented CLIENT subcommand")
	}
}

func TestHandleAuthenticated_CLIENT_WorksWithoutACLConfigured(t *testing.T) {
	h := NewHandler(newTestCache())
	sess := &Session{id: 1}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"ID"}}, sess)
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
}

func TestHandleAuthenticated_CLIENT_RequiresAuthWhenACLConfigured(t *testing.T) {
	h := NewHandler(newTestCache())
	h.SetAuth(newTestACL(t, auth.User{
		Username: "default", Password: "s3cret",
		Commands: []string{"*"}, KeyPatterns: []string{"*"},
	}))
	sess := &Session{}
	resp := h.HandleAuthenticated(&protocol.Command{Name: "CLIENT", Args: []string{"ID"}}, sess)
	if resp.err == nil {
		t.Fatal("expected NOAUTH for CLIENT on an unauthenticated session once ACL is configured")
	}
}

// --- CLUSTER ---

func TestHandle_CLUSTER_Info(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "CLUSTER", Args: []string{"INFO"}})
	decoded := decodeResponse(t, resp)
	if !strings.Contains(string(decoded.Data), "cluster_enabled:0") {
		t.Errorf("CLUSTER INFO = %q, want it to honestly report cluster_enabled:0 (no Redis Cluster protocol support)", decoded.Data)
	}
}

func TestHandle_CLUSTER_MyID(t *testing.T) {
	h := &Handler{cache: newTestCache(), nodeID: "node-abc"}
	resp := h.Handle(&protocol.Command{Name: "CLUSTER", Args: []string{"MYID"}})
	decoded := decodeResponse(t, resp)
	if string(decoded.Data) != "node-abc" {
		t.Errorf("CLUSTER MYID = %q, want %q", decoded.Data, "node-abc")
	}
}

func TestHandle_CLUSTER_UnsupportedSubcommandExplainsWhy(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "CLUSTER", Args: []string{"SLOTS"}})
	if resp.err == nil {
		t.Fatal("expected CLUSTER SLOTS to be refused: our consistent-hashing shards don't map to Redis Cluster slot ranges")
	}
	if !strings.Contains(resp.err.Error(), "does not speak the Redis Cluster protocol") {
		t.Errorf("error = %q, want an explanation rather than a bare 'unknown command'", resp.err.Error())
	}
}
