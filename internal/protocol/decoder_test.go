package protocol

import (
	"strings"
	"testing"
)

func TestDecodeSimpleString(t *testing.T) {
	r := strings.NewReader("+OK\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseSimpleString {
		t.Fatalf("expected SimpleString, got %d", resp.Type)
	}
	if resp.Str != "OK" {
		t.Fatalf("expected 'OK', got '%s'", resp.Str)
	}
}

func TestDecodeError(t *testing.T) {
	r := strings.NewReader("-ERR unknown command\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseError {
		t.Fatalf("expected Error, got %d", resp.Type)
	}
	if resp.Str != "ERR unknown command" {
		t.Fatalf("expected 'ERR unknown command', got '%s'", resp.Str)
	}
}

func TestDecodeInteger(t *testing.T) {
	r := strings.NewReader(":42\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseInteger {
		t.Fatalf("expected Integer, got %d", resp.Type)
	}
	if resp.Integer != 42 {
		t.Fatalf("expected 42, got %d", resp.Integer)
	}
}

func TestDecodeBulkString(t *testing.T) {
	r := strings.NewReader("$5\r\nhello\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseBulkString {
		t.Fatalf("expected BulkString, got %d", resp.Type)
	}
	if string(resp.Data) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", string(resp.Data))
	}
}

func TestDecodeNullBulkString(t *testing.T) {
	r := strings.NewReader("$-1\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseNull {
		t.Fatalf("expected Null, got %d", resp.Type)
	}
}

func TestDecodeArray(t *testing.T) {
	r := strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseArray {
		t.Fatalf("expected Array, got %d", resp.Type)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
	if string(resp.Items[0].Data) != "foo" {
		t.Fatalf("expected 'foo', got '%s'", string(resp.Items[0].Data))
	}
	if string(resp.Items[1].Data) != "bar" {
		t.Fatalf("expected 'bar', got '%s'", string(resp.Items[1].Data))
	}
}

func TestDecodeNullArray(t *testing.T) {
	r := strings.NewReader("*-1\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Type != ResponseNull {
		t.Fatalf("expected Null, got %d", resp.Type)
	}
}

func TestDecodePONG(t *testing.T) {
	r := strings.NewReader("+PONG\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Str != "PONG" {
		t.Fatalf("expected 'PONG', got '%s'", resp.Str)
	}
}

func TestDecodeNegativeInteger(t *testing.T) {
	r := strings.NewReader(":-2\r\n")
	dec := NewDecoder(r)
	resp, err := dec.Decode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Integer != -2 {
		t.Fatalf("expected -2, got %d", resp.Integer)
	}
}

func TestResponseString(t *testing.T) {
	tests := []struct {
		name     string
		resp     Response
		expected string
	}{
		{"simple string", Response{Type: ResponseSimpleString, Str: "OK"}, "OK"},
		{"error", Response{Type: ResponseError, Str: "ERR bad"}, "(error) ERR bad"},
		{"integer", Response{Type: ResponseInteger, Integer: 42}, "(integer) 42"},
		{"bulk string", Response{Type: ResponseBulkString, Data: []byte("hello")}, "hello"},
		{"null bulk", Response{Type: ResponseNull}, "(nil)"},
		{"nil bulk", Response{Type: ResponseBulkString, Data: nil}, "(nil)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.resp.String()
			if got != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, got)
			}
		})
	}
}

// --- Fix 4: negative EX/PX values must be rejected ---

func TestParseSetFlagsNegativeEX(t *testing.T) {
	_, _, _, err := ParseSetFlags([]string{"key", "val", "EX", "-5"})
	if err == nil {
		t.Fatal("negative EX value should return an error")
	}
}

func TestParseSetFlagsNegativePX(t *testing.T) {
	_, _, _, err := ParseSetFlags([]string{"key", "val", "PX", "-1000"})
	if err == nil {
		t.Fatal("negative PX value should return an error")
	}
}

func TestParseSetFlagsZeroEX(t *testing.T) {
	_, _, _, err := ParseSetFlags([]string{"key", "val", "EX", "0"})
	if err == nil {
		t.Fatal("EX 0 should return an error")
	}
}

func TestParseSetFlagsZeroPX(t *testing.T) {
	_, _, _, err := ParseSetFlags([]string{"key", "val", "PX", "0"})
	if err == nil {
		t.Fatal("PX 0 should return an error")
	}
}

func TestParseSetFlagsPositiveEX(t *testing.T) {
	_, ttl, _, err := ParseSetFlags([]string{"key", "val", "EX", "10"})
	if err != nil {
		t.Fatalf("valid EX should not error: %v", err)
	}
	if ttl != int64(10e9) {
		t.Errorf("expected 10e9 nanoseconds, got %d", ttl)
	}
}

func TestParseSetFlagsPositivePX(t *testing.T) {
	_, ttl, _, err := ParseSetFlags([]string{"key", "val", "PX", "5000"})
	if err != nil {
		t.Fatalf("valid PX should not error: %v", err)
	}
	if ttl != int64(5000e6) {
		t.Errorf("expected 5000e6 nanoseconds, got %d", ttl)
	}
}
