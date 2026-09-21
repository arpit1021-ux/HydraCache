package protocol

import (
	"strings"
	"testing"
)

func TestParser_ValidCommand(t *testing.T) {
	p := NewParser(strings.NewReader("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	cmd, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name != "SET" || len(cmd.Args) != 2 || cmd.Args[0] != "k" || cmd.Args[1] != "v" {
		t.Errorf("cmd = %+v", cmd)
	}
}

func TestParser_InlineCommand(t *testing.T) {
	p := NewParser(strings.NewReader("PING\r\n"))
	cmd, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name != "PING" {
		t.Errorf("cmd.Name = %q, want PING", cmd.Name)
	}
}

// TestParser_NegativeBulkLengthRejectedNotPanics is the direct regression
// test for a real crash bug: make([]byte, strLen+2) with strLen<-2 panics
// with a Go runtime "makeslice: len out of range" — a panic nothing
// upstream of this parser recovered from before the server-level
// recover() was added, meaning a single client sending this exact
// payload could crash the entire process. It must be a clean parse
// error, and the test itself must not panic.
func TestParser_NegativeBulkLengthRejectedNotPanics(t *testing.T) {
	p := NewParser(strings.NewReader("*1\r\n$-5\r\n"))
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error for a negative bulk string length")
	}
}

func TestParser_NegativeArrayCountTreatedAsNullArray(t *testing.T) {
	p := NewParser(strings.NewReader("*-1\r\n"))
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error for a negative array count")
	}
}

func TestParser_ArrayCountExceedingMaxRejected(t *testing.T) {
	p := NewParserWithLimits(strings.NewReader("*100\r\n"), DefaultMaxBulkLen, 10)
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error when array count exceeds the configured maximum")
	}
}

// TestParser_ArrayCountExceedingMaxDoesNotPreallocate proves the max-count
// check happens BEFORE make([]string, 0, count) — a huge claimed count
// must be rejected without ever attempting the allocation it implies.
// A claimed count of 2 billion with no such guard would itself be a
// memory-exhaustion vector even before reading any of the claimed
// elements.
func TestParser_ArrayCountExceedingMaxDoesNotPreallocate(t *testing.T) {
	p := NewParserWithLimits(strings.NewReader("*2000000000\r\n"), DefaultMaxBulkLen, DefaultMaxArrayLen)
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error for an array count far exceeding the default maximum")
	}
}

func TestParser_BulkLengthExceedingMaxRejected(t *testing.T) {
	p := NewParserWithLimits(strings.NewReader("*1\r\n$1000\r\n"), 16, DefaultMaxArrayLen)
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error when bulk length exceeds the configured maximum")
	}
}

func TestParser_BulkLengthExceedingMaxDoesNotAllocate(t *testing.T) {
	// A single claimed bulk length of 2GB, with no guard, would attempt a
	// 2GB allocation for one command before ever validating there's that
	// much data behind it.
	p := NewParserWithLimits(strings.NewReader("*1\r\n$2000000000\r\n"), DefaultMaxBulkLen, DefaultMaxArrayLen)
	_, err := p.ReadCommand()
	if err == nil {
		t.Fatal("expected an error for a bulk length far exceeding the default maximum")
	}
}

func TestParser_WithinLimitsAccepted(t *testing.T) {
	p := NewParserWithLimits(strings.NewReader("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"), 16, 16)
	cmd, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name != "GET" || cmd.Args[0] != "k" {
		t.Errorf("cmd = %+v", cmd)
	}
}
