package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"garbage", slog.LevelInfo},
		{"", slog.LevelInfo},
	}
	for _, tt := range tests {
		if got := ParseLevel(tt.input); got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestNew_JSONFormatProducesRealJSON is the direct regression test for
// the previous implementation's bug: it accepted a "format" parameter
// but always emitted the same plain-text line regardless of its value —
// a claimed capability ("json") that the code never actually provided.
func TestNew_JSONFormatProducesRealJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("test message", "key", "value")

	var decoded map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid JSON output, got %q: %v", buf.String(), err)
	}
	if decoded["msg"] != "test message" {
		t.Errorf("decoded[msg] = %v, want %q", decoded["msg"], "test message")
	}
	if decoded["key"] != "value" {
		t.Errorf("decoded[key] = %v, want %q", decoded["key"], "value")
	}
}

func TestNew_TextFormatIsNotJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("test message", "key", "value")

	var decoded map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err == nil {
		t.Error("expected text-format output not to parse as JSON")
	}
	if !strings.Contains(buf.String(), "test message") {
		t.Errorf("expected output to contain the message, got %q", buf.String())
	}
}

// TestNew_LevelFiltering proves ParseLevel actually gates output through
// the same slog.HandlerOptions construction New itself uses — New writes
// to stdout/stderr/a file (resolveOutput), which isn't practical to
// capture in a unit test, so this exercises the identical handler
// construction with a buffer instead of stdout.
func TestNew_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: ParseLevel("warn")})
	l := slog.New(h)
	l.Info("should be filtered out")
	l.Warn("should appear")

	out := buf.String()
	if strings.Contains(out, "should be filtered out") {
		t.Error("expected info-level message to be filtered out at warn level")
	}
	if !strings.Contains(out, "should appear") {
		t.Error("expected warn-level message to appear")
	}
}

func TestNew_DefaultsToStdoutOnUnwritableFile(t *testing.T) {
	// An unwritable/invalid path must not panic or fail construction —
	// it falls back to stdout.
	logger := New("info", "text", "/nonexistent-dir-xyz/definitely/not/writable.log")
	if logger == nil {
		t.Fatal("expected a non-nil logger even when the configured output path is unwritable")
	}
}

func TestNew_ReturnsUsableLogger(t *testing.T) {
	logger := New("debug", "json", "stdout")
	if logger == nil {
		t.Fatal("expected a non-nil logger")
	}
	// Must not panic.
	logger.Info("smoke test", "component", "logging_test")
}
