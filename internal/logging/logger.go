// Package logging builds a real structured logger (log/slog under the
// hood) from config: a text or JSON handler depending on format, level
// filtering that actually works, and output to stdout/stderr/a file.
//
// The previous version of this package claimed a "format: json" option
// but always wrote the same bracketed plain-text line regardless of what
// was configured — a capability claimed in config that the code never
// actually provided. It was also never wired into main.go at all
// (constructed, then immediately discarded). This version is real, and
// is set as the process's slog default in cmd/server/main.go.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New builds a *slog.Logger. level is one of debug/info/warn/error
// (case-insensitive; anything else defaults to info). format is "json"
// for slog.NewJSONHandler or anything else (including "text" or "") for
// slog.NewTextHandler. output is "stdout", "stderr", or a file path to
// append to; an unwritable file path falls back to stdout rather than
// failing startup over a logging misconfiguration.
func New(level, format, output string) *slog.Logger {
	w := resolveOutput(output)
	opts := &slog.HandlerOptions{Level: ParseLevel(level)}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	return slog.New(handler)
}

// ParseLevel converts a config string to a slog.Level, defaulting to
// Info for an unrecognized value.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func resolveOutput(output string) *os.File {
	switch output {
	case "", "stdout":
		return os.Stdout
	case "stderr":
		return os.Stderr
	default:
		f, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return os.Stdout
		}
		return f
	}
}
