package main

import (
	"log/slog"
	"os"
	"strings"
)

// logger is the process-wide structured logger. It is initialized by
// initLogger before the server starts; until then it defaults to an
// info-level text logger so package-level code (and tests) never see a
// nil logger.
var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

// initLogger configures the process-wide logger. level is one of
// "debug", "info", "warn"/"warning", "error" (case-insensitive, defaults
// to info). format is "text" (default) or "json".
func initLogger(level, format string) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	logger = slog.New(handler)
	slog.SetDefault(logger)
}
