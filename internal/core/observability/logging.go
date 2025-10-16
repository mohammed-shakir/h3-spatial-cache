package observability

import (
	"log/slog"
	"os"
)

func NewLogger(level string) *slog.Logger {
	logLevel := new(slog.LevelVar)
	switch level {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "info":
		logLevel.Set(slog.LevelInfo)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		logLevel.Set(slog.LevelInfo)
	}
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	return slog.New(h)
}
