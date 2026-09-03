package logger

import (
	"log/slog"
	"os"
)

func NewLogger(attrs ...any) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	return slog.New(handler).With(attrs...)
}
