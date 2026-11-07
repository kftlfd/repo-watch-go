package logger

import (
	"log/slog"
	"os"
	"strings"
)

func NewLogger(minLevel slog.Level, attrs ...any) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: minLevel,
	})

	return slog.New(handler).With(attrs...)
}

func ParseLevel(value string) slog.Level {
	value = strings.TrimSpace(value)
	var level slog.Level

	if err := level.UnmarshalText([]byte(value)); err != nil {
		return slog.LevelInfo
	}

	return level
}
