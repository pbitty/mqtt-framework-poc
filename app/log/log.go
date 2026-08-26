package log

import (
	"fmt"
	"log/slog"
	"os"

	"charm.land/log/v2"
	"go.uber.org/fx/fxevent"
)

func NewLogger() (*slog.Logger, error) {
	handler := log.New(os.Stderr)
	handler.SetReportTimestamp(true)

	if l := os.Getenv("LOG_LEVEL"); l != "" {
		lvl, err := log.ParseLevel(l)
		if err != nil {
			return nil, fmt.Errorf("parsing LOG_LEVEL env var: %w", err)
		}
		handler.SetLevel(lvl)
	}

	return slog.New(handler), nil
}

func NewFxLogger(l *slog.Logger) fxevent.Logger {
	return &fxevent.SlogLogger{
		Logger: l,
	}
}
