package logging

import (
	"io"
	"log/slog"
	"os"
)

func Configure(verbose bool) {
	slog.SetDefault(New(verbose, os.Stderr))
}

func New(verbose bool, output io.Writer) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: level}))
}
