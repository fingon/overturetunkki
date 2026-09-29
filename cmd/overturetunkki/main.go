package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fingon/overturetunkki/internal/app"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/logging"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("overture service failed", "error", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.Parse(args)
	if err != nil {
		return fmt.Errorf("parse configuration: %w", err)
	}
	logging.Configure(cfg.Verbose)
	return app.Run(ctx, cfg)
}
