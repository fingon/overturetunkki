package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mstenber/overturetunkki/internal/app"
	"github.com/mstenber/overturetunkki/internal/config"
	"github.com/mstenber/overturetunkki/internal/logging"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("overture service failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.Parse(args)
	if err != nil {
		return fmt.Errorf("parse configuration: %w", err)
	}
	logging.Configure(cfg.Verbose)
	return app.Run(ctx, cfg)
}
