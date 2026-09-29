package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/config"
)

func Run(ctx context.Context, cfg config.Config) error {
	if ctx == nil {
		return fmt.Errorf("run context is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate configuration: %w", err)
	}
	if err := config.ValidateCacheDirectory(cfg.CacheDir); err != nil {
		return fmt.Errorf("validate cache directory: %w", err)
	}
	switch cfg.Mode {
	case config.ModeSupervisor:
		return runSupervisor(ctx, cfg)
	case config.ModeWorker:
		return runWorker(ctx, cfg)
	default:
		return fmt.Errorf("unsupported process mode %q", cfg.Mode)
	}
}

func runSupervisor(ctx context.Context, cfg config.Config) error {
	slog.Info("supervisor mode started", "listen", cfg.Listen, "worker_count", cfg.WorkerCount)
	manager, err := catalog.New(catalog.Options{
		CatalogURL: cfg.CatalogURL,
		Fields:     cfg.Fields,
		Timeout:    cfg.CatalogTimeout,
	})
	if err != nil {
		return fmt.Errorf("create catalog manager: %w", err)
	}
	observer, err := catalog.NewObserver(manager, cfg.CatalogPollInterval)
	if err != nil {
		return fmt.Errorf("create catalog observer: %w", err)
	}
	return observer.Run(ctx)
}

func runWorker(ctx context.Context, cfg config.Config) error {
	slog.Info("worker mode started", "worker_threads", cfg.WorkerThreads, "worker_memory_bytes", cfg.WorkerMemoryBytes)
	return waitForShutdown(ctx, config.ModeWorker)
}

func waitForShutdown(ctx context.Context, mode config.Mode) error {
	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		slog.Info("process mode stopped", "mode", mode)
		return nil
	}
	return fmt.Errorf("process mode %q stopped: %w", mode, ctx.Err())
}
