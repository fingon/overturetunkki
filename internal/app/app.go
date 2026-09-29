package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/httpapi"
)

func Run(ctx context.Context, cfg config.Config) error {
	if ctx == nil {
		return errors.New("run context is nil")
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
	if err := ctx.Err(); err != nil {
		return waitForShutdown(ctx, config.ModeSupervisor)
	}
	slog.Info("supervisor mode started", "listen", cfg.Listen, "worker_count", cfg.WorkerCount)
	manager, err := catalog.New(catalog.Options{
		CatalogURL:  cfg.CatalogURL,
		CatalogHost: cfg.CatalogHost,
		AssetHost:   cfg.AssetHost,
		Fields:      cfg.Fields,
		Timeout:     cfg.CatalogTimeout,
	})
	if err != nil {
		return fmt.Errorf("create catalog manager: %w", err)
	}
	observer, err := catalog.NewObserver(manager, cfg.CatalogPollInterval)
	if err != nil {
		return fmt.Errorf("create catalog observer: %w", err)
	}
	workerCount, err := positiveInt(cfg.WorkerCount, "worker count")
	if err != nil {
		return err
	}
	negativeCacheEntries, err := positiveInt(cfg.NegativeCacheEntries, "negative cache entries")
	if err != nil {
		return err
	}
	tileCache, err := cache.New(cache.Options{Root: cfg.CacheDir, MaxBytes: cfg.CacheMaxBytes, MaxEntries: cfg.CacheMaxEntries})
	if err != nil {
		return fmt.Errorf("create tile cache: %w", err)
	}
	negative, err := cache.NewNegativeCache(negativeCacheEntries, cfg.NegativeCacheTTL)
	if err != nil {
		closeErr := tileCache.Close()
		if closeErr != nil {
			return fmt.Errorf("create negative cache: %w; close tile cache: %w", err, closeErr)
		}
		return fmt.Errorf("create negative cache: %w", err)
	}
	scratch, err := cache.NewScratchPool(cfg.ScratchMaxBytes)
	if err != nil {
		closeErr := negative.Close()
		cacheCloseErr := tileCache.Close()
		if closeErr != nil || cacheCloseErr != nil {
			return fmt.Errorf("create scratch pool: %w; close negative cache: %w; close tile cache: %w", err, closeErr, cacheCloseErr)
		}
		return fmt.Errorf("create scratch pool: %w", err)
	}
	provider, err := newTileProvider(cfg, tileCache, negative, scratch)
	if err != nil {
		negativeCloseErr := negative.Close()
		scratchCloseErr := scratch.Close()
		cacheCloseErr := tileCache.Close()
		if negativeCloseErr != nil || scratchCloseErr != nil || cacheCloseErr != nil {
			return fmt.Errorf("create tile provider: %w; close negative cache: %w; close scratch: %w; close tile cache: %w", err, negativeCloseErr, scratchCloseErr, cacheCloseErr)
		}
		return fmt.Errorf("create tile provider: %w", err)
	}
	apiServer, err := httpapi.New(httpapi.Options{
		Observer:        observer,
		Provider:        provider,
		Fields:          cfg.Fields,
		MaxTileBytes:    cfg.MaxTileBytes,
		MaxTileRows:     cfg.MaxTileRows,
		TileConcurrency: workerCount,
		WriteTimeout:    cfg.WriteTimeout,
		AttributionURL:  []string{"https://overturemaps.org"},
	})
	if err != nil {
		if closeErr := provider.Close(); closeErr != nil {
			return fmt.Errorf("create HTTP server: %w; close tile provider: %w", err, closeErr)
		}
		return fmt.Errorf("create HTTP server: %w", err)
	}
	return runHTTPService(ctx, httpServiceOptions{Config: cfg, Observer: observer, APIServer: apiServer, Provider: provider})
}

type httpServiceOptions struct {
	Config    config.Config
	Observer  *catalog.Observer
	APIServer *httpapi.Server
	Provider  *tileProvider
}

func runHTTPService(ctx context.Context, options httpServiceOptions) error {
	cfg := options.Config
	observer := options.Observer
	apiServer := options.APIServer
	provider := options.Provider
	serviceContext, cancel := context.WithCancel(ctx)
	defer cancel()
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		closeErr := provider.Close()
		if closeErr != nil {
			return fmt.Errorf("listen on %q: %w; close tile provider: %w", cfg.Listen, err, closeErr)
		}
		return fmt.Errorf("listen on %q: %w", cfg.Listen, err)
	}
	httpServer := &http.Server{Handler: apiServer}
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpServer.Serve(listener) }()
	observerResult := make(chan error, 1)
	go func() { observerResult <- observer.Run(serviceContext) }()
	var cause error
	observerStopped := false
	select {
	case err := <-serveResult:
		cancel()
		if !errors.Is(err, http.ErrServerClosed) {
			cause = fmt.Errorf("serve HTTP: %w", err)
		}
	case err := <-observerResult:
		observerStopped = true
		if err != nil && ctx.Err() == nil {
			cause = fmt.Errorf("run catalog observer: %w", err)
		}
		cancel()
	case <-ctx.Done():
		cancel()
		if !errors.Is(ctx.Err(), context.Canceled) {
			cause = fmt.Errorf("supervisor context: %w", ctx.Err())
		}
	}
	shutdownErr := shutdownHTTPService(apiServer, httpServer, cfg.WriteTimeout)
	cancel()
	if !observerStopped {
		observerWaitContext, observerWaitCancel := context.WithTimeout(context.Background(), cfg.CatalogTimeout)
		select {
		case err := <-observerResult:
			if err != nil && ctx.Err() == nil {
				cause = errors.Join(cause, fmt.Errorf("run catalog observer: %w", err))
			}
		case <-observerWaitContext.Done():
			cause = errors.Join(cause, fmt.Errorf("wait for catalog observer: %w", observerWaitContext.Err()))
		}
		observerWaitCancel()
	}
	providerErr := provider.Close()
	if shutdownErr != nil {
		cause = errors.Join(cause, shutdownErr)
	}
	if providerErr != nil {
		cause = errors.Join(cause, providerErr)
	}
	if cause != nil {
		return cause
	}
	return nil
}

func shutdownHTTPService(apiServer *httpapi.Server, httpServer *http.Server, timeout time.Duration) error {
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	apiErr := apiServer.Shutdown(shutdownContext)
	httpErr := httpServer.Shutdown(shutdownContext)
	if apiErr != nil || httpErr != nil {
		return errors.Join(apiErr, httpErr)
	}
	return nil
}

func runWorker(ctx context.Context, cfg config.Config) error {
	if os.Getenv(workerProtocolEnv) == workerProtocolValue {
		return runWorkerProtocol(ctx)
	}
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
