package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/httpapi"
)

func Run(ctx context.Context, cfg config.Config) (err error) {
	if ctx == nil {
		return errors.New("run context is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate configuration: %w", err)
	}
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return ctx.Err()
	}
	if cfg.ExtensionDirectory == "" {
		if dataPath := os.Getenv(duckdbDataPathEnv); dataPath != "" {
			cfg.ExtensionDirectory = filepath.Join(dataPath, "extensions")
		}
	}
	service, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, service.Close()) }()
	workerCount, err := positiveInt(cfg.WorkerCount, "worker count")
	if err != nil {
		return err
	}
	api, err := httpapi.New(httpapi.Options{
		Observer: service.observer, Provider: service.provider, Fields: cfg.Fields,
		MaxTileBytes: cfg.MaxTileBytes, MinTileResolution: cfg.MinTileResolution,
		MaxTileRows: cfg.MaxTileRows, TileConcurrency: workerCount, WriteTimeout: cfg.WriteTimeout,
		AttributionURL: []string{"https://overturemaps.org"},
	})
	if err != nil {
		return fmt.Errorf("create HTTP server: %w", err)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", cfg.Listen, err)
	}
	server := &http.Server{Handler: api}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			err = ctx.Err()
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.WriteTimeout)
	defer cancel()
	err = errors.Join(err, api.Shutdown(shutdownContext), server.Shutdown(shutdownContext))
	if err != nil {
		err = errors.Join(err, server.Close())
	}
	return err
}
