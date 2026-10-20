package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/httpapi"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
)

var (
	ErrClosed         = errors.New("tile service is closed")
	ErrCatalogChanged = errors.New("catalog changed")
)

type CatalogChangedError struct {
	ExpectedVersion string
	CurrentVersion  string
}

func (err *CatalogChangedError) Error() string {
	return fmt.Sprintf("catalog changed: expected %q, current %q", err.ExpectedVersion, err.CurrentVersion)
}
func (*CatalogChangedError) Unwrap() error { return ErrCatalogChanged }

type Service struct {
	observer     *catalog.Observer
	provider     *tileProvider
	config       config.Config
	context      context.Context
	cancel       context.CancelFunc
	observerDone chan struct{}
	mu           sync.Mutex
	closed       bool
	active       sync.WaitGroup
	leases       map[*Lease]struct{}
	closeOnce    sync.Once
	closeErr     error
}

type Lease struct {
	Tile     httpapi.Tile
	Snapshot catalog.Snapshot
	service  *Service
	once     sync.Once
	err      error
}

func (lease *Lease) Close() error {
	lease.once.Do(func() {
		lease.err = lease.Tile.Reader.Close()
		lease.service.mu.Lock()
		delete(lease.service.leases, lease)
		lease.service.mu.Unlock()
	})
	return lease.err
}

func Open(ctx context.Context, cfg config.Config) (*Service, error) {
	return open(ctx, cfg, prepareExtensions)
}

func open(ctx context.Context, cfg config.Config, prepare func(context.Context, string) (string, error)) (*Service, error) {
	if ctx == nil {
		return nil, errors.New("open service: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	cfg.Fields = append([]string(nil), cfg.Fields...)
	manager, err := catalog.New(catalog.Options{
		CatalogURL:  cfg.CatalogURL,
		Client:      cfg.CatalogClient,
		CatalogHost: cfg.CatalogHost,
		AssetHost:   cfg.AssetHost,
		Fields:      cfg.Fields,
		Timeout:     cfg.CatalogTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create catalog manager: %w", err)
	}
	observer, err := catalog.NewObserver(manager, catalog.ObserverOptions{PollInterval: cfg.CatalogPollInterval, StaleGrace: cfg.CatalogStaleGrace})
	if err != nil {
		return nil, fmt.Errorf("create catalog observer: %w", err)
	}
	negativeCacheEntries, err := positiveInt(cfg.NegativeCacheEntries, "negative cache entries")
	if err != nil {
		return nil, err
	}
	tileCache, err := cache.New(cache.Options{Root: cfg.CacheDir, MaxBytes: cfg.CacheMaxBytes, MaxEntries: cfg.CacheMaxEntries})
	if err != nil {
		return nil, fmt.Errorf("create tile cache: %w", err)
	}
	if cfg.ExtensionDirectory == "" {
		directory, prepareErr := prepare(ctx, cfg.CacheDir)
		if prepareErr != nil {
			return nil, errors.Join(prepareErr, tileCache.Close())
		}
		cfg.ExtensionDirectory = directory
	}
	negative, err := cache.NewNegativeCache(negativeCacheEntries, cfg.NegativeCacheTTL)
	if err != nil {
		closeErr := tileCache.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("create negative cache: %w; close tile cache: %w", err, closeErr)
		}
		return nil, fmt.Errorf("create negative cache: %w", err)
	}
	scratch, err := cache.NewScratchPool(cfg.ScratchMaxBytes)
	if err != nil {
		closeErr := negative.Close()
		cacheCloseErr := tileCache.Close()
		if closeErr != nil || cacheCloseErr != nil {
			return nil, fmt.Errorf("create scratch pool: %w; close negative cache: %w; close tile cache: %w", err, closeErr, cacheCloseErr)
		}
		return nil, fmt.Errorf("create scratch pool: %w", err)
	}
	provider, err := newTileProvider(cfg, tileCache, negative, scratch)
	if err != nil {
		negativeCloseErr := negative.Close()
		scratchCloseErr := scratch.Close()
		cacheCloseErr := tileCache.Close()
		if negativeCloseErr != nil || scratchCloseErr != nil || cacheCloseErr != nil {
			return nil, fmt.Errorf("create tile provider: %w; close negative cache: %w; close scratch: %w; close tile cache: %w", err, negativeCloseErr, scratchCloseErr, cacheCloseErr)
		}
		return nil, fmt.Errorf("create tile provider: %w", err)
	}

	serviceContext, cancel := context.WithCancel(ctx)
	service := &Service{observer: observer, provider: provider, config: cfg, context: serviceContext, cancel: cancel, observerDone: make(chan struct{}), leases: make(map[*Lease]struct{})}
	go func() {
		defer close(service.observerDone)
		if err := observer.Run(serviceContext); err != nil && serviceContext.Err() == nil {
			slog.Error("catalog observer stopped", "error", err)
		}
	}()
	return service, nil
}

func (service *Service) begin(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("service request: context is nil")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed || service.context.Err() != nil {
		return nil, nil, ErrClosed
	}
	service.active.Add(1)
	requestContext, cancel := context.WithTimeout(ctx, service.config.TileTimeout)
	stop := context.AfterFunc(service.context, cancel)
	return requestContext, func() { stop(); cancel(); service.active.Done() }, nil
}

func (service *Service) Catalog(ctx context.Context) (catalog.Snapshot, error) {
	ctx, finish, err := service.begin(ctx)
	if err != nil {
		return catalog.Snapshot{}, err
	}
	defer finish()
	return service.observer.Refresh(ctx)
}

func (service *Service) Get(ctx context.Context, query tilequery.Query, version string) (*Lease, error) {
	ctx, finish, err := service.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if err := query.Validate(); err != nil {
		return nil, err
	}
	if err := worker.ValidateMinimum(query, service.config.MinTileResolution); err != nil {
		return nil, err
	}

	snapshot, err := service.observer.Refresh(ctx)
	if err != nil {
		return nil, err
	}
	if version != "" && snapshot.CatalogVersion != version {
		return nil, &CatalogChangedError{ExpectedVersion: version, CurrentVersion: snapshot.CatalogVersion}
	}
	generation, err := service.observer.Current()
	if err != nil {
		return nil, err
	}
	if generation.Snapshot.CatalogVersion != snapshot.CatalogVersion {
		return nil, &CatalogChangedError{ExpectedVersion: snapshot.CatalogVersion, CurrentVersion: generation.Snapshot.CatalogVersion}
	}
	tile, err := service.provider.Get(ctx, generation, query)
	if err != nil {
		if generation.Context.Err() != nil {
			current, currentErr := service.observer.Current()
			if currentErr != nil {
				return nil, errors.Join(err, currentErr)
			}
			return nil, errors.Join(err, &CatalogChangedError{ExpectedVersion: snapshot.CatalogVersion, CurrentVersion: current.Snapshot.CatalogVersion})
		}
		return nil, err
	}
	latest, err := service.observer.Refresh(ctx)
	if err != nil {
		return nil, errors.Join(err, tile.Reader.Close())
	}
	var lease *Lease
	var publicationErr error
	err = service.observer.WithCurrent(func(current catalog.Generation) {
		if current.Number != generation.Number || latest.CatalogVersion != snapshot.CatalogVersion {
			publicationErr = &CatalogChangedError{ExpectedVersion: snapshot.CatalogVersion, CurrentVersion: current.Snapshot.CatalogVersion}
			return
		}
		service.mu.Lock()
		defer service.mu.Unlock()
		if service.closed || service.context.Err() != nil {
			publicationErr = ErrClosed
			return
		}
		lease = &Lease{Tile: tile, Snapshot: snapshot, service: service}
		service.leases[lease] = struct{}{}
	})
	if lease == nil {
		err = errors.Join(err, publicationErr)
		if err == nil {
			err = ErrCatalogChanged
		}
		return nil, errors.Join(err, tile.Reader.Close())
	}
	return lease, nil
}

func (service *Service) Close() error {
	service.closeOnce.Do(func() {
		service.mu.Lock()
		service.closed = true
		service.cancel()
		service.mu.Unlock()
		service.observer.Close()
		service.active.Wait()
		<-service.observerDone
		service.mu.Lock()
		leases := make([]*Lease, 0, len(service.leases))
		for lease := range service.leases {
			leases = append(leases, lease)
		}
		service.mu.Unlock()
		for _, lease := range leases {
			service.closeErr = errors.Join(service.closeErr, lease.Close())
		}
		service.closeErr = errors.Join(service.closeErr, service.provider.Close())
	})
	return service.closeErr
}

func (service *Service) Configuration() config.Config {
	cfg := service.config
	cfg.Fields = append([]string(nil), cfg.Fields...)
	return cfg
}
