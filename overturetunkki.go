// Package overturetunkki retrieves Overture places as local GeoParquet tiles.
package overturetunkki

import (
	"context"
	"net/http"
	"time"

	"github.com/fingon/overturetunkki/internal/app"
	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
)

// Options controls catalog discovery, disk caching, and bounded in-process builds.
// Start with DefaultOptions; Open validates every limit without reading environment variables.
type Options struct {
	// AssetCAFile optionally supplies a PEM CA bundle for DuckDB HTTPS asset reads.
	AssetCAFile string
	// CatalogClient optionally supplies a caller-owned client for STAC requests.
	CatalogClient        *http.Client
	CatalogURL           string
	CatalogHost          string
	AssetHost            string
	CatalogPollInterval  time.Duration
	CatalogStaleGrace    time.Duration
	CatalogTimeout       time.Duration
	Fields               []string
	MinTileResolution    int
	MaxTileBytes         int64
	MaxTileRows          int64
	CacheDir             string
	CacheMaxBytes        int64
	CacheMaxEntries      int64
	ScratchMaxBytes      int64
	WorkerCount          int64
	WorkerMemoryBytes    int64
	WorkerThreads        int64
	QueueCapacity        int64
	TileTimeout          time.Duration
	NegativeCacheEntries int64
	NegativeCacheTTL     time.Duration
	// ExtensionDirectory bypasses managed downloads when set to an absolute path.
	ExtensionDirectory string
	Profile            bool
}

// DefaultOptions returns independent options using the supplied absolute cache directory.
func DefaultOptions(cacheDir string) Options {
	return Options{
		CatalogURL:           config.DefaultCatalogURL,
		CatalogHost:          catalog.DefaultCatalogHost,
		AssetHost:            catalog.DefaultAssetHost,
		CatalogPollInterval:  config.DefaultCatalogPollInterval,
		CatalogStaleGrace:    config.DefaultCatalogStaleGrace,
		CatalogTimeout:       config.DefaultCatalogTimeout,
		Fields:               []string{"id", "geometry", "names", "basic_category"},
		MinTileResolution:    config.DefaultMinTileResolution,
		MaxTileBytes:         config.DefaultMaxTileBytes,
		MaxTileRows:          config.DefaultMaxTileRows,
		CacheDir:             cacheDir,
		CacheMaxBytes:        config.DefaultCacheMaxBytes,
		CacheMaxEntries:      config.DefaultCacheMaxEntries,
		ScratchMaxBytes:      config.DefaultScratchMaxBytes,
		WorkerCount:          config.DefaultWorkerCount,
		WorkerMemoryBytes:    config.DefaultWorkerMemoryBytes,
		WorkerThreads:        config.DefaultWorkerThreads,
		QueueCapacity:        config.DefaultQueueCapacity,
		TileTimeout:          config.DefaultTileTimeout,
		NegativeCacheEntries: config.DefaultNegativeCacheEntries,
		NegativeCacheTTL:     config.DefaultNegativeCacheTTL,
		ExtensionDirectory:   "",
		Profile:              false,
	}
}

func (options Options) config() config.Config {
	return config.Config{
		CatalogClient: options.CatalogClient,
		AssetCAFile:   options.AssetCAFile,
		Listen:        config.DefaultListen, WriteTimeout: config.DefaultWriteTimeout,
		CatalogURL:           options.CatalogURL,
		CatalogHost:          options.CatalogHost,
		AssetHost:            options.AssetHost,
		CatalogPollInterval:  options.CatalogPollInterval,
		CatalogStaleGrace:    options.CatalogStaleGrace,
		CatalogTimeout:       options.CatalogTimeout,
		Fields:               options.Fields,
		MinTileResolution:    options.MinTileResolution,
		MaxTileBytes:         options.MaxTileBytes,
		MaxTileRows:          options.MaxTileRows,
		CacheDir:             options.CacheDir,
		CacheMaxBytes:        options.CacheMaxBytes,
		CacheMaxEntries:      options.CacheMaxEntries,
		ScratchMaxBytes:      options.ScratchMaxBytes,
		WorkerCount:          options.WorkerCount,
		WorkerMemoryBytes:    options.WorkerMemoryBytes,
		WorkerThreads:        options.WorkerThreads,
		QueueCapacity:        options.QueueCapacity,
		TileTimeout:          options.TileTimeout,
		NegativeCacheEntries: options.NegativeCacheEntries,
		NegativeCacheTTL:     options.NegativeCacheTTL,
		ExtensionDirectory:   options.ExtensionDirectory,
		Verbose:              options.Profile,
	}
}

// Service owns one exclusive disk cache and its catalog observer and tile scheduler.
// Methods may be called concurrently. Close releases all resources and tile paths.
type Service struct{ service *app.Service }

// Open starts a service without a listener, signal handlers, or global logging changes.
// Missing extensions are installed by DuckDB into the cache before Open returns.
// Catalog discovery runs in the background; Catalog and Get wait for validated metadata.
// Cancellation of ctx cancels requests and observation; callers must still call Close.
func Open(ctx context.Context, options Options) (*Service, error) {
	service, err := app.Open(ctx, options.config())
	if err != nil {
		return nil, err
	}
	return &Service{service: service}, nil
}

// Close cancels builds and invalidates all outstanding tile paths. It is idempotent.
func (service *Service) Close() error { return service.service.Close() }

// Square identifies a standard Web Mercator XYZ square at zoom 0 through 22.
type Square struct {
	Z int
	X int
	Y int
}

// TileRequest selects exactly one H3 cell or XYZ square. An empty CatalogVersion
// selects the current catalog; a supplied version must match it.
type TileRequest struct {
	Cell           string
	Square         *Square
	CatalogVersion string
}

// Tile holds a read-only local path, pinned until Close or Service.Close.
// Ordinary H3 compositions are temporary; XYZ files remain in the bounded cache.
type Tile struct {
	Path           string
	SizeBytes      int64
	Digest         string
	Release        string
	CatalogVersion string
	ProjectionID   string
	lease          *app.Lease
}

// Close releases the path's pin and deletes temporary H3 output after its last holder closes.
func (tile *Tile) Close() error { return tile.lease.Close() }

// Get returns a complete GeoParquet file or an error; it never returns truncated tiles.
// Files exceeding serving limits are not returned, even when retained in the cache.
func (service *Service) Get(ctx context.Context, request TileRequest) (*Tile, error) {
	query := tilequery.Query{Cell: request.Cell}
	if request.Square != nil {
		query.Square = &tilequery.Square{Z: request.Square.Z, X: request.Square.X, Y: request.Square.Y}
	}
	lease, err := service.service.Get(ctx, query, request.CatalogVersion)
	if err != nil {
		return nil, err
	}
	return &Tile{
		Path: lease.Tile.Path, SizeBytes: lease.Tile.SizeBytes, Digest: lease.Tile.Digest,
		Release: lease.Snapshot.Release, CatalogVersion: lease.Snapshot.CatalogVersion,
		ProjectionID: lease.Snapshot.ProjectionID, lease: lease,
	}, nil
}

// Catalog describes the currently validated data and serving limits.
type Catalog struct {
	Release                string
	CatalogVersion         string
	ProjectionID           string
	Fields                 []string
	MaxTileBytes           int64
	MaxTileRows            int64
	SupportedXYZZooms      []int
	SupportedH3Resolutions []int
	Attribution            []string
}

// Catalog refreshes shared catalog metadata according to the configured freshness policy.
func (service *Service) Catalog(ctx context.Context) (Catalog, error) {
	snapshot, err := service.service.Catalog(ctx)
	if err != nil {
		return Catalog{}, err
	}
	options := service.service.Configuration()
	resolutions := make([]int, 0, 16-options.MinTileResolution)
	for resolution := options.MinTileResolution; resolution <= 15; resolution++ {
		resolutions = append(resolutions, resolution)
	}
	zooms := make([]int, tilequery.MaxZoom+1)
	for zoom := range zooms {
		zooms[zoom] = zoom
	}
	return Catalog{
		Release: snapshot.Release, CatalogVersion: snapshot.CatalogVersion,
		ProjectionID: snapshot.ProjectionID, Fields: append([]string(nil), options.Fields...),
		MaxTileBytes: options.MaxTileBytes, MaxTileRows: options.MaxTileRows,
		SupportedXYZZooms: zooms, SupportedH3Resolutions: resolutions, Attribution: []string{"https://overturemaps.org"},
	}, nil
}

// These errors support errors.Is; limit and version details support errors.As.
var (
	ErrClosed              = app.ErrClosed
	ErrCatalogChanged      = app.ErrCatalogChanged
	ErrCatalogUnavailable  = catalog.ErrCatalogUnavailable
	ErrCatalogNotReady     = catalog.ErrCatalogNotReady
	ErrCapacityUnavailable = cache.ErrCapacityUnavailable
	ErrQueueFull           = cache.ErrQueueFull
	ErrCacheInUse          = cache.ErrCacheInUse
	ErrTooManyRows         = worker.ErrTooManyRows
	ErrOutputTooLarge      = worker.ErrOutputTooLarge
	ErrResolutionTooCoarse = worker.ErrResolutionTooCoarse
	ErrInvalidCell         = worker.ErrInvalidCell
	ErrTileTimeout         = worker.ErrTileTimeout
	ErrTileCanceled        = worker.ErrTileCanceled
	ErrUpstream            = worker.ErrUpstream
	ErrDiskFailure         = worker.ErrDiskFailure
	ErrOutOfMemory         = worker.ErrOutOfMemory
	ErrInvalidOutput       = worker.ErrInvalidOutput
)

// CatalogChangedError reports a requested or in-flight catalog version change.
type (
	CatalogChangedError = app.CatalogChangedError
	// TooManyRowsError reports the bounded row probe and configured threshold.
	TooManyRowsError = worker.TooManyRowsError
	// OutputTooLargeError reports a completed file's size and serving threshold.
	OutputTooLargeError = worker.OutputTooLargeError
	// ResolutionTooCoarseError reports an H3 resolution below the configured minimum.
	ResolutionTooCoarseError = worker.ResolutionTooCoarseError
)
