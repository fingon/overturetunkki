package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	// Register DuckDB's database/sql driver.
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/fingon/overturetunkki/internal/httpapi"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
)

const (
	duckdbHTTPFSExtension       = "httpfs"
	duckdbSpatialExtension      = "spatial"
	duckdbExtensionDirectoryEnv = "OVERTURE_DUCKDB_EXTENSION_DIRECTORY"
	duckdbDataPathEnv           = "KO_DATA_PATH"
)

type tileProvider struct {
	cache              *cache.Cache
	negative           *cache.NegativeCache
	scheduler          *cache.Scheduler
	scratch            *cache.ScratchPool
	extensionDirectory string
	assetCAFile        string
	fields             []string
	maxRows            int64
	maxBytes           int64
	minResolution      int
	settings           worker.RuntimeSettings
	policyID           string

	mu                 sync.Mutex
	lastCatalogVersion string
	flights            map[cache.Key]*h3Flight
	flightsWait        sync.WaitGroup
	closed             bool
	closeOnce          sync.Once
	closeErr           error
}

func newTileProvider(cfg config.Config, tileCache *cache.Cache, negative *cache.NegativeCache, scratch *cache.ScratchPool) (*tileProvider, error) {
	if tileCache == nil {
		return nil, errors.New("create tile provider: cache is nil")
	}
	if negative == nil {
		return nil, errors.New("create tile provider: negative cache is nil")
	}
	if scratch == nil {
		return nil, errors.New("create tile provider: scratch pool is nil")
	}
	workerCount, err := positiveInt(cfg.WorkerCount, "worker count")
	if err != nil {
		return nil, err
	}
	queueCapacity, err := positiveInt(cfg.QueueCapacity, "queue capacity")
	if err != nil {
		return nil, err
	}
	perWorkerScratchBytes := cfg.ScratchMaxBytes / cfg.WorkerCount
	if perWorkerScratchBytes <= cfg.MaxTileBytes {
		return nil, errors.New("create tile provider: per-worker scratch capacity must exceed max-tile-bytes")
	}
	scheduler, err := cache.NewScheduler(tileCache, cache.SchedulerOptions{Workers: workerCount, QueueCapacity: queueCapacity, Timeout: cfg.TileTimeout})
	if err != nil {
		return nil, err
	}
	settings := worker.RuntimeSettings{
		Profile:          cfg.Verbose,
		MemoryBytes:      cfg.WorkerMemoryBytes,
		Threads:          cfg.WorkerThreads,
		ScratchDirectory: filepath.Join(cfg.CacheDir, "scratch"),
		ScratchMaxBytes:  perWorkerScratchBytes - cfg.MaxTileBytes,
		MaxOutputBytes:   cfg.MaxTileBytes,
		TileTimeout:      cfg.TileTimeout,
	}
	provider := &tileProvider{
		cache:              tileCache,
		negative:           negative,
		scheduler:          scheduler,
		scratch:            scratch,
		extensionDirectory: cfg.ExtensionDirectory,
		assetCAFile:        cfg.AssetCAFile,
		fields:             append([]string(nil), cfg.Fields...),
		maxRows:            cfg.MaxTileRows,
		maxBytes:           cfg.MaxTileBytes,
		minResolution:      cfg.MinTileResolution,
		settings:           settings,
		policyID:           sizePolicyID(cfg),
	}
	return provider, nil
}

func (provider *tileProvider) Get(ctx context.Context, generation catalog.Generation, query tilequery.Query) (httpapi.Tile, error) {
	return provider.get(ctx, generation, query, false)
}

func (provider *tileProvider) get(ctx context.Context, generation catalog.Generation, query tilequery.Query, source bool) (httpapi.Tile, error) {
	if provider == nil {
		return httpapi.Tile{}, errors.New("get tile: provider is nil")
	}
	if ctx == nil {
		return httpapi.Tile{}, errors.New("get tile: context is nil")
	}
	if err := query.Validate(); err != nil {
		return httpapi.Tile{}, err
	}
	if query.Square == nil {
		cell, err := worker.ParseCanonicalCell(query.Cell)
		if err != nil {
			return httpapi.Tile{}, err
		}
		if cell.Resolution() < provider.minResolution {
			return httpapi.Tile{}, &worker.ResolutionTooCoarseError{ActualResolution: cell.Resolution(), MinResolution: provider.minResolution}
		}
	}
	if err := ctx.Err(); err != nil {
		return httpapi.Tile{}, fmt.Errorf("get tile: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, provider.settings.TileTimeout)
	defer cancel()
	if generation.Context == nil {
		return httpapi.Tile{}, errors.New("get tile: catalog generation context is nil")
	}
	if err := generation.Context.Err(); err != nil {
		return httpapi.Tile{}, fmt.Errorf("get tile: catalog generation canceled: %w", err)
	}
	snapshot := generation.Snapshot
	if err := provider.observeCatalogVersion(snapshot.CatalogVersion); err != nil {
		return httpapi.Tile{}, err
	}
	key := cache.Key{
		CatalogVersion: snapshot.CatalogVersion,
		ProjectionID:   snapshot.ProjectionID,
		Tile:           query.String(),
		SizePolicyID:   provider.policyID,
	}
	provider.mu.Lock()
	closed := provider.closed
	provider.mu.Unlock()
	if closed {
		return httpapi.Tile{}, cache.ErrSchedulerClosed
	}
	if tile, err := provider.openCachedTile(key); err == nil {
		if source {
			return tile, nil
		}
		return provider.checkTileSize(tile)
	} else if !errors.Is(err, cache.ErrEntryNotFound) {
		return httpapi.Tile{}, err
	}
	if rejection, ok, err := provider.negative.Get(key); err != nil {
		return httpapi.Tile{}, fmt.Errorf("check tile negative cache: %w", err)
	} else if ok {
		return httpapi.Tile{}, rejectionError(rejection)
	}
	if query.Square == nil {
		return provider.getH3(ctx, generation, key)
	}
	if err := provider.scheduler.Do(ctx, key, provider.maxBytes, func(buildContext context.Context, reservation *cache.Reservation) error {
		generationBuildContext, cancel := context.WithCancel(buildContext)
		stopGenerationCancellation := context.AfterFunc(generation.Context, cancel)
		defer stopGenerationCancellation()
		defer cancel()
		if err := generation.Context.Err(); err != nil {
			return fmt.Errorf("build tile: catalog generation canceled: %w", err)
		}
		return provider.build(generationBuildContext, reservation, key, tileBuildOptions{Snapshot: snapshot, Query: query})
	}); err != nil {
		if source && errors.Is(err, worker.ErrOutputTooLarge) {
			if tile, cacheErr := provider.openCachedTile(key); cacheErr == nil {
				return tile, nil
			} else if !errors.Is(cacheErr, cache.ErrEntryNotFound) {
				return httpapi.Tile{}, errors.Join(err, cacheErr)
			}
		}
		return httpapi.Tile{}, err
	}
	tile, err := provider.openCachedTile(key)
	if err != nil {
		return httpapi.Tile{}, err
	}
	if source {
		return tile, nil
	}
	return provider.checkTileSize(tile)
}

func (provider *tileProvider) checkTileSize(tile httpapi.Tile) (httpapi.Tile, error) {
	if tile.SizeBytes <= provider.maxBytes {
		return tile, nil
	}
	return httpapi.Tile{}, errors.Join(&worker.OutputTooLargeError{ActualBytes: tile.SizeBytes, LimitBytes: provider.maxBytes}, tile.Reader.Close())
}

func (provider *tileProvider) openCachedTile(key cache.Key) (httpapi.Tile, error) {
	reader, err := provider.cache.Open(key)
	if err != nil {
		return httpapi.Tile{}, err
	}
	entry, err := reader.Entry()
	if err != nil {
		closeErr := reader.Close()
		if closeErr != nil {
			return httpapi.Tile{}, fmt.Errorf("read cache entry: %w; close reader: %w", err, closeErr)
		}
		return httpapi.Tile{}, fmt.Errorf("read cache entry: %w", err)
	}
	return httpapi.Tile{Path: entry.Path, SizeBytes: entry.SizeBytes, Digest: entry.Digest, Reader: reader}, nil
}

type tileBuildOptions struct {
	Snapshot    catalog.Snapshot
	Query       tilequery.Query
	SourcePaths []string
}

func (provider *tileProvider) build(ctx context.Context, reservation *cache.Reservation, key cache.Key, options tileBuildOptions) (err error) {
	if reservation == nil {
		return errors.New("build tile: cache reservation is nil")
	}
	if rejection, ok, negativeErr := provider.negative.Get(key); negativeErr != nil {
		return fmt.Errorf("recheck tile negative cache: %w", negativeErr)
	} else if ok {
		return rejectionError(rejection)
	}
	if tile, cacheErr := provider.openCachedTile(key); cacheErr == nil {
		if closeErr := tile.Reader.Close(); closeErr != nil {
			return fmt.Errorf("close rechecked cached tile: %w", closeErr)
		}
		return nil
	} else if !errors.Is(cacheErr, cache.ErrEntryNotFound) {
		return cacheErr
	}
	staging, err := provider.cache.CreateStaging(key)
	if err != nil {
		return fmt.Errorf("create tile staging: %w", err)
	}
	stagingPath := staging.Name()
	published := false
	defer func() {
		if published {
			return
		}
		if cleanupErr := provider.cache.RemoveStaging(stagingPath); cleanupErr != nil {
			if err == nil {
				err = fmt.Errorf("remove tile staging: %w", cleanupErr)
				return
			}
			err = fmt.Errorf("%w; remove tile staging: %w", err, cleanupErr)
		}
	}()
	if closeErr := staging.Close(); closeErr != nil {
		return fmt.Errorf("close tile staging: %w", closeErr)
	}
	scratchReservation, err := provider.scratch.Reserve(ctx, provider.settings.ScratchMaxBytes+provider.maxBytes)
	if err != nil {
		return fmt.Errorf("reserve tile scratch: %w", err)
	}
	defer func() {
		if releaseErr := scratchReservation.Release(); releaseErr != nil {
			if err == nil {
				err = fmt.Errorf("release tile scratch: %w", releaseErr)
				return
			}
			err = fmt.Errorf("%w; release tile scratch: %w", err, releaseErr)
		}
	}()
	plan, err := worker.BuildQueryPlan(options.Query, provider.fields, options.Snapshot.Schema)
	if err != nil {
		return fmt.Errorf("build tile query plan: %w", err)
	}
	sources, err := provider.cachedSources(key, *options.Query.Square)
	if err != nil {
		return fmt.Errorf("select cached tile sources: %w", err)
	}
	options.SourcePaths = sources.paths
	sourceKind := "upstream"
	if len(sources.paths) > 0 {
		sourceKind = "cache"
	}
	slog.Debug("tile build sources selected", "cell", key.Tile, "catalog_version", key.CatalogVersion, "source_kind", sourceKind, "source_count", len(sources.paths), "source_bytes", sources.sizeBytes)
	result, workerErr := provider.executeTile(ctx, plan, stagingPath, options)
	err = errors.Join(workerErr, sources.Close())
	if err != nil {
		if rejectionErr := provider.rememberSizeRejection(key, err); rejectionErr != nil {
			return fmt.Errorf("%w; remember size rejection: %w", err, rejectionErr)
		}
		return err
	}
	if result.SizeBytes <= 0 || result.Digest == "" {
		return errors.New("build tile returned invalid result")
	}
	if _, err := reservation.Publish(key, stagingPath, options.Snapshot.CatalogVersion); err != nil {
		if result.SizeBytes > provider.maxBytes {
			sizeErr := &worker.OutputTooLargeError{ActualBytes: result.SizeBytes, LimitBytes: provider.maxBytes}
			if errors.Is(err, cache.ErrCapacityUnavailable) {
				slog.Debug("oversized tile not retained", "tile", key.Tile, "size_bytes", result.SizeBytes, "error", err)
				return errors.Join(sizeErr, provider.rememberSizeRejection(key, sizeErr))
			}
			return errors.Join(sizeErr, fmt.Errorf("publish tile: %w", err))
		}
		return fmt.Errorf("publish tile: %w", err)
	}
	published = true
	if result.SizeBytes > provider.maxBytes {
		return &worker.OutputTooLargeError{ActualBytes: result.SizeBytes, LimitBytes: provider.maxBytes}
	}
	return nil
}

func (provider *tileProvider) rememberSizeRejection(key cache.Key, err error) error {
	if errors.Is(err, worker.ErrOutputTooLarge) {
		fallback, proofErr := worker.NewOutputTooLargeError(provider.maxBytes)
		if proofErr != nil {
			return fmt.Errorf("create output size rejection proof: %w", proofErr)
		}
		actualBytes := fallback.ActualBytes
		limitBytes := fallback.LimitBytes
		if outputTooLarge, ok := errors.AsType[*worker.OutputTooLargeError](err); ok && outputTooLarge.ActualBytes > outputTooLarge.LimitBytes {
			actualBytes = outputTooLarge.ActualBytes
			limitBytes = outputTooLarge.LimitBytes
		}
		rejection, rejectionErr := cache.NewSizeRejection(key, cache.RejectionLimitBytes, actualBytes, limitBytes)
		if rejectionErr != nil {
			return rejectionErr
		}
		return provider.negative.Put(rejection)
	}
	if tooManyRows, ok := errors.AsType[*worker.TooManyRowsError](err); ok {
		rejection, rejectionErr := cache.NewSizeRejection(key, cache.RejectionLimitRows, tooManyRows.ActualRows, tooManyRows.LimitRows)
		if rejectionErr != nil {
			return rejectionErr
		}
		return provider.negative.Put(rejection)
	}
	return nil
}

func (provider *tileProvider) observeCatalogVersion(catalogVersion string) error {
	if catalogVersion == "" {
		return errors.New("observe catalog version: version is empty")
	}
	provider.mu.Lock()
	previous := provider.lastCatalogVersion
	provider.lastCatalogVersion = catalogVersion
	provider.mu.Unlock()
	if previous != "" && previous != catalogVersion {
		if err := provider.negative.DeleteCatalogVersion(previous); err != nil {
			return fmt.Errorf("invalidate negative catalog version %q: %w", previous, err)
		}
	}
	return nil
}

func (provider *tileProvider) Close() error {
	if provider == nil {
		return errors.New("close tile provider: provider is nil")
	}
	provider.closeOnce.Do(func() {
		provider.mu.Lock()
		provider.closed = true
		for _, flight := range provider.flights {
			flight.cancel()
		}
		provider.mu.Unlock()
		var closeErrors []error
		if err := provider.scheduler.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close tile scheduler: %w", err))
		}
		provider.flightsWait.Wait()
		if err := provider.negative.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close tile negative cache: %w", err))
		}
		if err := provider.scratch.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close tile scratch: %w", err))
		}
		if err := provider.cache.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close tile cache: %w", err))
		}
		provider.closeErr = errors.Join(closeErrors...)
	})
	return provider.closeErr
}

func (provider *tileProvider) executeTile(ctx context.Context, plan worker.QueryPlan, outputPath string, options tileBuildOptions) (worker.TileResult, error) {
	request := tileExecutionRequest{
		Cell:               plan.CellText,
		Square:             plan.Square,
		Fields:             append([]string(nil), provider.fields...),
		Snapshot:           options.Snapshot,
		SourcePaths:        options.SourcePaths,
		MaxRows:            provider.maxRows,
		OutputPath:         outputPath,
		Settings:           provider.settings,
		ExtensionDirectory: provider.extensionDirectory,
		AssetCAFile:        provider.assetCAFile,
	}
	started := time.Now()
	diagnostics := worker.TileDiagnostics{Stage: "initialize"}
	slog.Debug("tile build started", "tile", options.Query.String(), "release", options.Snapshot.Release)
	result, err := executeTileRequest(ctx, request, &diagnostics)
	slog.Debug("tile build finished", "tile", options.Query.String(), "elapsed_ms", time.Since(started).Milliseconds(), "stage", diagnostics.Stage, "error", err)
	if diagnostics.MaterializeProfile != "" {
		slog.Debug("tile scan profile", "tile", options.Query.String(), "profile", diagnostics.MaterializeProfile)
	}
	return result, err
}

type tileExecutionRequest struct {
	AssetCAFile        string
	ExtensionDirectory string
	Square             *tilequery.Square
	Cell               string
	Fields             []string
	Snapshot           catalog.Snapshot
	MaxRows            int64
	OutputPath         string
	Settings           worker.RuntimeSettings
	SourcePaths        []string
}

func (request tileExecutionRequest) query() tilequery.Query {
	return tilequery.Query{Cell: request.Cell, Square: request.Square}
}

func executeTileRequest(ctx context.Context, request tileExecutionRequest, diagnostics *worker.TileDiagnostics) (result worker.TileResult, err error) {
	if request.OutputPath == "" {
		return worker.TileResult{}, errors.New("worker request output path is empty")
	}
	if err := request.Settings.Validate(); err != nil {
		return worker.TileResult{}, err
	}
	plan, err := worker.BuildQueryPlan(request.query(), request.Fields, request.Snapshot.Schema)
	if err != nil {
		return worker.TileResult{}, err
	}
	if err := os.MkdirAll(request.Settings.ScratchDirectory, 0o750); err != nil {
		return worker.TileResult{}, fmt.Errorf("create scratch root: %w", err)
	}
	scratchDirectory, err := os.MkdirTemp(request.Settings.ScratchDirectory, "build-*")
	if err != nil {
		return worker.TileResult{}, fmt.Errorf("create build scratch directory: %w", err)
	}
	defer func() { err = errors.Join(err, os.RemoveAll(scratchDirectory)) }()
	request.Settings.ScratchDirectory = scratchDirectory
	request.Settings.MaxOutputBytes = math.MaxInt64

	database, err := sql.Open("duckdb", "")
	if err != nil {
		return worker.TileResult{}, fmt.Errorf("open worker DuckDB: %w", err)
	}
	defer func() {
		if closeErr := database.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close worker DuckDB: %w", closeErr)
				return
			}
			err = fmt.Errorf("%w; close worker DuckDB: %w", err, closeErr)
		}
	}()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	connection, err := database.Conn(ctx)
	if err != nil {
		return worker.TileResult{}, fmt.Errorf("get worker DuckDB connection: %w", err)
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close worker DuckDB connection: %w", closeErr)
				return
			}
			err = fmt.Errorf("%w; close worker DuckDB connection: %w", err, closeErr)
		}
	}()
	if err := configureTileConnectionDirectory(ctx, connection, request.ExtensionDirectory); err != nil {
		return worker.TileResult{}, err
	}
	if request.AssetCAFile != "" {
		if _, err := connection.ExecContext(ctx, "SET ca_cert_file = ?", request.AssetCAFile); err != nil {
			return worker.TileResult{}, fmt.Errorf("set asset CA file: %w", err)
		}
		if _, err := connection.ExecContext(ctx, "SET enable_server_cert_verification = true"); err != nil {
			return worker.TileResult{}, fmt.Errorf("enable asset certificate verification: %w", err)
		}
	}
	return worker.BuildTileWithSettings(ctx, worker.RuntimeTileRequest{
		Conn:        connection,
		Plan:        plan,
		Snapshot:    request.Snapshot,
		MaxRows:     request.MaxRows,
		OutputPath:  request.OutputPath,
		Settings:    request.Settings,
		Diagnostics: diagnostics,
		SourcePaths: request.SourcePaths,
	})
}

func rejectionError(rejection cache.SizeRejection) error {
	switch rejection.Limit() {
	case cache.RejectionLimitBytes:
		return &worker.OutputTooLargeError{ActualBytes: rejection.Actual(), LimitBytes: rejection.Threshold()}
	case cache.RejectionLimitRows:
		return &worker.TooManyRowsError{ActualRows: rejection.Actual(), LimitRows: rejection.Threshold()}
	default:
		return fmt.Errorf("negative cache returned unsupported limit %q", rejection.Limit())
	}
}

func configureTileConnectionDirectory(ctx context.Context, connection *sql.Conn, extensionDirectory string) error {
	if ctx == nil {
		return errors.New("configure tile DuckDB connection: context is nil")
	}
	if connection == nil {
		return errors.New("configure tile DuckDB connection: connection is nil")
	}
	if extensionDirectory != "" {
		if _, err := connection.ExecContext(ctx, "SET extension_directory = ?", extensionDirectory); err != nil {
			return fmt.Errorf("set DuckDB extension directory: %w", err)
		}
	}
	for _, statement := range []string{
		"SET autoload_known_extensions = false",
		"SET autoinstall_known_extensions = false",
	} {
		if _, err := connection.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("disable DuckDB extension downloads: query=%q: %w", statement, err)
		}
	}
	for _, extension := range []string{duckdbHTTPFSExtension, duckdbSpatialExtension} {
		if _, err := connection.ExecContext(ctx, "LOAD "+extension); err != nil {
			return fmt.Errorf("load DuckDB extension %q: %w", extension, err)
		}
	}
	if err := h3filter.RegisterCellContains(connection); err != nil {
		return fmt.Errorf("register H3 function: %w", err)
	}
	return nil
}

func sizePolicyID(cfg config.Config) string {
	input := fmt.Sprintf("xyz-v1;bytes=%d;rows=%d;min_resolution=%d;fields=%s", cfg.MaxTileBytes, cfg.MaxTileRows, cfg.MinTileResolution, strings.Join(cfg.Fields, ","))
	digest := sha256.Sum256([]byte(input))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func positiveInt(value int64, name string) (int, error) {
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, value)
	}
	converted := int(value)
	if int64(converted) != value {
		return 0, fmt.Errorf("%s does not fit in int, got %d", name, value)
	}
	return converted, nil
}
