//nolint:tagliatelle // Worker protocol fields use the documented snake_case schema.
package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	// Register DuckDB's database/sql driver.
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/fingon/overturetunkki/internal/httpapi"
	"github.com/fingon/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
)

const (
	duckdbHTTPFSExtension             = "httpfs"
	duckdbSpatialExtension            = "spatial"
	duckdbExtensionDirectoryEnv       = "OVERTURE_DUCKDB_EXTENSION_DIRECTORY"
	duckdbDataPathEnv                 = "KO_DATA_PATH"
	workerProtocolEnv                 = "OVERTURE_WORKER_PROTOCOL"
	workerProtocolValue               = "tile-v1"
	workerProtocolMaxBytes      int64 = 1 << 20
)

var errWorkerProtocolResponseTooLarge = errors.New("isolated worker response exceeds protocol limit")

type tileProvider struct {
	cache      *cache.Cache
	negative   *cache.NegativeCache
	scheduler  *cache.Scheduler
	scratch    *cache.ScratchPool
	executable string
	cacheDir   string
	fields     []string
	maxRows    int64
	maxBytes   int64
	settings   worker.RuntimeSettings
	policyID   string

	mu                 sync.Mutex
	lastCatalogVersion string
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
	if perWorkerScratchBytes <= 0 {
		return nil, errors.New("create tile provider: per-worker scratch capacity is not positive")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate tile worker executable: %w", err)
	}
	if !filepath.IsAbs(executable) {
		return nil, fmt.Errorf("tile worker executable is not absolute: %q", executable)
	}
	scheduler, err := cache.NewScheduler(tileCache, cache.SchedulerOptions{Workers: workerCount, QueueCapacity: queueCapacity})
	if err != nil {
		return nil, err
	}
	settings := worker.RuntimeSettings{
		MemoryBytes:      cfg.WorkerMemoryBytes,
		Threads:          cfg.WorkerThreads,
		ScratchDirectory: filepath.Join(cfg.CacheDir, "scratch"),
		ScratchMaxBytes:  perWorkerScratchBytes,
		MaxOutputBytes:   cfg.MaxTileBytes,
		TileTimeout:      cfg.TileTimeout,
	}
	provider := &tileProvider{
		cache:      tileCache,
		negative:   negative,
		scheduler:  scheduler,
		scratch:    scratch,
		executable: executable,
		cacheDir:   cfg.CacheDir,
		fields:     append([]string(nil), cfg.Fields...),
		maxRows:    cfg.MaxTileRows,
		maxBytes:   cfg.MaxTileBytes,
		settings:   settings,
		policyID:   sizePolicyID(cfg),
	}
	return provider, nil
}

func (provider *tileProvider) Get(ctx context.Context, snapshot catalog.Snapshot, cell h3.Cell) (httpapi.Tile, error) {
	if provider == nil {
		return httpapi.Tile{}, errors.New("get tile: provider is nil")
	}
	if ctx == nil {
		return httpapi.Tile{}, errors.New("get tile: context is nil")
	}
	if !cell.IsValid() {
		return httpapi.Tile{}, errors.New("get tile: cell is invalid")
	}
	if err := provider.observeCatalogVersion(snapshot.CatalogVersion); err != nil {
		return httpapi.Tile{}, err
	}
	key := cache.Key{
		CatalogVersion: snapshot.CatalogVersion,
		ProjectionID:   snapshot.ProjectionID,
		Cell:           cell.String(),
		SizePolicyID:   provider.policyID,
	}
	if rejection, ok, err := provider.negative.Get(key); err != nil {
		return httpapi.Tile{}, fmt.Errorf("check tile negative cache: %w", err)
	} else if ok {
		return httpapi.Tile{}, rejectionError(rejection)
	}
	if tile, err := provider.openCachedTile(key); err == nil {
		return tile, nil
	} else if !errors.Is(err, cache.ErrEntryNotFound) {
		return httpapi.Tile{}, err
	}
	if err := provider.scheduler.Do(ctx, key, provider.maxBytes, func(buildContext context.Context, reservation *cache.Reservation) error {
		return provider.build(buildContext, reservation, key, tileBuildOptions{Snapshot: snapshot, Cell: cell})
	}); err != nil {
		return httpapi.Tile{}, err
	}
	return provider.openCachedTile(key)
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
	Snapshot catalog.Snapshot
	Cell     h3.Cell
}

func (provider *tileProvider) build(ctx context.Context, reservation *cache.Reservation, key cache.Key, options tileBuildOptions) (err error) {
	if reservation == nil {
		return errors.New("build tile: cache reservation is nil")
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
	scratchReservation, err := provider.scratch.Reserve(ctx, provider.settings.ScratchMaxBytes)
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
	plan, err := worker.BuildQueryPlan(worker.TileRequest{Cell: options.Cell.String()}, provider.fields, options.Snapshot.Schema)
	if err != nil {
		return fmt.Errorf("build tile query plan: %w", err)
	}
	result, err := provider.runWorker(ctx, plan, options.Snapshot, stagingPath)
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
		return fmt.Errorf("publish tile: %w", err)
	}
	published = true
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
		var closeErrors []error
		if err := provider.scheduler.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close tile scheduler: %w", err))
		}
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

func (provider *tileProvider) runWorker(ctx context.Context, plan worker.QueryPlan, snapshot catalog.Snapshot, outputPath string) (worker.TileResult, error) {
	request := tileWorkerRequest{
		Cell:       plan.CellText,
		Fields:     append([]string(nil), provider.fields...),
		Snapshot:   snapshot,
		MaxRows:    provider.maxRows,
		OutputPath: outputPath,
		Settings:   provider.settings,
	}
	return runIsolatedTileWorker(ctx, provider.executable, provider.cacheDir, request)
}

type tileWorkerRequest struct {
	Cell       string                 `json:"cell"`
	Fields     []string               `json:"fields"`
	Snapshot   catalog.Snapshot       `json:"snapshot"`
	MaxRows    int64                  `json:"max_rows"`
	OutputPath string                 `json:"output_path"`
	Settings   worker.RuntimeSettings `json:"settings"`
}

type tileWorkerResponse struct {
	Result *worker.TileResult `json:"result,omitempty"`
	Error  *tileWorkerError   `json:"error,omitempty"`
}

type tileWorkerError struct {
	Kind        string `json:"kind"`
	Message     string `json:"message"`
	ActualBytes int64  `json:"actual_bytes,omitempty"`
	LimitBytes  int64  `json:"limit_bytes,omitempty"`
	ActualRows  int64  `json:"actual_rows,omitempty"`
	LimitRows   int64  `json:"limit_rows,omitempty"`
}

const (
	workerErrorGeneric        = "generic"
	workerErrorOutputTooLarge = "output_too_large"
	workerErrorTooManyRows    = "too_many_rows"
	workerErrorDisk           = "disk"
	workerErrorMemory         = "memory"
	workerErrorUpstream       = "upstream"
	workerErrorTimeout        = "timeout"
	workerErrorCanceled       = "canceled"
	workerErrorInvalidOutput  = "invalid_output"
)

func runIsolatedTileWorker(ctx context.Context, executable, cacheDir string, request tileWorkerRequest) (worker.TileResult, error) {
	if ctx == nil {
		return worker.TileResult{}, errors.New("run isolated tile worker: context is nil")
	}
	if executable == "" {
		return worker.TileResult{}, errors.New("run isolated tile worker: executable is empty")
	}
	if cacheDir == "" {
		return worker.TileResult{}, errors.New("run isolated tile worker: cache directory is empty")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return worker.TileResult{}, fmt.Errorf("encode isolated tile request: %w", err)
	}
	if int64(len(payload)) > workerProtocolMaxBytes {
		return worker.TileResult{}, fmt.Errorf("encode isolated tile request: payload exceeds %d bytes", workerProtocolMaxBytes)
	}
	command := exec.CommandContext(ctx, executable, "--mode=worker", "--cache-dir", cacheDir)
	command.Env = append(os.Environ(), workerProtocolEnv+"="+workerProtocolValue)
	command.Stdin = bytes.NewReader(payload)
	var output boundedProtocolBuffer
	output.limit = workerProtocolMaxBytes
	var diagnostics boundedProtocolBuffer
	diagnostics.limit = workerProtocolMaxBytes
	command.Stdout = &output
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return worker.TileResult{}, fmt.Errorf("%w: isolated tile worker: %w", worker.ErrTileCanceled, ctx.Err())
		}
		if workerExitedOnFileSizeLimit(err) {
			outputTooLarge, proofErr := worker.NewOutputTooLargeError(request.Settings.MaxOutputBytes)
			if proofErr == nil {
				return worker.TileResult{}, fmt.Errorf("%w: isolated tile worker exited: %w", outputTooLarge, err)
			}
			return worker.TileResult{}, fmt.Errorf("%w: isolated tile worker exited: %w", worker.ErrOutputTooLarge, err)
		}
		if diagnostics.Len() != 0 {
			return worker.TileResult{}, fmt.Errorf("isolated tile worker exited: %w: %s", err, strings.TrimSpace(diagnostics.String()))
		}
		return worker.TileResult{}, fmt.Errorf("isolated tile worker exited: %w", err)
	}
	if err := output.Err(); err != nil {
		return worker.TileResult{}, fmt.Errorf("read isolated tile worker response: %w", err)
	}
	var response tileWorkerResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		return worker.TileResult{}, fmt.Errorf("decode isolated tile worker response: %w", err)
	}
	if response.Error != nil {
		return worker.TileResult{}, decodeTileWorkerError(*response.Error)
	}
	if response.Result == nil {
		return worker.TileResult{}, errors.New("decode isolated tile worker response: result and error are both absent")
	}
	return *response.Result, nil
}

func workerExitedOnFileSizeLimit(err error) bool {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ProcessState == nil {
		return false
	}
	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	return ok && waitStatus.Signaled() && waitStatus.Signal() == syscall.SIGXFSZ
}

type boundedProtocolBuffer struct {
	bytes.Buffer
	limit int64
	err   error
}

func (buffer *boundedProtocolBuffer) Write(value []byte) (int, error) {
	if buffer.err != nil {
		return 0, buffer.err
	}
	if int64(buffer.Len()+len(value)) > buffer.limit {
		buffer.err = errWorkerProtocolResponseTooLarge
		return 0, buffer.err
	}
	return buffer.Buffer.Write(value)
}

func (buffer *boundedProtocolBuffer) Err() error {
	return buffer.err
}

func encodeTileWorkerErrorWithLimit(err error, maxOutputBytes int64) tileWorkerError {
	result := tileWorkerError{Kind: workerErrorGeneric, Message: err.Error()}
	var outputTooLarge *worker.OutputTooLargeError
	var tooManyRows *worker.TooManyRowsError
	switch {
	case errors.As(err, &outputTooLarge):
		result.Kind = workerErrorOutputTooLarge
		result.ActualBytes = outputTooLarge.ActualBytes
		result.LimitBytes = outputTooLarge.LimitBytes
	case errors.As(err, &tooManyRows):
		result.Kind = workerErrorTooManyRows
		result.ActualRows = tooManyRows.ActualRows
		result.LimitRows = tooManyRows.LimitRows
	case errors.Is(err, worker.ErrOutputTooLarge):
		result.Kind = workerErrorOutputTooLarge
		if outputTooLarge, proofErr := worker.NewOutputTooLargeError(maxOutputBytes); proofErr == nil {
			result.ActualBytes = outputTooLarge.ActualBytes
			result.LimitBytes = outputTooLarge.LimitBytes
		}
	case errors.Is(err, worker.ErrDiskFailure):
		result.Kind = workerErrorDisk
	case errors.Is(err, worker.ErrOutOfMemory):
		result.Kind = workerErrorMemory
	case errors.Is(err, worker.ErrUpstream):
		result.Kind = workerErrorUpstream
	case errors.Is(err, worker.ErrTileTimeout):
		result.Kind = workerErrorTimeout
	case errors.Is(err, worker.ErrTileCanceled):
		result.Kind = workerErrorCanceled
	case errors.Is(err, worker.ErrInvalidOutput):
		result.Kind = workerErrorInvalidOutput
	}
	return result
}

func decodeTileWorkerError(encoded tileWorkerError) error {
	message := encoded.Message
	if message == "" {
		message = "isolated tile worker failed"
	}
	switch encoded.Kind {
	case workerErrorOutputTooLarge:
		return &worker.OutputTooLargeError{ActualBytes: encoded.ActualBytes, LimitBytes: encoded.LimitBytes}
	case workerErrorTooManyRows:
		return &worker.TooManyRowsError{ActualRows: encoded.ActualRows, LimitRows: encoded.LimitRows}
	case workerErrorDisk:
		return fmt.Errorf("%w: %s", worker.ErrDiskFailure, message)
	case workerErrorMemory:
		return fmt.Errorf("%w: %s", worker.ErrOutOfMemory, message)
	case workerErrorUpstream:
		return fmt.Errorf("%w: %s", worker.ErrUpstream, message)
	case workerErrorTimeout:
		return fmt.Errorf("%w: %s", worker.ErrTileTimeout, message)
	case workerErrorCanceled:
		return fmt.Errorf("%w: %s", worker.ErrTileCanceled, message)
	case workerErrorInvalidOutput:
		return fmt.Errorf("%w: %s", worker.ErrInvalidOutput, message)
	default:
		return errors.New(message)
	}
}

func runWorkerProtocol(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run worker protocol: context is nil")
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, workerProtocolMaxBytes))
	var request tileWorkerRequest
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode worker request: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("decode worker request: multiple messages")
		}
		return fmt.Errorf("decode worker request trailer: %w", err)
	}
	result, err := executeWorkerRequest(ctx, request)
	response := tileWorkerResponse{Result: &result}
	if err != nil {
		response.Result = nil
		encoded := encodeTileWorkerErrorWithLimit(err, request.Settings.MaxOutputBytes)
		response.Error = &encoded
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		return fmt.Errorf("encode worker response: %w", err)
	}
	return nil
}

func executeWorkerRequest(ctx context.Context, request tileWorkerRequest) (result worker.TileResult, err error) {
	if request.OutputPath == "" {
		return worker.TileResult{}, errors.New("worker request output path is empty")
	}
	if err := request.Settings.Validate(); err != nil {
		return worker.TileResult{}, err
	}
	plan, err := worker.BuildQueryPlan(worker.TileRequest{Cell: request.Cell}, request.Fields, request.Snapshot.Schema)
	if err != nil {
		return worker.TileResult{}, err
	}
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
	if err := configureTileConnection(ctx, connection); err != nil {
		return worker.TileResult{}, err
	}
	return worker.BuildTileWithSettings(ctx, worker.RuntimeTileRequest{
		Conn:       connection,
		Plan:       plan,
		Snapshot:   request.Snapshot,
		MaxRows:    request.MaxRows,
		OutputPath: request.OutputPath,
		Settings:   request.Settings,
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

func configureTileConnection(ctx context.Context, connection *sql.Conn) error {
	if ctx == nil {
		return errors.New("configure tile DuckDB connection: context is nil")
	}
	if connection == nil {
		return errors.New("configure tile DuckDB connection: connection is nil")
	}
	if extensionDirectory := os.Getenv(duckdbExtensionDirectoryEnv); extensionDirectory != "" {
		if _, err := connection.ExecContext(ctx, "SET extension_directory = ?", extensionDirectory); err != nil {
			return fmt.Errorf("set DuckDB extension directory: %w", err)
		}
	} else if dataPath := os.Getenv(duckdbDataPathEnv); dataPath != "" {
		directory := filepath.Join(dataPath, "extensions")
		if _, err := connection.ExecContext(ctx, "SET extension_directory = ?", directory); err != nil {
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
	input := fmt.Sprintf("bytes=%d;rows=%d;fields=%s", cfg.MaxTileBytes, cfg.MaxTileRows, strings.Join(cfg.Fields, ","))
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
