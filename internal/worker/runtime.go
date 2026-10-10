package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/geoparquet"
)

var (
	ErrDiskFailure   = errors.New("worker disk failure")
	ErrOutOfMemory   = errors.New("worker out of memory")
	ErrUpstream      = errors.New("worker upstream failure")
	ErrTileTimeout   = errors.New("worker tile timeout")
	ErrTileCanceled  = errors.New("worker tile canceled")
	ErrInvalidOutput = errors.New("worker output validation failed")
)

type RuntimeSettings struct {
	Profile          bool
	MemoryBytes      int64
	Threads          int64
	ScratchDirectory string
	ScratchMaxBytes  int64
	MaxOutputBytes   int64
	TileTimeout      time.Duration
}

type TileResult struct {
	SizeBytes int64
	RowCount  int64
	Digest    string
}

type RuntimeTileRequest struct {
	Conn        *sql.Conn
	Plan        QueryPlan
	Snapshot    catalog.Snapshot
	MaxRows     int64
	OutputPath  string
	Settings    RuntimeSettings
	Diagnostics *TileDiagnostics
	SourcePaths []string
}

func (settings RuntimeSettings) Validate() error {
	values := []struct {
		name  string
		value int64
	}{
		{name: "memory bytes", value: settings.MemoryBytes},
		{name: "threads", value: settings.Threads},
		{name: "scratch max bytes", value: settings.ScratchMaxBytes},
		{name: "max output bytes", value: settings.MaxOutputBytes},
	}
	for _, value := range values {
		if value.value <= 0 {
			return fmt.Errorf("worker %s must be positive, got %d", value.name, value.value)
		}
	}
	if settings.TileTimeout <= 0 {
		return fmt.Errorf("worker tile timeout must be positive, got %s", settings.TileTimeout)
	}
	if settings.ScratchDirectory == "" {
		return errors.New("worker scratch directory must not be empty")
	}
	if !filepath.IsAbs(settings.ScratchDirectory) {
		return fmt.Errorf("worker scratch directory must be absolute, got %q", settings.ScratchDirectory)
	}
	return nil
}

func ApplyRuntimeSettings(ctx context.Context, conn *sql.Conn, settings RuntimeSettings) error {
	if ctx == nil {
		return errors.New("configure DuckDB: context is nil")
	}
	if conn == nil {
		return errors.New("configure DuckDB: connection is nil")
	}
	if err := settings.Validate(); err != nil {
		return fmt.Errorf("configure DuckDB: %w", err)
	}
	if err := os.MkdirAll(settings.ScratchDirectory, 0o750); err != nil {
		return fmt.Errorf("create worker scratch directory %q: %w", settings.ScratchDirectory, err)
	}
	statements := []struct {
		query string
		arg   any
	}{
		{query: "SET memory_limit = ?", arg: fmt.Sprintf("%dB", settings.MemoryBytes)},
		{query: "SET threads = ?", arg: settings.Threads},
		{query: "SET temp_directory = ?", arg: settings.ScratchDirectory},
		{query: "SET max_temp_directory_size = ?", arg: fmt.Sprintf("%dB", settings.ScratchMaxBytes)},
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement.query, statement.arg); err != nil {
			return fmt.Errorf("configure DuckDB setting %q: %w", statement.query, err)
		}
	}
	return nil
}

func BuildTileWithSettings(ctx context.Context, request RuntimeTileRequest) (TileResult, error) {
	if ctx == nil {
		return TileResult{}, errors.New("build tile with settings: context is nil")
	}
	if err := request.Settings.Validate(); err != nil {
		return TileResult{}, err
	}
	tileContext, cancel := context.WithTimeout(ctx, request.Settings.TileTimeout)
	defer cancel()
	if err := ApplyRuntimeSettings(tileContext, request.Conn, request.Settings); err != nil {
		return TileResult{}, classifyWorkerError(err)
	}
	if request.Diagnostics != nil {
		request.Diagnostics.ProfileEnabled = request.Settings.Profile
	}
	copyResult, err := BuildTile(tileContext, TileBuildRequest{
		Conn:        request.Conn,
		Plan:        request.Plan,
		Snapshot:    request.Snapshot,
		MaxRows:     request.MaxRows,
		OutputPath:  request.OutputPath,
		MaxBytes:    request.Settings.MaxOutputBytes,
		Diagnostics: request.Diagnostics,
		SourcePaths: request.SourcePaths,
	})
	if err != nil {
		return TileResult{}, classifyWorkerError(err)
	}
	expectedFields := make([]string, 0, len(request.Plan.Columns))
	for _, column := range request.Plan.Columns {
		expectedFields = append(expectedFields, column.Name)
	}
	finishValidation := request.Diagnostics.start(tileStageValidation)
	validation, err := geoparquet.ValidateFile(request.OutputPath, expectedFields, request.Settings.MaxOutputBytes)
	finishValidation()
	if err != nil {
		return TileResult{}, cleanupInvalidOutput(request.OutputPath, classifyValidationError(err))
	}
	if validation.SizeBytes != copyResult.SizeBytes {
		cause := fmt.Errorf("%w: COPY reported %d bytes, validator found %d", ErrInvalidOutput, copyResult.SizeBytes, validation.SizeBytes)
		return TileResult{}, cleanupInvalidOutput(request.OutputPath, cause)
	}
	if request.Diagnostics != nil {
		request.Diagnostics.Stage = tileStageComplete
	}
	return TileResult{
		SizeBytes: validation.SizeBytes,
		RowCount:  validation.RowCount,
		Digest:    validation.Digest,
	}, nil
}

func classifyValidationError(err error) error {
	if errors.Is(err, geoparquet.ErrFileTooLarge) {
		if tooLarge, ok := errors.AsType[*geoparquet.FileTooLargeError](err); ok {
			return &OutputTooLargeError{ActualBytes: tooLarge.ActualBytes, LimitBytes: tooLarge.LimitBytes}
		}
		return fmt.Errorf("%w: %w", ErrOutputTooLarge, err)
	}
	if errors.Is(err, geoparquet.ErrInvalidGeoParquet) {
		return fmt.Errorf("%w: %w", ErrInvalidOutput, err)
	}
	return err
}

func cleanupInvalidOutput(path string, cause error) error {
	if err := RemoveOutput(path); err != nil {
		return fmt.Errorf("%w; cleanup failed: %w", classifyWorkerError(cause), err)
	}
	return cause
}

func classifyWorkerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrOutputTooLarge) || errors.Is(err, ErrDiskFailure) || errors.Is(err, ErrOutOfMemory) || errors.Is(err, ErrUpstream) || errors.Is(err, ErrTileTimeout) || errors.Is(err, ErrTileCanceled) || errors.Is(err, ErrInvalidOutput) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrTileTimeout, err)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", ErrTileCanceled, err)
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrInvalid) || errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EIO) {
		return fmt.Errorf("%w: %w", ErrDiskFailure, err)
	}
	lowerMessage := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lowerMessage, "out of memory"), strings.Contains(lowerMessage, "memory limit"):
		return fmt.Errorf("%w: %w", ErrOutOfMemory, err)
	case strings.Contains(lowerMessage, "no space left"), strings.Contains(lowerMessage, "disk full"), strings.Contains(lowerMessage, "file system"):
		return fmt.Errorf("%w: %w", ErrDiskFailure, err)
	case strings.Contains(lowerMessage, "s3"), strings.Contains(lowerMessage, "httpfs"), strings.Contains(lowerMessage, "http status"), strings.Contains(lowerMessage, "remote file"), strings.Contains(lowerMessage, "no files found"), strings.Contains(lowerMessage, "read_parquet"):
		return fmt.Errorf("%w: %w", ErrUpstream, err)
	default:
		return err
	}
}
