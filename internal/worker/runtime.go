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

	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/geoparquet"
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
		return fmt.Errorf("worker scratch directory must not be empty")
	}
	if !filepath.IsAbs(settings.ScratchDirectory) {
		return fmt.Errorf("worker scratch directory must be absolute, got %q", settings.ScratchDirectory)
	}
	return nil
}

func ApplyRuntimeSettings(ctx context.Context, conn *sql.Conn, settings RuntimeSettings) error {
	if ctx == nil {
		return fmt.Errorf("configure DuckDB: context is nil")
	}
	if conn == nil {
		return fmt.Errorf("configure DuckDB: connection is nil")
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

func BuildTileWithSettings(ctx context.Context, conn *sql.Conn, plan QueryPlan, snapshot catalog.Snapshot, maxRows int64, outputPath string, settings RuntimeSettings) (TileResult, error) {
	if ctx == nil {
		return TileResult{}, fmt.Errorf("build tile with settings: context is nil")
	}
	if err := settings.Validate(); err != nil {
		return TileResult{}, err
	}
	tileContext, cancel := context.WithTimeout(ctx, settings.TileTimeout)
	defer cancel()
	if err := ApplyRuntimeSettings(tileContext, conn, settings); err != nil {
		return TileResult{}, classifyWorkerError(err)
	}
	copyResult, err := BuildTile(tileContext, conn, plan, snapshot, maxRows, outputPath, settings.MaxOutputBytes)
	if err != nil {
		return TileResult{}, classifyWorkerError(err)
	}
	expectedFields := make([]string, 0, len(plan.Columns))
	for _, column := range plan.Columns {
		expectedFields = append(expectedFields, column.Name)
	}
	validation, err := geoparquet.ValidateFile(outputPath, expectedFields, settings.MaxOutputBytes)
	if err != nil {
		return TileResult{}, cleanupInvalidOutput(outputPath, classifyValidationError(err))
	}
	if validation.SizeBytes != copyResult.SizeBytes {
		cause := fmt.Errorf("%w: COPY reported %d bytes, validator found %d", ErrInvalidOutput, copyResult.SizeBytes, validation.SizeBytes)
		return TileResult{}, cleanupInvalidOutput(outputPath, cause)
	}
	return TileResult{
		SizeBytes: validation.SizeBytes,
		RowCount:  validation.RowCount,
		Digest:    validation.Digest,
	}, nil
}

func classifyValidationError(err error) error {
	if errors.Is(err, geoparquet.ErrFileTooLarge) {
		var tooLarge *geoparquet.FileTooLargeError
		if errors.As(err, &tooLarge) {
			return &OutputTooLargeError{ActualBytes: tooLarge.ActualBytes, LimitBytes: tooLarge.LimitBytes}
		}
		return fmt.Errorf("%w: %v", ErrOutputTooLarge, err)
	}
	if errors.Is(err, geoparquet.ErrInvalidGeoParquet) {
		return fmt.Errorf("%w: %v", ErrInvalidOutput, err)
	}
	return err
}

func cleanupInvalidOutput(path string, cause error) error {
	if err := RemoveOutput(path); err != nil {
		return fmt.Errorf("%w; cleanup failed: %v", classifyWorkerError(cause), err)
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
		return fmt.Errorf("%w: %v", ErrTileTimeout, err)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %v", ErrTileCanceled, err)
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrInvalid) || errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EIO) {
		return fmt.Errorf("%w: %v", ErrDiskFailure, err)
	}
	lowerMessage := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lowerMessage, "out of memory"), strings.Contains(lowerMessage, "memory limit"):
		return fmt.Errorf("%w: %v", ErrOutOfMemory, err)
	case strings.Contains(lowerMessage, "no space left"), strings.Contains(lowerMessage, "disk full"), strings.Contains(lowerMessage, "file system"):
		return fmt.Errorf("%w: %v", ErrDiskFailure, err)
	case strings.Contains(lowerMessage, "s3"), strings.Contains(lowerMessage, "httpfs"), strings.Contains(lowerMessage, "http status"), strings.Contains(lowerMessage, "remote file"):
		return fmt.Errorf("%w: %v", ErrUpstream, err)
	default:
		return err
	}
}
