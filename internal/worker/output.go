package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
)

var ErrOutputTooLarge = errors.New("COPY output exceeds byte limit")

type OutputTooLargeError struct {
	ActualBytes int64
	LimitBytes  int64
}

func (err *OutputTooLargeError) Error() string {
	return fmt.Sprintf("COPY output is %d bytes, limit is %d bytes", err.ActualBytes, err.LimitBytes)
}

func (*OutputTooLargeError) Unwrap() error {
	return ErrOutputTooLarge
}

type CopyResult struct {
	SizeBytes int64
}

func SetFileSizeLimit(maxBytes int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("file-size limit must be positive, got %d", maxBytes)
	}
	if err := setFileSizeLimit(maxBytes); err != nil {
		return fmt.Errorf("set RLIMIT_FSIZE to %d bytes: %w", maxBytes, err)
	}
	return nil
}

func DisableCopySpill(ctx context.Context, conn *sql.Conn) error {
	if conn == nil {
		return fmt.Errorf("disable COPY spill: nil DuckDB connection")
	}
	statements := []string{
		"SET temp_directory = ''",
		"SET max_temp_directory_size = '0B'",
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute COPY spill guard %q: %w", statement, err)
		}
	}
	return nil
}

func CopyWithOutputLimit(ctx context.Context, conn *sql.Conn, query, outputPath string, maxBytes int64) (CopyResult, error) {
	if conn == nil {
		return CopyResult{}, fmt.Errorf("COPY output guard: nil DuckDB connection")
	}
	if strings.TrimSpace(query) == "" {
		return CopyResult{}, fmt.Errorf("COPY output guard: empty query")
	}
	if outputPath == "" {
		return CopyResult{}, fmt.Errorf("COPY output guard: empty output path")
	}
	if maxBytes <= 0 {
		return CopyResult{}, fmt.Errorf("COPY output guard: byte limit must be positive, got %d", maxBytes)
	}

	if err := DisableCopySpill(ctx, conn); err != nil {
		return CopyResult{}, cleanupAfterFailure(outputPath, err)
	}
	if err := SetFileSizeLimit(maxBytes); err != nil {
		return CopyResult{}, cleanupAfterFailure(outputPath, err)
	}

	if _, err := conn.ExecContext(ctx, query); err != nil {
		return CopyResult{}, cleanupAfterFailure(outputPath, classifyCopyError(err, maxBytes))
	}
	fileInfo, err := os.Stat(outputPath)
	if err != nil {
		return CopyResult{}, cleanupAfterFailure(outputPath, fmt.Errorf("stat COPY output %q: %w", outputPath, err))
	}
	if fileInfo.Size() > maxBytes {
		return CopyResult{}, cleanupAfterFailure(outputPath, &OutputTooLargeError{
			ActualBytes: fileInfo.Size(),
			LimitBytes:  maxBytes,
		})
	}
	return CopyResult{SizeBytes: fileInfo.Size()}, nil
}

func RemoveOutput(path string) error {
	if path == "" {
		return fmt.Errorf("remove output: empty path")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove output %q: %w", path, err)
	}
	return nil
}

func CandidateRowLimit(maxRows int64) (int64, error) {
	const maxInt64 = int64(1<<63 - 1)
	if maxRows < 0 {
		return 0, fmt.Errorf("candidate row limit must not be negative, got %d", maxRows)
	}
	if maxRows == maxInt64 {
		return 0, fmt.Errorf("candidate row limit overflows max_rows+1")
	}
	return maxRows + 1, nil
}

func cleanupAfterFailure(path string, cause error) error {
	if err := RemoveOutput(path); err != nil {
		return fmt.Errorf("%w; cleanup failed: %v", cause, err)
	}
	return cause
}

func classifyCopyError(err error, maxBytes int64) error {
	lowerMessage := strings.ToLower(err.Error())
	if errors.Is(err, syscall.EFBIG) || strings.Contains(lowerMessage, "file size") ||
		strings.Contains(lowerMessage, "file too large") {
		return fmt.Errorf("%w: RLIMIT_FSIZE rejected COPY at %d bytes: %v", ErrOutputTooLarge, maxBytes, err)
	}
	return fmt.Errorf("DuckDB COPY failed: %w", err)
}
