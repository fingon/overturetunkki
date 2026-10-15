package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sys/unix"
	"gotest.tools/v3/assert"
)

const (
	standardCopyMode = "standard"
	hugeRowCopyMode  = "huge-row"
)

func TestCandidateRowLimit(t *testing.T) {
	connection := openDuckDBConnection(t)
	cases := []struct {
		name    string
		maxRows int64
		want    int64
	}{
		{name: "zero rows still probes one", maxRows: 0, want: 1},
		{name: "ordinary limit", maxRows: 7, want: 8},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := CandidateRowLimit(test.maxRows)
			assert.NilError(t, err)
			if err == nil {
				assert.Equal(t, got, test.want)
			}
		})
	}

	for _, maxRows := range []int64{-1, int64(1<<63 - 1)} {
		_, err := CandidateRowLimit(maxRows)
		assert.Assert(t, err != nil)
	}

	boundedLimit, err := CandidateRowLimit(2)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	rows, err := connection.QueryContext(
		context.Background(),
		"SELECT i::BIGINT FROM range(1000000) AS source(i) LIMIT ?",
		boundedLimit,
	)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() { assert.NilError(t, rows.Close()) }()
	var rowCount int
	for rows.Next() {
		var value int64
		assert.NilError(t, rows.Scan(&value))
		rowCount++
	}
	assert.NilError(t, rows.Err())
	assert.Equal(t, rowCount, int(boundedLimit))
}

func TestClassifyCopyErrorRetainsOutputSizeProof(t *testing.T) {
	err := classifyCopyError(syscall.EFBIG, 1024)
	assert.Assert(t, errors.Is(err, ErrOutputTooLarge))
	var outputTooLarge *OutputTooLargeError
	assert.Assert(t, errors.As(err, &outputTooLarge))
	if outputTooLarge != nil {
		assert.Equal(t, outputTooLarge.ActualBytes, int64(1025))
		assert.Equal(t, outputTooLarge.LimitBytes, int64(1024))
	}
}

func TestCopyLimitsDoNotChangeHostLimits(t *testing.T) {
	var before unix.Rlimit
	assert.NilError(t, unix.Getrlimit(unix.RLIMIT_FSIZE, &before))
	for _, mode := range []string{standardCopyMode, hugeRowCopyMode} {
		t.Run(mode, func(t *testing.T) {
			connection := openDuckDBConnection(t)
			source, err := createCopySource(t.Context(), connection, mode)
			assert.NilError(t, err)
			outputPath := filepath.Join(t.TempDir(), "tile.parquet")
			request := CopyRequest{Conn: connection, Query: parquetCopySQL(source, outputPath), OutputPath: outputPath, MaxBytes: math.MaxInt64}
			result, err := CopyWithOutputLimit(t.Context(), request)
			assert.NilError(t, err)
			request.MaxBytes = result.SizeBytes
			assert.NilError(t, RemoveOutput(outputPath))
			_, err = CopyWithOutputLimit(t.Context(), request)
			assert.NilError(t, err)
			assert.NilError(t, RemoveOutput(outputPath))
			request.MaxBytes--
			_, err = CopyWithOutputLimit(t.Context(), request)
			assert.Assert(t, errors.Is(err, ErrOutputTooLarge))
			_, err = os.Stat(outputPath)
			assert.Assert(t, errors.Is(err, os.ErrNotExist))
		})
	}
	var after unix.Rlimit
	assert.NilError(t, unix.Getrlimit(unix.RLIMIT_FSIZE, &after))
	assert.Equal(t, before, after)
}

func TestCopyCancellationAndRecovery(t *testing.T) {
	connection := openDuckDBConnection(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
	request := CopyRequest{Conn: connection, Query: parquetCopySQL("SELECT 1 AS id", outputPath), OutputPath: outputPath, MaxBytes: 1 << 20}
	_, err := CopyWithOutputLimit(ctx, request)
	assert.Assert(t, err != nil)
	_, err = CopyWithOutputLimit(t.Context(), request)
	assert.NilError(t, err)
}

func createCopySource(ctx context.Context, connection *sql.Conn, mode string) (string, error) {
	switch mode {
	case standardCopyMode:
		return "SELECT i::BIGINT AS id, repeat('x', 128) AS payload FROM range(250000) AS source(i)", nil
	case hugeRowCopyMode:
		if _, err := connection.ExecContext(ctx, "CREATE TABLE huge_rows (id INTEGER, payload BLOB)"); err != nil {
			return "", fmt.Errorf("create huge-row source: %w", err)
		}
		payload := make([]byte, 2*1024*1024)
		state := uint32(0x9e3779b9)
		for index := range payload {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			payload[index] = byte(state)
		}
		if _, err := connection.ExecContext(ctx, "INSERT INTO huge_rows VALUES (?, ?)", 1, payload); err != nil {
			return "", fmt.Errorf("insert huge-row source: %w", err)
		}
		return "SELECT id, payload FROM huge_rows", nil
	default:
		return "", fmt.Errorf("unknown COPY source mode %q", mode)
	}
}

func parquetCopySQL(selectSQL, outputPath string) string {
	quotedPath := strings.ReplaceAll(outputPath, "'", "''")
	return fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", selectSQL, quotedPath)
}

func openDuckDBConnection(tb testing.TB) *sql.Conn {
	tb.Helper()
	db, err := sql.Open("duckdb", "")
	assert.NilError(tb, err)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	tb.Cleanup(func() { assert.NilError(tb, db.Close()) })
	connection, err := db.Conn(context.Background())
	assert.NilError(tb, err)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { assert.NilError(tb, connection.Close()) })
	return connection
}
