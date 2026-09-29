package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"gotest.tools/v3/assert"
)

const (
	copyHelperEnv         = "OVERTURE_TUNKKI_COPY_HELPER"
	copyHelperModeEnv     = "OVERTURE_TUNKKI_COPY_MODE"
	copyHelperPathEnv     = "OVERTURE_TUNKKI_COPY_PATH"
	copyHelperLimitEnv    = "OVERTURE_TUNKKI_COPY_LIMIT_BYTES"
	copyHelperFailureExit = 42
	copyHelperSpillExit   = 43
	standardCopyMode      = "standard"
	hugeRowCopyMode       = "huge-row"
	spillCopyMode         = "spill"
)

func TestCopyOutputLimitSubprocess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("RLIMIT_FSIZE proof requires Linux")
	}

	t.Run("spill settings are explicit", func(t *testing.T) {
		connection := openDuckDBConnection(t)
		assert.NilError(t, DisableCopySpill(context.Background(), connection))

		var tempDirectory, maxTempDirectorySize string
		err := connection.QueryRowContext(
			context.Background(),
			"SELECT current_setting('temp_directory'), current_setting('max_temp_directory_size')",
		).Scan(&tempDirectory, &maxTempDirectorySize)
		assert.NilError(t, err)
		if err == nil {
			assert.Equal(t, tempDirectory, "")
			assert.Assert(t, strings.Contains(strings.ToLower(maxTempDirectorySize), "0"))
		}
	})

	t.Run("buffered parquet footer and exact limit", func(t *testing.T) {
		testCopyLimitScenario(t, standardCopyMode, 0)
	})

	t.Run("huge individual row", func(t *testing.T) {
		testCopyLimitScenario(t, hugeRowCopyMode, 64*1024)
	})

	t.Run("spill is not an output escape hatch", func(t *testing.T) {
		outputPath := t.TempDir() + "/spill.parquet"
		output, err := runCopyChild(t, spillCopyMode, outputPath, 0)
		assert.Assert(t, err != nil)
		assert.Assert(t, strings.Contains(output, "spill disabled query failed"), output)
		assert.NilError(t, RemoveOutput(outputPath))
	})
}

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

func TestCopyLimitSubprocess(t *testing.T) {
	if os.Getenv(copyHelperEnv) != "1" {
		return
	}

	mode := os.Getenv(copyHelperModeEnv)
	outputPath := os.Getenv(copyHelperPathEnv)
	switch mode {
	case standardCopyMode, hugeRowCopyMode:
		if err := runCopyWorkload(mode, outputPath); err != nil {
			if _, writeErr := fmt.Fprintf(os.Stderr, "copy failed: %v\n", err); writeErr != nil {
				os.Exit(copyHelperFailureExit)
			}
			os.Exit(copyHelperFailureExit)
		}
	case spillCopyMode:
		err := runSpillWorkload(outputPath)
		if err == nil {
			if _, writeErr := fmt.Fprintln(os.Stderr, "spill disabled query unexpectedly succeeded"); writeErr != nil {
				os.Exit(copyHelperSpillExit)
			}
			os.Exit(copyHelperSpillExit)
		}
		if _, writeErr := fmt.Fprintf(os.Stderr, "spill disabled query failed: %v\n", err); writeErr != nil {
			os.Exit(copyHelperSpillExit)
		}
		os.Exit(copyHelperSpillExit)
	default:
		t.Fatalf("unknown copy helper mode %q", mode)
	}
}

func testCopyLimitScenario(t *testing.T, mode string, failureLimitBytes int64) {
	t.Helper()
	probePath := t.TempDir() + "/probe.parquet"
	output, err := runCopyChild(t, mode, probePath, 0)
	assert.NilError(t, err, output)
	if err != nil {
		return
	}
	probeInfo, err := os.Stat(probePath)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, probeInfo.Size() > 1)
	assert.NilError(t, RemoveOutput(probePath))

	exactPath := t.TempDir() + "/exact.parquet"
	output, err = runCopyChild(t, mode, exactPath, probeInfo.Size())
	assert.NilError(t, err, output)
	if err == nil {
		exactInfo, statErr := os.Stat(exactPath)
		assert.NilError(t, statErr)
		if statErr == nil {
			assert.Equal(t, exactInfo.Size(), probeInfo.Size())
		}
	}
	assert.NilError(t, RemoveOutput(exactPath))

	failurePath := t.TempDir() + "/too-small.parquet"
	limitBytes := probeInfo.Size() - 1
	if failureLimitBytes > 0 {
		limitBytes = failureLimitBytes
	}
	output, err = runCopyChild(t, mode, failurePath, limitBytes)
	assert.Assert(t, err != nil, output)
	assert.Assert(t, expectedFileSizeFailure(err, output), output)
	if info, statErr := os.Stat(failurePath); statErr == nil {
		assert.Assert(t, info.Size() <= limitBytes)
	} else {
		assert.Assert(t, errors.Is(statErr, os.ErrNotExist), statErr)
	}
	assert.NilError(t, RemoveOutput(failurePath))
}

func runCopyChild(tb testing.TB, mode, outputPath string, maxBytes int64) (string, error) {
	tb.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCopyLimitSubprocess$", "--")
	command.Env = append(os.Environ(),
		copyHelperEnv+"=1",
		copyHelperModeEnv+"="+mode,
		copyHelperPathEnv+"="+outputPath,
		copyHelperLimitEnv+"="+strconv.FormatInt(maxBytes, 10),
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	return output.String(), err
}

func runCopyWorkload(mode, outputPath string) (runErr error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("close DuckDB: %w", closeErr)
			} else {
				runErr = fmt.Errorf("%w; close DuckDB: %w", runErr, closeErr)
			}
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	connection, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("get DuckDB connection: %w", err)
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("close DuckDB connection: %w", closeErr)
			} else {
				runErr = fmt.Errorf("%w; close DuckDB connection: %w", runErr, closeErr)
			}
		}
	}()

	selectSQL, err := createCopySource(context.Background(), connection, mode)
	if err != nil {
		return err
	}
	copySQL := parquetCopySQL(selectSQL, outputPath)
	maxBytes, err := strconv.ParseInt(os.Getenv(copyHelperLimitEnv), 10, 64)
	if err != nil {
		return fmt.Errorf("parse COPY limit: %w", err)
	}
	if maxBytes == 0 {
		if err := DisableCopySpill(context.Background(), connection); err != nil {
			return err
		}
		if _, err := connection.ExecContext(context.Background(), copySQL); err != nil {
			return fmt.Errorf("probe DuckDB COPY: %w", err)
		}
		return nil
	}
	_, err = CopyWithOutputLimit(context.Background(), CopyRequest{Conn: connection, Query: copySQL, OutputPath: outputPath, MaxBytes: maxBytes})
	return err
}

func runSpillWorkload(outputPath string) (runErr error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("close DuckDB: %w", closeErr)
			} else {
				runErr = fmt.Errorf("%w; close DuckDB: %w", runErr, closeErr)
			}
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	connection, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("get DuckDB connection: %w", err)
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("close DuckDB connection: %w", closeErr)
			} else {
				runErr = fmt.Errorf("%w; close DuckDB connection: %w", runErr, closeErr)
			}
		}
	}()
	if err := DisableCopySpill(context.Background(), connection); err != nil {
		return err
	}
	settings := []string{
		"SET memory_limit = '1MB'",
		"SET threads = 1",
	}
	for _, setting := range settings {
		if _, err := connection.ExecContext(context.Background(), setting); err != nil {
			return fmt.Errorf("set spill workload setting %q: %w", setting, err)
		}
	}
	selectSQL := "SELECT i::BIGINT AS id, md5(i::VARCHAR) AS sort_key FROM range(1000000) AS source(i) ORDER BY sort_key"
	_, err = connection.ExecContext(context.Background(), parquetCopySQL(selectSQL, outputPath))
	return err
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

func expectedFileSizeFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	lowerOutput := strings.ToLower(output)
	if strings.Contains(lowerOutput, "copy failed") || strings.Contains(lowerOutput, "file too large") ||
		strings.Contains(lowerOutput, "file size") {
		return true
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return false
	}
	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	return ok && waitStatus.Signaled() && waitStatus.Signal() == syscall.SIGXFSZ
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
