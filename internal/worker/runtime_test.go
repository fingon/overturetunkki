package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestRuntimeSettingsValidate(t *testing.T) {
	valid := RuntimeSettings{
		MemoryBytes:      64 * 1024 * 1024,
		Threads:          1,
		ScratchDirectory: t.TempDir(),
		ScratchMaxBytes:  1024 * 1024,
		MaxOutputBytes:   1024 * 1024,
		TileTimeout:      time.Second,
	}
	cases := []struct {
		name   string
		mutate func(*RuntimeSettings)
		want   string
	}{
		{name: "memory", mutate: func(settings *RuntimeSettings) { settings.MemoryBytes = 0 }, want: "memory bytes"},
		{name: "threads", mutate: func(settings *RuntimeSettings) { settings.Threads = 0 }, want: "threads"},
		{name: "scratch directory", mutate: func(settings *RuntimeSettings) { settings.ScratchDirectory = "relative" }, want: "absolute"},
		{name: "scratch bytes", mutate: func(settings *RuntimeSettings) { settings.ScratchMaxBytes = 0 }, want: "scratch max bytes"},
		{name: "output bytes", mutate: func(settings *RuntimeSettings) { settings.MaxOutputBytes = 0 }, want: "max output bytes"},
		{name: "tile timeout", mutate: func(settings *RuntimeSettings) { settings.TileTimeout = 0 }, want: "tile timeout"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			settings := valid
			test.mutate(&settings)
			assert.ErrorContains(t, settings.Validate(), test.want)
		})
	}
}

func TestApplyRuntimeSettings(t *testing.T) {
	connection := openDuckDBConnection(t)
	settings := RuntimeSettings{
		MemoryBytes:      64 * 1024 * 1024,
		Threads:          1,
		ScratchDirectory: t.TempDir(),
		ScratchMaxBytes:  1024 * 1024,
		MaxOutputBytes:   1024 * 1024,
		TileTimeout:      time.Second,
	}
	err := ApplyRuntimeSettings(context.Background(), connection, settings)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	var memoryLimit, threads, tempDirectory, scratchLimit string
	err = connection.QueryRowContext(
		context.Background(),
		"SELECT current_setting('memory_limit'), current_setting('threads'), current_setting('temp_directory'), current_setting('max_temp_directory_size')",
	).Scan(&memoryLimit, &threads, &tempDirectory, &scratchLimit)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, strings.Contains(memoryLimit, "64"), memoryLimit)
	assert.Equal(t, threads, "1")
	assert.Equal(t, tempDirectory, settings.ScratchDirectory)
	assert.Assert(t, strings.Contains(scratchLimit, "1"), scratchLimit)
}

func TestClassifyWorkerError(t *testing.T) {
	deadline := context.DeadlineExceeded
	cases := []struct {
		name string
		err  error
		want error
	}{
		{name: "deadline", err: deadline, want: ErrTileTimeout},
		{name: "canceled", err: context.Canceled, want: ErrTileCanceled},
		{name: "disk", err: syscall.ENOSPC, want: ErrDiskFailure},
		{name: "memory", err: errors.New("Out of Memory Error: could not allocate block"), want: ErrOutOfMemory},
		{name: "upstream", err: errors.New("HTTP status 503 while reading S3 object"), want: ErrUpstream},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			classified := classifyWorkerError(fmt.Errorf("query failed: %w", test.err))
			assert.Assert(t, errors.Is(classified, test.want), classified)
		})
	}
}
