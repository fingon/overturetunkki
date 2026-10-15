//nolint:goconst // Fixture identities and independent assertions stay readable.
package app

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
	"gotest.tools/v3/assert"
)

func seedSquare(t *testing.T, provider *tileProvider, generation catalog.Generation, square tilequery.Square) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", "geoparquet", "places.parquet"))
	assert.NilError(t, err)
	key := squareKey(provider, generation, square)
	staging, err := provider.cache.CreateStaging(key)
	assert.NilError(t, err)
	assert.NilError(t, staging.Close())
	assert.NilError(t, os.WriteFile(staging.Name(), body, 0o600))
	_, err = provider.cache.Publish(key, staging.Name(), key.CatalogVersion)
	assert.NilError(t, err)
}

func TestCompletedOversizedTilesAreRetained(t *testing.T) {
	if os.Getenv(duckdbExtensionDirectoryEnv) == "" {
		t.Skip("make test supplies pinned extensions")
	}
	for _, kind := range []string{"xyz", "h3"} {
		t.Run(kind, func(t *testing.T) {
			provider := newTestTileProvider(t)
			provider.extensionDirectory = os.Getenv(duckdbExtensionDirectoryEnv)
			generation, h3Query := testH3Generation(t)
			seedSquare(t, provider, generation, tilequery.Square{})
			query := h3Query
			if kind == "xyz" {
				query = tilequery.Query{Square: &tilequery.Square{Z: 1, X: 1, Y: 0}}
			}
			provider.maxBytes = 1
			for range 2 {
				tile, err := provider.Get(t.Context(), generation, query)
				assert.Assert(t, errors.Is(err, worker.ErrOutputTooLarge), err)
				assert.Equal(t, tile.Path, "")
				key := squareKey(provider, generation, tilequery.Square{})
				key.Tile = query.String()
				retained, err := provider.openCachedTile(key)
				assert.NilError(t, err)
				if err != nil {
					return
				}
				validation, err := geoparquet.ValidateFile(retained.Path, provider.fields, math.MaxInt64)
				assert.NilError(t, err)
				assert.Assert(t, validation.SizeBytes > provider.maxBytes)
				assert.NilError(t, retained.Reader.Close())
			}
			assert.Equal(t, provider.cache.Stats().ReservedBytes, int64(0))
			assert.Equal(t, provider.scratch.Stats().ReservedBytes, int64(0))
			// A finer square can use the retained file as a source without an upstream manifest.
			if kind == "xyz" {
				provider.maxBytes = 1 << 20
				child, err := provider.Get(t.Context(), generation, tilequery.Query{Square: &tilequery.Square{Z: 2, X: 2, Y: 1}})
				assert.NilError(t, err)
				if err == nil {
					assert.NilError(t, child.Reader.Close())
				}
			}
		})
	}
}

func TestOversizedTileThatCannotFitCacheIsDiscarded(t *testing.T) {
	if os.Getenv(duckdbExtensionDirectoryEnv) == "" {
		t.Skip("make test supplies pinned extensions")
	}
	provider := newTestTileProvider(t)
	provider.extensionDirectory = os.Getenv(duckdbExtensionDirectoryEnv)
	generation, _ := testH3Generation(t)
	query := tilequery.Query{Square: &tilequery.Square{Z: 1, X: 1, Y: 0}}
	seedSquare(t, provider, generation, tilequery.Square{})
	// Pin the source while testing admission, so it cannot be evicted to fit output.
	source, err := provider.cache.Open(squareKey(provider, generation, tilequery.Square{}))
	assert.NilError(t, err)
	defer func() { assert.NilError(t, source.Close()) }()
	provider.maxBytes = 1
	providerCache := provider.cache
	// A full cache is represented by a second reservation occupying all free bytes.
	remainingBytes := config.DefaultCacheMaxBytes - providerCache.Stats().UsedBytes - 1
	reservation, err := providerCache.Reserve(t.Context(), remainingBytes)
	assert.NilError(t, err)
	defer func() { assert.NilError(t, reservation.Release()) }()
	tile, err := provider.Get(t.Context(), generation, query)
	assert.Assert(t, errors.Is(err, worker.ErrOutputTooLarge), err)
	assert.Equal(t, tile.Path, "")
	key := squareKey(provider, generation, tilequery.Square{})
	key.Tile = query.String()
	_, err = provider.cache.Open(key)
	assert.Assert(t, errors.Is(err, cache.ErrEntryNotFound))
	rejection, found, err := provider.negative.Get(key)
	assert.NilError(t, err)
	assert.Assert(t, found)
	assert.Equal(t, rejection.Limit(), cache.RejectionLimitBytes)
}

func TestConcurrentBuildScratchIsolation(t *testing.T) {
	directory := os.Getenv(duckdbExtensionDirectoryEnv)
	if directory == "" {
		t.Skip("make test supplies pinned extensions")
	}
	generation, query := testH3Generation(t)
	scratchRoot := t.TempDir()
	sourcePath, err := filepath.Abs(filepath.Join("..", "..", "testdata", "geoparquet", "places.parquet"))
	assert.NilError(t, err)
	start := make(chan struct{})
	errorsDone := make(chan error, 2)
	for range 2 {
		request := tileExecutionRequest{
			Cell: query.Cell, Fields: []string{"id", "geometry", "names"}, Snapshot: generation.Snapshot,
			SourcePaths: []string{sourcePath},
			OutputPath:  filepath.Join(t.TempDir(), "tile.parquet"), MaxRows: 100_000, ExtensionDirectory: directory,
			Settings: worker.RuntimeSettings{MemoryBytes: 64 << 20, Threads: 1, ScratchDirectory: scratchRoot, ScratchMaxBytes: 1 << 20, MaxOutputBytes: 8 << 20, TileTimeout: 10 * time.Second},
		}
		go func() { <-start; _, err := executeTileRequest(t.Context(), request, nil); errorsDone <- err }()
	}
	close(start)
	for range 2 {
		assert.NilError(t, <-errorsDone)
	}
	directories, err := os.ReadDir(scratchRoot)
	assert.NilError(t, err)
	assert.Equal(t, len(directories), 0)
}
