//nolint:goconst // Fixture identities and independent assertions stay readable.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/httpapi"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestMain(tests *testing.M) {
	if os.Getenv(workerProtocolEnv) == workerProtocolValue {
		if err := runWorkerProtocol(context.Background()); err != nil {
			slog.Error("test worker protocol failed", "error", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(tests.Run())
}

func newTestTileProvider(t *testing.T) *tileProvider {
	t.Helper()
	cfg, err := config.Parse([]string{"--cache-dir=" + t.TempDir(), "--worker-count=1", "--fields=id,geometry,names", "--tile-timeout=10s"})
	assert.NilError(t, err)
	tileCache, err := cache.New(cache.Options{Root: cfg.CacheDir, MaxBytes: cfg.CacheMaxBytes, MaxEntries: cfg.CacheMaxEntries})
	assert.NilError(t, err)
	negative, err := cache.NewNegativeCache(int(cfg.NegativeCacheEntries), cfg.NegativeCacheTTL)
	assert.NilError(t, err)
	scratch, err := cache.NewScratchPool(cfg.ScratchMaxBytes)
	assert.NilError(t, err)
	provider, err := newTileProvider(cfg, tileCache, negative, scratch)
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, provider.Close()) })
	return provider
}

func testH3Generation(t *testing.T) (catalog.Generation, tilequery.Query) {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 60.171, Lng: 24.941}, 9)
	assert.NilError(t, err)
	return catalog.Generation{Context: t.Context(), Snapshot: catalog.Snapshot{CatalogVersion: "catalog", ProjectionID: "projection", Schema: catalog.Schema{PrimaryGeometry: "geometry", Columns: []catalog.Column{{Name: "id"}, {Name: "geometry"}, {Name: "names"}}}}}, tilequery.Query{Cell: cell.String()}
}

func squareKey(provider *tileProvider, generation catalog.Generation, square tilequery.Square) cache.Key {
	return cache.Key{CatalogVersion: generation.Snapshot.CatalogVersion, ProjectionID: generation.Snapshot.ProjectionID, Tile: square.String(), SizePolicyID: provider.policyID}
}

func TestH3StopsAtOversizedSourceWithoutRecursion(t *testing.T) {
	for _, limit := range []cache.RejectionLimit{cache.RejectionLimitRows, cache.RejectionLimitBytes} {
		t.Run(string(limit), func(t *testing.T) {
			provider := newTestTileProvider(t)
			generation, query := testH3Generation(t)
			cover, err := h3SquareCover(query)
			assert.NilError(t, err)
			assert.Assert(t, len(cover) > 0 && len(cover) <= tilequery.MaxSources)
			key := squareKey(provider, generation, cover[0])
			rejection, err := cache.NewSizeRejection(key, limit, 2, 1)
			assert.NilError(t, err)
			assert.NilError(t, provider.negative.Put(rejection))
			_, err = provider.Get(t.Context(), generation, query)
			sourceError, ok := errors.AsType[*worker.SourceSquareError](err)
			assert.Assert(t, ok, err)
			assert.Equal(t, sourceError.Square, cover[0])
			h3Key := key
			h3Key.Tile = query.Cell
			_, remembered, getErr := provider.negative.Get(h3Key)
			assert.NilError(t, getErr)
			assert.Assert(t, !remembered)
			assert.Equal(t, provider.cache.Stats().UsedEntries, int64(0))
			assert.Equal(t, provider.scratch.Stats().ReservedBytes, int64(0))
			// No executable or upstream snapshot is usable; a replacement fetch would fail differently.
		})
	}
}

func TestH3UsesItsOwnProvenSizeRejection(t *testing.T) {
	provider := newTestTileProvider(t)
	generation, query := testH3Generation(t)
	key := cache.Key{CatalogVersion: generation.Snapshot.CatalogVersion, ProjectionID: generation.Snapshot.ProjectionID, Tile: query.Cell, SizePolicyID: provider.policyID}
	assert.NilError(t, provider.rememberSizeRejection(key, &worker.TooManyRowsError{ActualRows: provider.maxRows + 1, LimitRows: provider.maxRows}))
	_, err := provider.Get(t.Context(), generation, query)
	assert.Assert(t, errors.Is(err, worker.ErrTooManyRows))
	assert.Equal(t, len(provider.flights), 0)
	assert.Equal(t, provider.cache.Stats().UsedEntries, int64(0))
}

func TestH3TemporaryResultSharingAndCleanup(t *testing.T) {
	if os.Getenv(duckdbExtensionDirectoryEnv) == "" {
		t.Skip("requires bundled DuckDB extensions")
	}
	provider := newTestTileProvider(t)
	generation, query := testH3Generation(t)
	cover, err := h3SquareCover(query)
	assert.NilError(t, err)
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "geoparquet", "places.parquet"))
	assert.NilError(t, err)
	for _, square := range cover {
		key := squareKey(provider, generation, square)
		staging, createErr := provider.cache.CreateStaging(key)
		assert.NilError(t, createErr)
		assert.NilError(t, staging.Close())
		assert.NilError(t, os.WriteFile(staging.Name(), fixture, 0o600))
		_, publishErr := provider.cache.Publish(key, staging.Name(), key.CatalogVersion)
		assert.NilError(t, publishErr)
	}
	type result struct {
		tile httpapi.Tile
		err  error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			tile, getErr := provider.Get(t.Context(), generation, query)
			results <- result{tile: tile, err: getErr}
		}()
	}
	firstResult, secondResult := <-results, <-results
	assert.NilError(t, firstResult.err)
	assert.NilError(t, secondResult.err)
	first, second := firstResult.tile, secondResult.tile
	assert.Equal(t, first.Path, second.Path)
	validation, err := geoparquet.ValidateFile(first.Path, provider.fields, provider.maxBytes)
	assert.NilError(t, err)
	assert.Equal(t, validation.RowCount, int64(1))
	assert.Equal(t, provider.cache.Stats().UsedEntries, int64(len(cover)))
	assert.Equal(t, provider.scratch.Stats().ReservedBytes, first.SizeBytes)
	assert.NilError(t, first.Reader.Close())
	_, err = os.Stat(second.Path)
	assert.NilError(t, err)
	assert.NilError(t, second.Reader.Close())
	_, err = os.Stat(second.Path)
	assert.Assert(t, errors.Is(err, os.ErrNotExist))
	assert.Equal(t, provider.scratch.Stats().ReservedBytes, int64(0))
	third, err := provider.Get(t.Context(), generation, query)
	assert.NilError(t, err)
	assert.Equal(t, third.Digest, first.Digest)
	assert.NilError(t, third.Reader.Close())
}

func TestH3CallerCancellationRemovesFlight(t *testing.T) {
	provider := newTestTileProvider(t)
	generation, query := testH3Generation(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := provider.Get(ctx, generation, query)
	assert.Assert(t, errors.Is(err, context.Canceled))
	provider.mu.Lock()
	assert.Equal(t, len(provider.flights), 0)
	provider.mu.Unlock()
}

func TestH3CoverForPolarAndBoundaryCells(t *testing.T) {
	for _, point := range []h3.LatLng{{Lat: 90}, {Lat: -90}, {Lat: 0, Lng: 180}, {Lat: 85, Lng: 45}} {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			cell, err := h3.LatLngToCell(point, 9)
			assert.NilError(t, err)
			cover, err := h3SquareCover(tilequery.Query{Cell: cell.String()})
			assert.NilError(t, err)
			assert.Assert(t, len(cover) <= tilequery.MaxSources)
			if point.Lat == 90 || point.Lat == -90 {
				assert.Equal(t, len(cover), 0)
			}
		})
	}
}

func TestTemporaryReaderCloseIsIdempotent(t *testing.T) {
	provider := newTestTileProvider(t)
	key := cache.Key{CatalogVersion: "catalog", ProjectionID: "projection", Tile: "temporary", SizePolicyID: provider.policyID}
	file, err := provider.cache.CreateStaging(key)
	assert.NilError(t, err)
	reservation, err := provider.scratch.Reserve(t.Context(), 1)
	assert.NilError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	flight := &h3Flight{done: make(chan struct{}), users: 1, cancel: cancel, reservation: reservation}
	flight.tile.Path = file.Name()
	close(flight.done)
	reader := &temporaryReader{File: file, provider: provider, key: key, flight: flight}
	assert.NilError(t, reader.Close())
	assert.NilError(t, reader.Close())
	assert.Assert(t, ctx.Err() != nil)
}
