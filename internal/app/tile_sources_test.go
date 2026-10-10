//nolint:goconst // Independent cases keep cache identities readable.
package app

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/fingon/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

const (
	testSourceCatalogVersion = "catalog"
	testSourceProjectionID   = "projection"
	testSourcePolicyID       = "policy"
	testSourceCell           = "cell"
)

func TestCachedSourcesRequireCompleteCompatibleCoverage(t *testing.T) {
	for _, test := range []struct {
		name     string
		missing  bool
		mismatch string
	}{
		{name: "complete"},
		{name: "missing source", missing: true},
		{name: "wrong catalog", mismatch: testSourceCatalogVersion},
		{name: "wrong projection", mismatch: testSourceProjectionID},
		{name: "wrong policy", mismatch: testSourcePolicyID},
	} {
		t.Run(test.name, func(t *testing.T) {
			tileCache, err := cache.New(cache.Options{Root: t.TempDir(), MaxBytes: 1 << 20, MaxEntries: 100})
			assert.NilError(t, err)
			t.Cleanup(func() { assert.NilError(t, tileCache.Close()) })
			provider := &tileProvider{cache: tileCache}
			cell, err := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, 9)
			assert.NilError(t, err)
			key := cache.Key{CatalogVersion: testSourceCatalogVersion, ProjectionID: testSourceProjectionID, Cell: cell.String(), SizePolicyID: testSourcePolicyID}
			cells, err := h3filter.CoveringCells(cell, cell.Resolution()-1)
			assert.NilError(t, err)
			assert.Assert(t, len(cells) > 1)
			var keys []cache.Key
			for index, sourceCell := range cells {
				if test.missing && index == len(cells)-1 {
					continue
				}
				sourceKey := key
				sourceKey.Cell = sourceCell.String()
				switch test.mismatch {
				case testSourceCatalogVersion:
					sourceKey.CatalogVersion = "different"
				case testSourceProjectionID:
					sourceKey.ProjectionID = "different"
				case testSourcePolicyID:
					sourceKey.SizePolicyID = "different"
				}
				publishSource(t, tileCache, sourceKey)
				keys = append(keys, sourceKey)
			}
			sources, err := provider.cachedSources(key, cell)
			assert.NilError(t, err)
			if test.missing || test.mismatch != "" {
				assert.Equal(t, len(sources.paths), 0)
			} else {
				assert.Equal(t, len(sources.paths), len(cells))
				assert.Equal(t, sources.sizeBytes, int64(len(cells)))
				for _, sourceKey := range keys {
					assert.Assert(t, errors.Is(tileCache.Remove(sourceKey), cache.ErrEntryPinned))
				}
			}
			assert.NilError(t, sources.Close())
			for _, sourceKey := range keys {
				assert.NilError(t, tileCache.Remove(sourceKey))
			}
		})
	}
}

func publishSource(t *testing.T, tileCache *cache.Cache, key cache.Key) {
	t.Helper()
	staging, err := tileCache.CreateStaging(key)
	assert.NilError(t, err)
	assert.NilError(t, staging.Close())
	assert.NilError(t, os.WriteFile(staging.Name(), []byte("x"), 0o600))
	_, err = tileCache.Publish(key, staging.Name(), key.CatalogVersion)
	assert.NilError(t, err)
}

func TestCachedSourcesTryCoarserCoverageAndSurfaceReadErrors(t *testing.T) {
	tileCache, err := cache.New(cache.Options{Root: t.TempDir(), MaxBytes: 1 << 20, MaxEntries: 100})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, tileCache.Close()) })
	provider := &tileProvider{cache: tileCache}
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, 9)
	assert.NilError(t, err)
	key := cache.Key{CatalogVersion: testSourceCatalogVersion, ProjectionID: testSourceProjectionID, Cell: cell.String(), SizePolicyID: testSourcePolicyID}
	cells, err := h3filter.CoveringCells(cell, cell.Resolution()-2)
	assert.NilError(t, err)
	for _, sourceCell := range cells {
		sourceKey := key
		sourceKey.Cell = sourceCell.String()
		publishSource(t, tileCache, sourceKey)
	}
	sources, err := provider.cachedSources(key, cell)
	assert.NilError(t, err)
	assert.Equal(t, len(sources.paths), len(cells))
	assert.NilError(t, sources.Close())
	assert.NilError(t, os.Remove(sources.paths[len(sources.paths)-1]))
	_, err = provider.cachedSources(key, cell)
	assert.Assert(t, err != nil)
	assert.Assert(t, errors.Is(err, os.ErrNotExist))
	// Successfully closing the cache also verifies partial selection released pins.
}

func TestTileBuildRechecksPositiveAndNegativeCache(t *testing.T) {
	tileCache, err := cache.New(cache.Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, tileCache.Close()) })
	negative, err := cache.NewNegativeCache(4, time.Minute)
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, negative.Close()) })
	provider := &tileProvider{cache: tileCache, negative: negative}
	key := cache.Key{CatalogVersion: testSourceCatalogVersion, ProjectionID: testSourceProjectionID, Cell: testSourceCell, SizePolicyID: testSourcePolicyID}
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "negative", true: "positive"}[cached], func(t *testing.T) {
			currentKey := key
			if cached {
				currentKey.Cell = "positive"
				publishSource(t, tileCache, currentKey)
			} else {
				rejection, rejectionErr := cache.NewSizeRejection(currentKey, cache.RejectionLimitRows, 2, 1)
				assert.NilError(t, rejectionErr)
				assert.NilError(t, negative.Put(rejection))
			}
			reservation, reserveErr := tileCache.Reserve(context.Background(), 10)
			assert.NilError(t, reserveErr)
			buildErr := provider.build(t.Context(), reservation, currentKey, tileBuildOptions{})
			if cached {
				assert.NilError(t, buildErr)
			} else {
				assert.Assert(t, errors.Is(buildErr, worker.ErrTooManyRows))
			}
			assert.NilError(t, reservation.Release())
			assert.Equal(t, tileCache.Stats().ReservedBytes, int64(0))
		})
	}
}
