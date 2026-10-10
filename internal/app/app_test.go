package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestRunModesStopOnCancellation(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cfg.CacheDir = t.TempDir()
	cases := []config.Mode{config.ModeSupervisor, config.ModeWorker}
	for _, mode := range cases {
		t.Run(string(mode), func(t *testing.T) {
			modeConfig := cfg
			modeConfig.Mode = mode
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assert.NilError(t, Run(ctx, modeConfig))
		})
	}
}

func TestRunDeadlineIsReturned(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cfg.CacheDir = t.TempDir()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	assert.ErrorContains(t, Run(ctx, cfg), "deadline exceeded")
}

func TestTileProviderRemembersOutputSizeRejectionWithoutByteCounts(t *testing.T) {
	negative, err := cache.NewNegativeCache(3, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	t.Cleanup(func() { assert.NilError(t, negative.Close()) })

	provider := &tileProvider{negative: negative, maxBytes: 100}
	cases := []struct {
		name string
		err  error
	}{
		{name: "complete typed error", err: &worker.OutputTooLargeError{ActualBytes: 101, LimitBytes: 100}},
		{name: "incomplete typed error", err: &worker.OutputTooLargeError{}},
		{name: "sentinel error", err: worker.ErrOutputTooLarge},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			key := cache.Key{
				CatalogVersion: "release",
				ProjectionID:   testSourceProjectionID,
				Cell:           test.name,
				SizePolicyID:   testSourcePolicyID,
			}
			assert.NilError(t, provider.rememberSizeRejection(key, test.err))
			rejection, ok, getErr := negative.Get(key)
			assert.NilError(t, getErr)
			assert.Assert(t, ok)
			if getErr == nil && ok {
				assert.Equal(t, rejection.Actual(), int64(101))
				assert.Equal(t, rejection.Threshold(), int64(100))
			}
		})
	}
}

func TestEncodeTileWorkerErrorIncludesOutputLimit(t *testing.T) {
	encoded := encodeTileWorkerErrorWithLimit(worker.ErrOutputTooLarge, 100)
	assert.Equal(t, encoded.Kind, workerErrorOutputTooLarge)
	assert.Equal(t, encoded.ActualBytes, int64(101))
	assert.Equal(t, encoded.LimitBytes, int64(100))

	decoded := decodeTileWorkerError(encoded)
	var outputTooLarge *worker.OutputTooLargeError
	assert.Assert(t, errors.As(decoded, &outputTooLarge))
	if outputTooLarge != nil {
		assert.Equal(t, outputTooLarge.ActualBytes, int64(101))
		assert.Equal(t, outputTooLarge.LimitBytes, int64(100))
	}
}

func TestTileProviderRejectsCoarseTilesBeforeCache(t *testing.T) {
	provider := &tileProvider{minResolution: config.DefaultMinTileResolution}
	for resolution := range config.DefaultMinTileResolution {
		cell, err := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, resolution)
		assert.NilError(t, err)
		_, err = provider.Get(t.Context(), catalog.Snapshot{}, cell)
		assert.Assert(t, errors.Is(err, worker.ErrResolutionTooCoarse))
		tooCoarse, ok := errors.AsType[*worker.ResolutionTooCoarseError](err)
		assert.Assert(t, ok)
		assert.Equal(t, tooCoarse.MinResolution, config.DefaultMinTileResolution)
	}
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	original := sizePolicyID(cfg)
	cfg.MinTileResolution++
	assert.Assert(t, sizePolicyID(cfg) != original)
}
