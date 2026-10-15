package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/fingon/overturetunkki/internal/httpapi"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
)

type h3Flight struct {
	done        chan struct{}
	cancel      context.CancelFunc
	users       int
	tile        httpapi.Tile
	reservation *cache.ScratchReservation
	err         error
}

type temporaryReader struct {
	*os.File
	provider *tileProvider
	key      cache.Key
	flight   *h3Flight
	once     sync.Once
	err      error
}

func (reader *temporaryReader) Close() error {
	reader.once.Do(func() {
		reader.err = errors.Join(reader.File.Close(), reader.provider.releaseH3(reader.key, reader.flight))
	})
	return reader.err
}

func (provider *tileProvider) getH3(ctx context.Context, generation catalog.Generation, key cache.Key) (httpapi.Tile, error) {
	provider.mu.Lock()
	if provider.closed {
		provider.mu.Unlock()
		return httpapi.Tile{}, cache.ErrSchedulerClosed
	}
	if provider.flights == nil {
		provider.flights = make(map[cache.Key]*h3Flight)
	}
	flight := provider.flights[key]
	if flight == nil {
		buildContext, cancel := context.WithTimeout(context.Background(), provider.settings.TileTimeout)
		flight = &h3Flight{done: make(chan struct{}), cancel: cancel}
		provider.flights[key] = flight
		provider.flightsWait.Add(1)
		go provider.buildH3Flight(buildContext, generation, key, flight)
	}
	flight.users++
	provider.mu.Unlock()
	select {
	case <-ctx.Done():
		return httpapi.Tile{}, errors.Join(ctx.Err(), provider.releaseH3(key, flight))
	case <-flight.done:
	}
	if flight.err != nil {
		return httpapi.Tile{}, errors.Join(flight.err, provider.releaseH3(key, flight))
	}
	file, err := os.Open(flight.tile.Path)
	if err != nil {
		return httpapi.Tile{}, errors.Join(err, provider.releaseH3(key, flight))
	}
	tile := flight.tile
	tile.Reader = &temporaryReader{File: file, provider: provider, key: key, flight: flight}
	return tile, nil
}

func (provider *tileProvider) buildH3Flight(ctx context.Context, generation catalog.Generation, key cache.Key, flight *h3Flight) {
	defer provider.flightsWait.Done()
	defer flight.cancel()
	stop := context.AfterFunc(generation.Context, flight.cancel)
	defer stop()
	tile, reservation, err := provider.composeH3(ctx, generation, key, tilequery.Query{Cell: key.Tile})
	var sourceError *worker.SourceSquareError
	if err != nil && !errors.As(err, &sourceError) {
		err = errors.Join(err, provider.rememberSizeRejection(key, err))
	}
	provider.mu.Lock()
	flight.tile, flight.reservation, flight.err = tile, reservation, err
	close(flight.done)
	if flight.users == 0 {
		if cleanupErr := provider.cleanH3(flight); cleanupErr != nil {
			slog.Error("clean abandoned H3 result", "tile", key.Tile, "error", cleanupErr)
		}
		if err != nil {
			level := slog.LevelError
			if errors.Is(err, context.Canceled) {
				level = slog.LevelDebug
			}
			slog.Log(ctx, level, "abandoned H3 build finished", "tile", key.Tile, "error", err)
		}
	}
	provider.mu.Unlock()
}

func (provider *tileProvider) releaseH3(key cache.Key, flight *h3Flight) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	flight.users--
	if flight.users != 0 {
		return nil
	}
	flight.cancel()
	if provider.flights[key] == flight {
		delete(provider.flights, key)
	}
	select {
	case <-flight.done:
		return provider.cleanH3(flight)
	default:
		return nil
	}
}

func (provider *tileProvider) cleanH3(flight *h3Flight) error {
	var err error
	if flight.tile.Path != "" {
		err = provider.cache.RemoveStaging(flight.tile.Path)
		flight.tile.Path = ""
	}
	if flight.reservation != nil {
		err = errors.Join(err, flight.reservation.Release())
		flight.reservation = nil
	}
	return err
}

func h3SquareCover(query tilequery.Query) ([]tilequery.Square, error) {
	cell, err := worker.ParseCanonicalCell(query.Cell)
	if err != nil {
		return nil, err
	}
	bounds, err := h3filter.CellBounds(cell)
	if err != nil {
		return nil, err
	}
	coverBounds := make([]tilequery.Rect, 0, len(bounds.LongitudeIntervals))
	for _, interval := range bounds.LongitudeIntervals {
		coverBounds = append(coverBounds, tilequery.Rect{WestDeg: interval.MinDeg, EastDeg: interval.MaxDeg, SouthDeg: bounds.LatitudeMinDeg, NorthDeg: bounds.LatitudeMaxDeg})
	}
	return tilequery.Cover(coverBounds)
}

func (provider *tileProvider) composeH3(ctx context.Context, generation catalog.Generation, key cache.Key, query tilequery.Query) (tile httpapi.Tile, reservation *cache.ScratchReservation, err error) {
	cover, err := h3SquareCover(query)
	if err != nil {
		return tile, nil, err
	}
	sources := tileSources{}
	defer func() { err = errors.Join(err, sources.Close()) }()
	for _, square := range cover {
		source, sourceErr := provider.get(ctx, generation, tilequery.Query{Square: &square}, true)
		if sourceErr != nil {
			if errors.Is(sourceErr, worker.ErrTooManyRows) || errors.Is(sourceErr, worker.ErrOutputTooLarge) {
				return tile, nil, &worker.SourceSquareError{Square: square, Err: sourceErr}
			}
			return tile, nil, fmt.Errorf("fetch source square %s: %w", square, sourceErr)
		}
		reader, ok := source.Reader.(*cache.Reader)
		if !ok {
			return tile, nil, errors.Join(errors.New("square source reader is not a cache reader"), source.Reader.Close())
		}
		sources.readers = append(sources.readers, reader)
		sources.paths = append(sources.paths, source.Path)
		sources.sizeBytes += source.SizeBytes
	}
	slog.Debug("compose H3 from squares", "cell", query.Cell, "source_count", len(sources.paths), "source_bytes", sources.sizeBytes)
	staging, err := provider.cache.CreateStaging(key)
	if err != nil {
		return tile, nil, err
	}
	outputPath := staging.Name()
	if closeErr := staging.Close(); closeErr != nil {
		return tile, nil, errors.Join(closeErr, provider.cache.RemoveStaging(outputPath))
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, provider.cache.RemoveStaging(outputPath))
			if reservation != nil {
				err = errors.Join(err, reservation.Release())
				reservation = nil
			}
		}
	}()
	assemblyKey := key
	assemblyKey.Tile += ":" + outputPath
	err = provider.scheduler.DoTransient(ctx, assemblyKey, func(jobContext context.Context) error {
		buildContext, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(jobContext, cancel)
		defer stop()
		defer cancel()
		var reserveErr error
		reservation, reserveErr = provider.scratch.Reserve(buildContext, provider.settings.ScratchMaxBytes+provider.maxBytes)
		if reserveErr != nil {
			return reserveErr
		}
		plan, planErr := worker.BuildQueryPlan(query, provider.fields, generation.Snapshot.Schema)
		if planErr != nil {
			return planErr
		}
		result, buildErr := provider.executeTile(buildContext, plan, outputPath, tileBuildOptions{Snapshot: generation.Snapshot, Query: query, SourcePaths: sources.paths})
		if buildErr != nil {
			return buildErr
		}
		if result.SizeBytes > provider.maxBytes {
			sizeErr := &worker.OutputTooLargeError{ActualBytes: result.SizeBytes, LimitBytes: provider.maxBytes}
			if _, publishErr := provider.cache.Publish(key, outputPath, generation.Snapshot.CatalogVersion); publishErr != nil {
				if errors.Is(publishErr, cache.ErrCapacityUnavailable) {
					slog.Debug("oversized H3 tile not retained", "tile", key.Tile, "size_bytes", result.SizeBytes, "error", publishErr)
					return sizeErr
				}
				return errors.Join(sizeErr, fmt.Errorf("cache oversized H3 tile: %w", publishErr))
			}
			return sizeErr
		}
		if shrinkErr := reservation.Shrink(result.SizeBytes); shrinkErr != nil {
			return shrinkErr
		}
		tile = httpapi.Tile{Path: outputPath, SizeBytes: result.SizeBytes, Digest: result.Digest}
		return nil
	})
	return tile, reservation, err
}
