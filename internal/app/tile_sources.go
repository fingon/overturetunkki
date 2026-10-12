package app

import (
	"errors"
	"fmt"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/tilequery"
)

type tileSources struct {
	paths     []string
	readers   []*cache.Reader
	sizeBytes int64
}

func (sources *tileSources) Close() error {
	var closeErrors []error
	for _, reader := range sources.readers {
		if err := reader.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close cached tile source: %w", err))
		}
	}
	sources.readers = nil
	return errors.Join(closeErrors...)
}

func (provider *tileProvider) cachedSources(key cache.Key, square tilequery.Square) (tileSources, error) {
	for square.Z > 0 {
		square = square.Parent()
		sources, err := provider.openSources(key, []tilequery.Square{square})
		if err != nil {
			return tileSources{}, err
		}
		if len(sources.paths) > 0 {
			return sources, nil
		}
	}
	return tileSources{}, nil
}

func (provider *tileProvider) openSources(key cache.Key, squares []tilequery.Square) (sources tileSources, err error) {
	defer func() {
		if err != nil {
			closeErr := sources.Close()
			if errors.Is(err, cache.ErrEntryNotFound) && closeErr == nil {
				err = nil
			} else {
				err = errors.Join(err, closeErr)
			}
			sources = tileSources{}
		}
	}()
	for _, square := range squares {
		sourceKey := key
		sourceKey.Tile = square.String()
		reader, openErr := provider.cache.Open(sourceKey)
		if openErr != nil {
			return sources, openErr
		}
		sources.readers = append(sources.readers, reader)
		entry, entryErr := reader.Entry()
		if entryErr != nil {
			return sources, fmt.Errorf("read cached tile source: %w", entryErr)
		}
		sources.paths = append(sources.paths, entry.Path)
		sources.sizeBytes += entry.SizeBytes
	}
	return sources, nil
}
