//nolint:goconst // Independent API scenarios and fixture paths stay readable.
package overturetunkki_test

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	overture "github.com/fingon/overturetunkki"
	"gotest.tools/v3/assert"
)

func fixtureOptions(t *testing.T, assetURL string) overture.Options {
	t.Helper()
	options := overture.DefaultOptions(t.TempDir())
	options.Fields = []string{"id", "geometry", "names"}
	options.WorkerCount = 1
	options.ExtensionDirectory = os.Getenv("OVERTURE_DUCKDB_EXTENSION_DIRECTORY")
	options.TileTimeout = 10 * time.Second
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		name := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if strings.Count(name, "/") == 4 && strings.HasSuffix(name, ".json") {
			name = path.Join(path.Dir(path.Dir(name)), path.Base(name))
		}
		body, err := os.ReadFile(filepath.Join("testdata", "stac", filepath.FromSlash(name)))
		if err != nil {
			http.NotFound(writer, request)
			return
		}
		body = bytes.ReplaceAll(body, []byte("https://stac.overturemaps.org"), []byte(server.URL))
		if assetURL != "" {
			body = bytes.ReplaceAll(body, []byte("https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com"), []byte(assetURL))
		}
		if _, err := writer.Write(body); err != nil {
			t.Errorf("serve catalog fixture: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	options.CatalogURL = server.URL + "/catalog.json"
	options.CatalogHost = strings.TrimPrefix(server.URL, "https://")
	options.CatalogClient = server.Client()
	if assetURL != "" {
		options.AssetHost = strings.TrimPrefix(assetURL, "https://")
	}
	return options
}

func TestPublicCatalogAndLifecycle(t *testing.T) {
	options := fixtureOptions(t, "")
	// Embedding must ignore command configuration from the surrounding process.
	t.Setenv("OVERTURE_FIELDS", "invalid")
	service, err := overture.Open(t.Context(), options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	t.Cleanup(func() { assert.NilError(t, service.Close()) })
	options.Fields[0] = "invalid"
	catalog, err := service.Catalog(t.Context())
	assert.NilError(t, err)
	assert.Equal(t, catalog.Release, "2026-09-23.1")
	assert.DeepEqual(t, catalog.Fields, []string{"id", "geometry", "names"})
	catalog.Fields[0] = "mutated"
	again, err := service.Catalog(t.Context())
	assert.NilError(t, err)
	assert.Equal(t, again.Fields[0], "id")
	lockedOptions := options
	lockedOptions.Fields = []string{"id", "geometry", "names"}
	_, err = overture.Open(t.Context(), lockedOptions)
	assert.Assert(t, errors.Is(err, overture.ErrCacheInUse))
	_, err = service.Get(t.Context(), overture.TileRequest{Square: &overture.Square{}, CatalogVersion: "stale"})
	var changed *overture.CatalogChangedError
	assert.Assert(t, errors.As(err, &changed))
	assert.Equal(t, changed.CurrentVersion, catalog.CatalogVersion)
	for _, request := range []overture.TileRequest{{}, {Cell: "invalid"}, {Square: &overture.Square{Z: 23}}, {Cell: "invalid", Square: &overture.Square{}}} {
		tile, err := service.Get(t.Context(), request)
		assert.Assert(t, err != nil)
		assert.Assert(t, tile == nil)
	}
	assert.NilError(t, service.Close())
	_, err = service.Catalog(t.Context())
	assert.Assert(t, errors.Is(err, overture.ErrClosed))
	options.Fields[0] = "id"
	reopened, err := overture.Open(t.Context(), options)
	assert.NilError(t, err)
	assert.NilError(t, reopened.Close())
}

func TestPublicOpenValidation(t *testing.T) {
	options := overture.DefaultOptions(t.TempDir())
	for _, test := range []struct {
		name   string
		mutate func(*overture.Options)
	}{
		{"relative cache", func(options *overture.Options) { options.CacheDir = "relative" }},
		{"relative extensions", func(options *overture.Options) { options.ExtensionDirectory = "relative" }},
		{"row limit", func(options *overture.Options) { options.MaxTileRows = 0 }},
		{"concurrency", func(options *overture.Options) { options.WorkerCount = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := options
			test.mutate(&cfg)
			_, err := overture.Open(t.Context(), cfg)
			assert.Assert(t, err != nil)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := overture.Open(ctx, options)
	assert.Assert(t, errors.Is(err, context.Canceled))
}

func tileFixtureOptions(t *testing.T) overture.Options {
	t.Helper()
	if os.Getenv("OVERTURE_DUCKDB_EXTENSION_DIRECTORY") == "" {
		t.Skip("make test supplies pinned extensions")
	}
	data, err := os.ReadFile(filepath.Join("integration", "testdata", "san-francisco.parquet"))
	assert.NilError(t, err)
	assets := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.ServeContent(writer, request, "places.parquet", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(assets.Close)
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: assets.Certificate().Raw})
	certificatePath := filepath.Join(t.TempDir(), "ca.pem")
	assert.NilError(t, os.WriteFile(certificatePath, certificate, 0o600))

	options := fixtureOptions(t, assets.URL)
	options.AssetCAFile = certificatePath
	return options
}

func TestPublicTilePaths(t *testing.T) {
	options := tileFixtureOptions(t)
	service, err := overture.Open(t.Context(), options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() { assert.NilError(t, service.Close()) }()
	requests := []overture.TileRequest{{Square: &overture.Square{Z: 0}}, {Cell: "891126d3303ffff"}}
	for _, request := range requests {
		t.Run(fmt.Sprint(request), func(t *testing.T) {
			tile, err := service.Get(t.Context(), request)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			info, err := os.Stat(tile.Path)
			assert.NilError(t, err)
			assert.Equal(t, info.Size(), tile.SizeBytes)
			assert.Assert(t, tile.Digest != "")
			path := tile.Path
			assert.NilError(t, tile.Close())
			assert.NilError(t, tile.Close())
			_, err = os.Stat(path)
			if request.Square != nil {
				assert.NilError(t, err)
			} else {
				assert.Assert(t, errors.Is(err, os.ErrNotExist))
			}
		})
	}
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			tile, err := service.Get(t.Context(), requests[0])
			assert.NilError(t, err)
			if err == nil {
				assert.NilError(t, tile.Close())
			}
		})
	}
	wait.Wait()
	outstanding, err := service.Get(t.Context(), requests[1])
	assert.NilError(t, err)
	assert.NilError(t, service.Close())
	if outstanding != nil {
		_, err = os.Stat(outstanding.Path)
		assert.Assert(t, errors.Is(err, os.ErrNotExist))
		assert.NilError(t, outstanding.Close())
	}
}

func TestPublicSizeErrors(t *testing.T) {
	for _, limit := range []string{"rows", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			options := tileFixtureOptions(t)
			if limit == "rows" {
				options.MaxTileRows = 1
			} else {
				options.MaxTileBytes = 1
			}
			service, err := overture.Open(t.Context(), options)
			assert.NilError(t, err)
			defer func() { assert.NilError(t, service.Close()) }()
			for range 2 {
				tile, err := service.Get(t.Context(), overture.TileRequest{Square: &overture.Square{Z: 0}})
				assert.Assert(t, tile == nil)
				if limit == "rows" {
					var tooMany *overture.TooManyRowsError
					assert.Assert(t, errors.As(err, &tooMany), err)
					assert.Equal(t, tooMany.ActualRows, options.MaxTileRows+1)
				} else {
					var tooLarge *overture.OutputTooLargeError
					assert.Assert(t, errors.As(err, &tooLarge), err)
					assert.Assert(t, tooLarge.ActualBytes > options.MaxTileBytes)
				}
			}
		})
	}
}
