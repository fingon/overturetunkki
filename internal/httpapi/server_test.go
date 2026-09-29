package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mstenber/overturetunkki/internal/cache"
	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

type testObserver struct {
	mu         sync.Mutex
	snapshot   catalog.Snapshot
	refreshErr error
	readyErr   error
	refreshes  int
}

func (observer *testObserver) Refresh(context.Context) (catalog.Snapshot, error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.refreshes++
	if observer.refreshErr != nil {
		return catalog.Snapshot{}, observer.refreshErr
	}
	return observer.snapshot, nil
}

func (observer *testObserver) Current() (catalog.Generation, error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.readyErr != nil {
		return catalog.Generation{}, observer.readyErr
	}
	return catalog.Generation{Number: 3, Snapshot: observer.snapshot}, nil
}

func (observer *testObserver) refreshCount() int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.refreshes
}

type testProvider struct {
	mu    sync.Mutex
	tile  Tile
	err   error
	calls int
}

func (provider *testProvider) Get(context.Context, catalog.Snapshot, h3.Cell) (Tile, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls++
	return provider.tile, provider.err
}

func (provider *testProvider) callCount() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

func TestServerCatalogAndHealthEndpoints(t *testing.T) {
	observer := &testObserver{snapshot: testSnapshot()}
	server, err := New(testOptions(observer, &testProvider{}))
	assert.NilError(t, err)
	if err != nil {
		return
	}

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/catalog", nil))
	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Equal(t, recorder.Header().Get(releaseHeader), observer.snapshot.Release)
	assert.Equal(t, recorder.Header().Get(catalogVersionHeader), observer.snapshot.CatalogVersion)
	assert.Equal(t, recorder.Header().Get("Cache-Control"), cacheControlFresh)
	var response catalogResponse
	assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, response.Release, observer.snapshot.Release)
	assert.Equal(t, response.MaxTileBytes, int64(1024))
	assert.DeepEqual(t, response.Fields, []string{"id", "geometry", "names"})

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, recorder.Code, http.StatusOK)

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, recorder.Code, http.StatusOK)

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Assert(t, strings.Contains(recorder.Body.String(), "overture_http_requests_total"))
}

func TestServerTileVersionValidationAndConditionalResponse(t *testing.T) {
	data := []byte("complete parquet bytes")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	digest := sha256.Sum256(data)
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &testProvider{tile: Tile{Path: path, SizeBytes: int64(len(data)), Digest: "sha256:" + hex.EncodeToString(digest[:])}}
	server, err := New(testOptions(observer, provider))
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cell := testCell(t)
	pathWithVersion := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String(), nil))
	assert.Equal(t, recorder.Code, http.StatusBadRequest)

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/tiles/places/0x"+cell.String()+"?catalog_version="+observer.snapshot.CatalogVersion, nil))
	assert.Equal(t, recorder.Code, http.StatusBadRequest)

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version=old", nil))
	assert.Equal(t, recorder.Code, http.StatusConflict)

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, pathWithVersion, nil))
	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Equal(t, recorder.Header().Get("Content-Type"), contentTypeParquet)
	assert.Equal(t, recorder.Header().Get("Content-Length"), "22")
	assert.Equal(t, recorder.Header().Get("ETag"), `"`+provider.tile.Digest+`"`)
	assert.Equal(t, recorder.Header().Get("Cache-Control"), cacheControlFresh)
	assert.DeepEqual(t, recorder.Body.Bytes(), data)

	conditionalRequest := httptest.NewRequest(http.MethodGet, pathWithVersion, nil)
	conditionalRequest.Header.Set("If-None-Match", recorder.Header().Get("ETag"))
	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, conditionalRequest)
	assert.Equal(t, recorder.Code, http.StatusNotModified)
	assert.Equal(t, recorder.Body.Len(), 0)

	rangeRequest := httptest.NewRequest(http.MethodGet, pathWithVersion, nil)
	rangeRequest.Header.Set("Range", "bytes=0-1")
	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, rangeRequest)
	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.DeepEqual(t, recorder.Body.Bytes(), data)
	assert.Assert(t, observer.refreshCount() >= 7)
	assert.Assert(t, provider.callCount() >= 3)
}

func TestServerMapsTileFailuresAndCatalogReadiness(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "too large", err: worker.ErrOutputTooLarge, status: http.StatusUnprocessableEntity, code: "tile_too_large"},
		{name: "too many rows", err: worker.ErrTooManyRows, status: http.StatusUnprocessableEntity, code: "tile_too_large"},
		{name: "capacity", err: cache.ErrCapacityUnavailable, status: http.StatusServiceUnavailable, code: "capacity_unavailable"},
		{name: "timeout", err: worker.ErrTileTimeout, status: http.StatusGatewayTimeout, code: "tile_timeout"},
		{name: "upstream", err: worker.ErrUpstream, status: http.StatusServiceUnavailable, code: "upstream_unavailable"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			observer := &testObserver{snapshot: testSnapshot()}
			provider := &testProvider{err: test.err}
			server, err := New(testOptions(observer, provider))
			assert.NilError(t, err)
			cell := testCell(t)
			request := httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version="+url.QueryEscape(observer.snapshot.CatalogVersion), nil)
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, request)
			assert.Equal(t, recorder.Code, test.status)
			var response errorResponse
			assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, response.Code, test.code)
			if test.code == "tile_too_large" {
				assert.Equal(t, response.Cell, cell.String())
				assert.Assert(t, response.Resolution != nil)
				assert.Assert(t, response.CanRefine != nil)
			}
		})
	}

	t.Run("resolution 15 is terminal", func(t *testing.T) {
		observer := &testObserver{snapshot: testSnapshot()}
		provider := &testProvider{err: worker.ErrOutputTooLarge}
		server, err := New(testOptions(observer, provider))
		assert.NilError(t, err)
		cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), h3.MaxResolution)
		assert.NilError(t, err)
		request := httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version="+url.QueryEscape(observer.snapshot.CatalogVersion), nil)
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, request)
		var response errorResponse
		assert.Equal(t, recorder.Code, http.StatusUnprocessableEntity)
		assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		assert.Assert(t, response.CanRefine != nil)
		assert.Assert(t, !*response.CanRefine)
		assert.Assert(t, response.SuggestedResolution == nil)
	})

	observer := &testObserver{snapshot: testSnapshot(), readyErr: errors.New("not ready")}
	server, err := New(testOptions(observer, &testProvider{}))
	assert.NilError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, recorder.Code, http.StatusServiceUnavailable)

	observer.refreshErr = errors.New("upstream unavailable")
	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/catalog", nil))
	assert.Equal(t, recorder.Code, http.StatusServiceUnavailable)
}

func TestNewServerValidatesOptions(t *testing.T) {
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &testProvider{}
	cases := []struct {
		name    string
		options Options
		message string
	}{
		{name: "observer", options: Options{Provider: provider, Fields: []string{"id"}, MaxTileBytes: 1, MaxTileRows: 1}, message: "observer"},
		{name: "provider", options: Options{Observer: observer, Fields: []string{"id"}, MaxTileBytes: 1, MaxTileRows: 1}, message: "provider"},
		{name: "fields", options: Options{Observer: observer, Provider: provider, MaxTileBytes: 1, MaxTileRows: 1}, message: "fields"},
		{name: "limits", options: Options{Observer: observer, Provider: provider, Fields: []string{"id"}}, message: "limits"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.options)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func testOptions(observer CatalogObserver, provider TileProvider) Options {
	return Options{
		Observer:     observer,
		Provider:     provider,
		Fields:       []string{"id", "geometry", "names"},
		MaxTileBytes: 1024,
		MaxTileRows:  10,
		AttributionURL: []string{
			"https://overturemaps.org",
		},
	}
}

func testSnapshot() catalog.Snapshot {
	return catalog.Snapshot{
		Release:        "2026-09-23.1",
		CatalogVersion: "2026-09-23.1+sha256:test",
		ProjectionID:   "sha256:projection",
		CollectionID:   catalog.DefaultCollectionID,
	}
}

func testCell(t testing.TB) h3.Cell {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(t, err)
	return cell
}
