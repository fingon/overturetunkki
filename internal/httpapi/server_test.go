//nolint:goconst // Repeated literals keep independent test cases readable.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/config"
	"github.com/fingon/overturetunkki/internal/worker"
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

func (observer *testObserver) setSnapshot(snapshot catalog.Snapshot) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.snapshot = snapshot
}

type testProvider struct {
	mu    sync.Mutex
	tile  Tile
	err   error
	calls int
	onGet func()
}

type trackingTileReader struct {
	*os.File
	closes int
}

func (reader *trackingTileReader) Close() error {
	reader.closes++
	return reader.File.Close()
}

type blockingProvider struct {
	started chan struct{}
	release chan struct{}
	tile    Tile
	err     error
	once    sync.Once
}

func (provider *blockingProvider) Get(ctx context.Context, _ catalog.Snapshot, _ h3.Cell) (Tile, error) {
	provider.once.Do(func() { close(provider.started) })
	select {
	case <-provider.release:
		return provider.tile, provider.err
	case <-ctx.Done():
		return Tile{}, ctx.Err()
	}
}

type deadlineResponseWriter struct {
	inner     *httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
}

func (writer *deadlineResponseWriter) Header() http.Header {
	return writer.inner.Header()
}

func (writer *deadlineResponseWriter) Write(data []byte) (int, error) {
	return writer.inner.Write(data)
}

func (writer *deadlineResponseWriter) WriteHeader(status int) {
	writer.inner.WriteHeader(status)
}

func (writer *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.deadlines = append(writer.deadlines, deadline)
	return nil
}

func (writer *deadlineResponseWriter) deadlineValues() []time.Time {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return append([]time.Time(nil), writer.deadlines...)
}

func (provider *testProvider) Get(context.Context, catalog.Snapshot, h3.Cell) (Tile, error) {
	provider.mu.Lock()
	provider.calls++
	tile, err, onGet := provider.tile, provider.err, provider.onGet
	provider.mu.Unlock()
	if onGet != nil {
		onGet()
	}
	return tile, err
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
	assert.Equal(t, response.SupportedH3Resolutions[0], config.DefaultMinTileResolution)
	assert.Equal(t, response.SupportedH3Resolutions[len(response.SupportedH3Resolutions)-1], h3.MaxResolution)
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

func TestServerRejectsNonCanonicalTileDigest(t *testing.T) {
	data := []byte("complete parquet bytes")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	cases := []string{
		"sha256:" + strings.Repeat("A", 64),
		"sha256:" + strings.Repeat("0", 63),
		"sha512:" + strings.Repeat("0", 64),
	}
	for _, digest := range cases {
		t.Run(digest, func(t *testing.T) {
			observer := &testObserver{snapshot: testSnapshot()}
			provider := &testProvider{tile: Tile{Path: path, SizeBytes: int64(len(data)), Digest: digest}}
			server, err := New(testOptions(observer, provider))
			assert.NilError(t, err)
			if err != nil {
				return
			}
			cell := testCell(t)
			requestURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, requestURL, nil))
			assert.Equal(t, recorder.Code, http.StatusInternalServerError)
			var response errorResponse
			assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, response.Code, "internal_error")
		})
	}
}

func TestServerRejectsRolloverOnConditionalTileHit(t *testing.T) {
	data := []byte("complete parquet bytes")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	digest := sha256.Sum256(data)
	oldSnapshot := testSnapshot()
	newSnapshot := testSnapshotRelease("2026-09-30.1")
	observer := &testObserver{snapshot: oldSnapshot}
	file, err := os.Open(path)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	reader := &trackingTileReader{File: file}
	provider := &testProvider{tile: Tile{Path: path, SizeBytes: int64(len(data)), Digest: "sha256:" + hex.EncodeToString(digest[:]), Reader: reader}}
	provider.onGet = func() { observer.setSnapshot(newSnapshot) }
	server, err := New(testOptions(observer, provider))
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cell := testCell(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version="+url.QueryEscape(oldSnapshot.CatalogVersion), nil)
	request.Header.Set("If-None-Match", `"`+provider.tile.Digest+`"`)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	assert.Equal(t, recorder.Code, http.StatusConflict)
	assert.Assert(t, recorder.Header().Get("Content-Type") != contentTypeParquet)
	assert.Assert(t, recorder.Header().Get("Content-Length") == "")
	var response errorResponse
	assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, response.Code, "catalog_changed")
	assert.Equal(t, response.CatalogVersion, newSnapshot.CatalogVersion)
	assert.Equal(t, provider.callCount(), 1)
	assert.Equal(t, reader.closes, 1)
}

func TestServerRejectsRolloverDuringTileBuildBeforeHeaders(t *testing.T) {
	data := []byte("complete parquet bytes")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	digest := sha256.Sum256(data)
	oldSnapshot := testSnapshot()
	newSnapshot := testSnapshotRelease("2026-09-30.1")
	observer := &testObserver{snapshot: oldSnapshot}
	provider := &blockingProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
		tile:    Tile{Path: path, SizeBytes: int64(len(data)), Digest: "sha256:" + hex.EncodeToString(digest[:])},
	}
	server, err := New(testOptions(observer, provider))
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cell := testCell(t)
	requestURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(oldSnapshot.CatalogVersion)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, requestURL, nil))
		done <- recorder
	}()
	<-provider.started
	observer.setSnapshot(newSnapshot)
	close(provider.release)
	recorder := <-done
	assert.Equal(t, recorder.Code, http.StatusConflict)
	assert.Assert(t, recorder.Header().Get("Content-Type") != contentTypeParquet)
	assert.Assert(t, recorder.Header().Get("Content-Length") == "")
	var response errorResponse
	assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, response.Code, "catalog_changed")
	assert.Equal(t, response.CatalogVersion, newSnapshot.CatalogVersion)
}

func TestServerRequestIDsBackpressureAndMetrics(t *testing.T) {
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &testProvider{err: cache.ErrCapacityUnavailable}
	options := testOptions(observer, provider)
	options.TileConcurrency = 1
	server, err := New(options)
	assert.NilError(t, err)
	if err != nil {
		return
	}

	catalogRequest := httptest.NewRequest(http.MethodGet, "/v1/catalog", nil)
	catalogRequest.Header.Set(requestIDHeader, "client-42")
	catalogRecorder := httptest.NewRecorder()
	server.ServeHTTP(catalogRecorder, catalogRequest)
	assert.Equal(t, catalogRecorder.Code, http.StatusOK)
	assert.Equal(t, catalogRecorder.Header().Get(requestIDHeader), "client-42")

	invalidRecorder := httptest.NewRecorder()
	server.ServeHTTP(invalidRecorder, httptest.NewRequest(http.MethodGet, "/v1/tiles/places/not-an-h3-cell", nil))
	assert.Equal(t, invalidRecorder.Code, http.StatusBadRequest)
	assert.Assert(t, validRequestID(invalidRecorder.Header().Get(requestIDHeader)))

	cell := testCell(t)
	tileURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	capacityRecorder := httptest.NewRecorder()
	server.ServeHTTP(capacityRecorder, httptest.NewRequest(http.MethodGet, tileURL, nil))
	assert.Equal(t, capacityRecorder.Code, http.StatusServiceUnavailable)
	assert.Equal(t, capacityRecorder.Header().Get("Retry-After"), retryAfterSeconds)
	var response errorResponse
	assert.NilError(t, json.Unmarshal(capacityRecorder.Body.Bytes(), &response))
	assert.Equal(t, response.Code, "capacity_unavailable")

	metricsRecorder := httptest.NewRecorder()
	server.ServeHTTP(metricsRecorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, metricsRecorder.Code, http.StatusOK)
	metrics := metricsRecorder.Body.String()
	assert.Assert(t, strings.Contains(metrics, "overture_http_catalog_requests_total 1\n"))
	assert.Assert(t, strings.Contains(metrics, "overture_http_tile_requests_total 2\n"))
	assert.Assert(t, strings.Contains(metrics, "overture_http_errors_4xx_total 1\n"))
	assert.Assert(t, strings.Contains(metrics, "overture_http_errors_5xx_total 1\n"))
	assert.Assert(t, strings.Contains(metrics, "overture_http_capacity_rejections_total 1\n"))
	assert.Assert(t, strings.Contains(metrics, "overture_catalog_ready 1\n"))
}

func TestServerShutdownDrainsAndRejectsNewRequests(t *testing.T) {
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &blockingProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     worker.ErrTileTimeout,
	}
	options := testOptions(observer, provider)
	options.TileConcurrency = 1
	server, err := New(options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cell := testCell(t)
	tileURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tileURL, nil))
		firstDone <- recorder
	}()
	<-provider.started

	capacityRecorder := httptest.NewRecorder()
	neighbor, cellErr := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, cell.Resolution())
	assert.NilError(t, cellErr)
	assert.Assert(t, neighbor != cell)
	neighborURL := "/v1/tiles/places/" + neighbor.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	server.ServeHTTP(capacityRecorder, httptest.NewRequest(http.MethodGet, neighborURL, nil))
	assert.Equal(t, capacityRecorder.Code, http.StatusServiceUnavailable)
	var capacityResponse errorResponse
	assert.NilError(t, json.Unmarshal(capacityRecorder.Body.Bytes(), &capacityResponse))
	assert.Equal(t, capacityResponse.Code, "capacity_unavailable")

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(context.Background()) }()
	waitForShutdown(t, server)
	select {
	case err := <-shutdownDone:
		assert.Assert(t, false, "shutdown returned while tile request was active: %v", err)
	default:
	}

	shutdownRecorder := httptest.NewRecorder()
	server.ServeHTTP(shutdownRecorder, httptest.NewRequest(http.MethodGet, tileURL, nil))
	assert.Equal(t, shutdownRecorder.Code, http.StatusServiceUnavailable)
	assert.Equal(t, shutdownRecorder.Header().Get("Retry-After"), retryAfterSeconds)
	var shutdownResponse errorResponse
	assert.NilError(t, json.Unmarshal(shutdownRecorder.Body.Bytes(), &shutdownResponse))
	assert.Equal(t, shutdownResponse.Code, "server_shutting_down")

	close(provider.release)
	assert.NilError(t, <-shutdownDone)
	firstRecorder := <-firstDone
	assert.Equal(t, firstRecorder.Code, http.StatusGatewayTimeout)
}

func TestServerSetsTileWriteDeadline(t *testing.T) {
	data := []byte("complete parquet bytes")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	digest := sha256.Sum256(data)
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &testProvider{tile: Tile{Path: path, SizeBytes: int64(len(data)), Digest: "sha256:" + hex.EncodeToString(digest[:])}}
	options := testOptions(observer, provider)
	options.WriteTimeout = time.Second
	server, err := New(options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cell := testCell(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version="+url.QueryEscape(observer.snapshot.CatalogVersion), nil)
	writer := &deadlineResponseWriter{inner: httptest.NewRecorder()}
	server.ServeHTTP(writer, request)
	assert.Equal(t, writer.inner.Code, http.StatusOK)
	deadlines := writer.deadlineValues()
	assert.Equal(t, len(deadlines), 2)
	assert.Assert(t, !deadlines[0].IsZero())
	assert.Assert(t, deadlines[1].IsZero())
}

func waitForShutdown(t *testing.T, server *Server) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		server.stateMu.Lock()
		shuttingDown := server.shuttingDown
		server.stateMu.Unlock()
		if shuttingDown {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("server did not enter shutdown")
		case <-ticker.C:
		}
	}
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
		{name: "queue full", err: cache.ErrQueueFull, status: http.StatusServiceUnavailable, code: "capacity_unavailable"},
		{name: "out of memory", err: worker.ErrOutOfMemory, status: http.StatusServiceUnavailable, code: "capacity_unavailable"},
		{name: "timeout", err: worker.ErrTileTimeout, status: http.StatusGatewayTimeout, code: "tile_timeout"},
		{name: "caller detached", err: fmt.Errorf("cache job caller detached: %w", context.Canceled), status: http.StatusGatewayTimeout, code: "tile_timeout"},
		{name: "canceled", err: worker.ErrTileCanceled, status: http.StatusGatewayTimeout, code: "tile_timeout"},
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
		{name: "tile concurrency", options: Options{Observer: observer, Provider: provider, Fields: []string{"id"}, MaxTileBytes: 1, MaxTileRows: 1, TileConcurrency: -1}, message: "tile concurrency"},
		{name: "write timeout", options: Options{Observer: observer, Provider: provider, Fields: []string{"id"}, MaxTileBytes: 1, MaxTileRows: 1, WriteTimeout: -time.Second}, message: "write timeout"},
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
		Observer:          observer,
		Provider:          provider,
		Fields:            []string{"id", "geometry", "names"},
		MaxTileBytes:      1024,
		MaxTileRows:       10,
		MinTileResolution: config.DefaultMinTileResolution,
		AttributionURL: []string{
			"https://overturemaps.org",
		},
	}
}

func testSnapshot() catalog.Snapshot {
	return testSnapshotRelease("2026-09-23.1")
}

func testSnapshotRelease(release string) catalog.Snapshot {
	return catalog.Snapshot{
		Release:        release,
		CatalogVersion: release + "+sha256:test",
		ProjectionID:   "sha256:projection",
		CollectionID:   catalog.DefaultCollectionID,
	}
}

func testCell(tb testing.TB) h3.Cell {
	tb.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(tb, err)
	return cell
}

func TestServerDuplicateTilesShareAdmission(t *testing.T) {
	data := []byte("complete cached tile")
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, data, 0o600))
	digest := sha256.Sum256(data)
	observer := &testObserver{snapshot: testSnapshot()}
	provider := &blockingProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
		tile:    Tile{Path: path, SizeBytes: int64(len(data)), Digest: "sha256:" + hex.EncodeToString(digest[:])},
	}
	options := testOptions(observer, provider)
	options.TileConcurrency = 1
	server, err := New(options)
	assert.NilError(t, err)
	cell := testCell(t)
	tileURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	done := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tileURL, nil))
			done <- recorder
		}()
	}
	<-provider.started
	deadline := time.Now().Add(time.Second)
	key := tileAdmissionKey{catalogVersion: observer.snapshot.CatalogVersion, projectionID: observer.snapshot.ProjectionID, cell: cell}
	for {
		server.stateMu.Lock()
		count := server.tileAdmissions[key]
		server.stateMu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("duplicate request was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	neighbor, err := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, cell.Resolution())
	assert.NilError(t, err)
	assert.Assert(t, neighbor != cell)
	recorder := httptest.NewRecorder()
	neighborURL := "/v1/tiles/places/" + neighbor.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, neighborURL, nil))
	assert.Equal(t, recorder.Code, http.StatusServiceUnavailable)
	provider.release <- struct{}{}
	first := <-done
	assert.Equal(t, first.Code, http.StatusOK)
	assert.Equal(t, first.Body.String(), string(data))
	server.stateMu.Lock()
	assert.Equal(t, server.tileAdmissions[key], 1)
	server.stateMu.Unlock()
	close(provider.release)
	second := <-done
	assert.Equal(t, second.Code, http.StatusOK)
	assert.Equal(t, second.Body.String(), string(data))
	server.stateMu.Lock()
	assert.Equal(t, len(server.tileAdmissions), 0)
	server.stateMu.Unlock()
}

func TestServerRejectsCoarseTilesBeforeCatalogAndProvider(t *testing.T) {
	for _, minimum := range []int{0, 2, 4, h3.MaxResolution} {
		t.Run(strconv.Itoa(minimum), func(t *testing.T) {
			observer := &testObserver{snapshot: testSnapshot(), refreshErr: errors.New("catalog unavailable")}
			provider := &testProvider{err: worker.ErrTooManyRows}
			options := testOptions(observer, provider)
			options.MinTileResolution = minimum
			server, err := New(options)
			assert.NilError(t, err)
			for resolution := range minimum {
				cell, cellErr := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, resolution)
				assert.NilError(t, cellErr)
				request := httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+cell.String()+"?catalog_version=version", nil)
				recorder := httptest.NewRecorder()
				server.ServeHTTP(recorder, request)
				assert.Equal(t, recorder.Code, http.StatusUnprocessableEntity)
				var response errorResponse
				assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
				assert.Equal(t, response.Code, "tile_too_large")
				assert.Equal(t, response.FailedLimit, "resolution")
				assert.Equal(t, *response.Resolution, resolution)
				assert.Equal(t, *response.SuggestedResolution, minimum)
				assert.Assert(t, *response.CanRefine)
			}
			assert.Equal(t, observer.refreshes, 0)
			assert.Equal(t, provider.calls, 0)
			observer.refreshErr = nil
			cell, err := h3.LatLngToCell(h3.LatLng{Lat: 60.17, Lng: 24.94}, minimum)
			assert.NilError(t, err)
			recorder := httptest.NewRecorder()
			tileURL := "/v1/tiles/places/" + cell.String() + "?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
			server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tileURL, nil))
			assert.Equal(t, recorder.Code, http.StatusUnprocessableEntity)
			var response errorResponse
			assert.NilError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, response.FailedLimit, "rows")
			assert.Equal(t, provider.calls, 1)
			assert.Equal(t, observer.refreshes, 1)
			assert.Equal(t, supportedH3Resolutions(minimum)[0], minimum)
		})
	}
	for _, minimum := range []int{-1, h3.MaxResolution + 1} {
		options := testOptions(&testObserver{}, &testProvider{})
		options.MinTileResolution = minimum
		_, err := New(options)
		assert.ErrorContains(t, err, "minimum tile resolution")
	}
}
