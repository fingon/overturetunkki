package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

const fixtureRelease = "2026-09-23.1"

type catalogFixtureServer struct {
	server *httptest.Server

	mu                    sync.Mutex
	bodies                map[string][]byte
	mutate                func(string, []byte) []byte
	status                map[string]int
	delay                 time.Duration
	conditionalRequestCnt int
	sawIfModifiedSince    bool
	t                     *testing.T
}

func newCatalogFixtureServer(t *testing.T) *catalogFixtureServer {
	t.Helper()
	fixture := &catalogFixtureServer{
		bodies: make(map[string][]byte),
		status: make(map[string]int),
		t:      t,
	}
	fixture.bodies["/catalog.json"] = fixtureFile(t, "catalog.json")
	fixture.bodies[fmt.Sprintf("/%s/catalog.json", fixtureRelease)] = fixtureFile(t, filepath.Join(fixtureRelease, "catalog.json"))
	fixture.bodies[fmt.Sprintf("/%s/places/catalog.json", fixtureRelease)] = fixtureFile(t, filepath.Join(fixtureRelease, "places", "catalog.json"))
	fixture.bodies[fmt.Sprintf("/%s/places/place/collection.json", fixtureRelease)] = fixtureFile(t, filepath.Join(fixtureRelease, "places", "place", "collection.json"))
	for index := 0; index < 16; index++ {
		partition := fmt.Sprintf("%05d", index)
		fixture.bodies[fmt.Sprintf("/%s/places/place/%s/%s.json", fixtureRelease, partition, partition)] = fixtureFile(t, filepath.Join(fixtureRelease, "places", "place", partition+".json"))
	}
	fixture.server = httptest.NewTLSServer(http.HandlerFunc(fixture.serveHTTP))
	fixture.server.Config.ErrorLog = log.New(io.Discard, "", 0)
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *catalogFixtureServer) serveHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	fixture.mu.Lock()
	body, ok := fixture.bodies[request.URL.Path]
	status := fixture.status[request.URL.Path]
	delay := fixture.delay
	mutate := fixture.mutate
	fixture.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-request.Context().Done():
			timer.Stop()
			return
		}
	}
	if !ok {
		http.NotFound(responseWriter, request)
		return
	}
	if status != 0 {
		http.Error(responseWriter, http.StatusText(status), status)
		return
	}
	body = append([]byte(nil), body...)
	body = bytes.ReplaceAll(body, []byte("https://stac.overturemaps.org"), []byte(fixture.server.URL))
	if mutate != nil {
		body = mutate(request.URL.Path, body)
	}
	etagDigest := sha256.Sum256(body)
	etag := fmt.Sprintf(`"fixture-%x"`, etagDigest)
	if request.Header.Get("If-Modified-Since") != "" {
		fixture.mu.Lock()
		fixture.sawIfModifiedSince = true
		fixture.mu.Unlock()
	}
	if request.Header.Get("If-None-Match") == etag {
		fixture.mu.Lock()
		fixture.conditionalRequestCnt++
		fixture.mu.Unlock()
		responseWriter.Header().Set("ETag", etag)
		responseWriter.Header().Set("Last-Modified", "Tue, 29 Sep 2026 00:00:00 GMT")
		responseWriter.WriteHeader(http.StatusNotModified)
		return
	}
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.Header().Set("ETag", etag)
	responseWriter.Header().Set("Last-Modified", "Tue, 29 Sep 2026 00:00:00 GMT")
	responseWriter.WriteHeader(http.StatusOK)
	if _, err := responseWriter.Write(body); err != nil {
		fixture.t.Errorf("write fixture response: %v", err)
	}
}

func (fixture *catalogFixtureServer) options() Options {
	parsed, err := url.Parse(fixture.server.URL)
	if err != nil {
		fixture.t.Fatalf("parse fixture server URL: %v", err)
	}
	return Options{
		CatalogURL:  fixture.server.URL + "/catalog.json",
		CatalogHost: parsed.Host,
		Client:      fixture.server.Client(),
	}
}

func (fixture *catalogFixtureServer) setStatus(path string, status int) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.status[path] = status
}

func (fixture *catalogFixtureServer) setDelay(delay time.Duration) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.delay = delay
}

func (fixture *catalogFixtureServer) conditionalRequests() (int, bool) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return fixture.conditionalRequestCnt, fixture.sawIfModifiedSince
}

func fixtureFile(t *testing.T, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate catalog fixture test")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "testdata", "stac", name)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %q: %v", name, err)
	}
	return body
}

func TestRefreshSTACFixture(t *testing.T) {
	fixture := newCatalogFixtureServer(t)
	manager, err := New(fixture.options())
	assert.NilError(t, err)
	if err != nil {
		return
	}

	observations := make([]Observation, 0, 2)
	first, err := manager.RefreshObserved(context.Background(), func(observation Observation) {
		observations = append(observations, observation)
	})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, first.Release, fixtureRelease)
	assert.Equal(t, first.CollectionID, DefaultCollectionID)
	assert.Equal(t, len(first.Manifest), 16)
	assert.Equal(t, first.Manifest[0].PartitionID, "00000")
	assert.Equal(t, first.Manifest[len(first.Manifest)-1].PartitionID, "00015")
	assert.Equal(t, first.Schema.GeoParquetVersion, "1.1.0")
	assert.Equal(t, first.Schema.PrimaryGeometry, "geometry")
	assert.Equal(t, len(first.Schema.Columns), 16)
	assert.Equal(t, first.Schema.Columns[0].Name, "id")
	assert.Assert(t, strings.HasPrefix(first.CatalogVersion, fixtureRelease+"+sha256:"))
	assert.Assert(t, strings.HasPrefix(first.ProjectionID, "sha256:"))
	assert.Equal(t, len(observations), 2)
	assert.Equal(t, observations[0], Observation{Release: fixtureRelease})
	assert.Equal(t, observations[1], Observation{Release: fixtureRelease, CatalogVersion: first.CatalogVersion})

	second, err := manager.Refresh(context.Background())
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.DeepEqual(t, second, first)
	conditionalRequests, sawIfModifiedSince := fixture.conditionalRequests()
	assert.Assert(t, conditionalRequests >= 20)
	assert.Assert(t, sawIfModifiedSince)
}

func TestRefreshFailsClosedAfterCatalogUnavailable(t *testing.T) {
	fixture := newCatalogFixtureServer(t)
	manager, err := New(fixture.options())
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = manager.Refresh(context.Background())
	assert.NilError(t, err)

	fixture.setStatus("/catalog.json", http.StatusServiceUnavailable)
	snapshot, err := manager.Refresh(context.Background())
	assert.ErrorContains(t, err, "503")
	assert.Equal(t, snapshot.Release, "")
	assert.Equal(t, snapshot.CatalogVersion, "")
}

func TestRefreshChangesCatalogVersionForManifestChange(t *testing.T) {
	fixture := newCatalogFixtureServer(t)
	manager, err := New(fixture.options())
	assert.NilError(t, err)
	if err != nil {
		return
	}
	first, err := manager.Refresh(context.Background())
	assert.NilError(t, err)
	if err != nil {
		return
	}

	fixture.mu.Lock()
	fixture.mutate = func(path string, body []byte) []byte {
		if !strings.HasSuffix(path, "/00000/00000.json") {
			return body
		}
		return bytes.Replace(body, []byte(`"file:size":680811676`), []byte(`"file:size":680811677`), 1)
	}
	fixture.mu.Unlock()
	second, err := manager.Refresh(context.Background())
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, second.CatalogVersion != first.CatalogVersion)
	assert.Equal(t, second.Manifest[0].SizeBytes, int64(680811677))
}

func TestRefreshRejectsInvalidCatalogData(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(string, []byte) []byte
		message string
	}{
		{
			name: "unsupported primary geometry",
			mutate: func(path string, body []byte) []byte {
				if !strings.HasSuffix(path, "/collection.json") {
					return body
				}
				return bytes.Replace(body, []byte(`"table:primary_geometry":"geometry"`), []byte(`"table:primary_geometry":"other"`), 1)
			},
			message: "primary geometry",
		},
		{
			name: "untrusted partition glob",
			mutate: func(path string, body []byte) []byte {
				if !strings.HasSuffix(path, "/collection.json") {
					return body
				}
				return bytes.Replace(body, []byte(DefaultAssetHost), []byte("evil.example"), 1)
			},
			message: "partition glob",
		},
		{
			name: "untrusted asset",
			mutate: func(path string, body []byte) []byte {
				if !strings.HasSuffix(path, "/00000/00000.json") {
					return body
				}
				return bytes.Replace(body, []byte(DefaultAssetHost), []byte("evil.example"), 1)
			},
			message: "asset URL",
		},
		{
			name: "wrong item count",
			mutate: func(path string, body []byte) []byte {
				if !strings.HasSuffix(path, "/collection.json") {
					return body
				}
				return bytes.Replace(body, []byte(`"partition:file_count":16`), []byte(`"partition:file_count":15`), 1)
			},
			message: "item links",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCatalogFixtureServer(t)
			fixture.mu.Lock()
			fixture.mutate = test.mutate
			fixture.mu.Unlock()
			manager, err := New(fixture.options())
			assert.NilError(t, err)
			if err != nil {
				return
			}
			_, err = manager.Refresh(context.Background())
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestRefreshHonorsDeadline(t *testing.T) {
	fixture := newCatalogFixtureServer(t)
	fixture.setDelay(100 * time.Millisecond)
	options := fixture.options()
	options.Timeout = 10 * time.Millisecond
	manager, err := New(options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = manager.Refresh(context.Background())
	assert.ErrorContains(t, err, "deadline")
}

func TestNewRejectsUntrustedCatalogURLs(t *testing.T) {
	fixture := newCatalogFixtureServer(t)
	options := fixture.options()
	parsed, err := url.Parse(fixture.server.URL)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cases := []struct {
		name string
		url  string
	}{
		{name: "non HTTPS", url: "http://" + parsed.Host + "/catalog.json"},
		{name: "query", url: fixture.server.URL + "/catalog.json?mutable=true"},
		{name: "different host", url: "https://example.com/catalog.json"},
		{name: "user info", url: "https://user@" + parsed.Host + "/catalog.json"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options.CatalogURL = test.url
			_, err := New(options)
			assert.Assert(t, err != nil)
		})
	}
}

func TestCanonicalJSON(t *testing.T) {
	value := map[string]any{
		"z": []any{json.Number("1.0"), map[string]any{"b": false, "a": true}},
		"a": "value",
	}
	encoded, err := canonicalJSON(value)
	assert.NilError(t, err)
	assert.Equal(t, string(encoded), `{"a":"value","z":[1.0,{"a":true,"b":false}]}`)

	firstHash, _, err := canonicalSHA256(map[string]any{"one": 1, "two": 2})
	assert.NilError(t, err)
	secondHash, _, err := canonicalSHA256(map[string]any{"two": 2, "one": 1})
	assert.NilError(t, err)
	assert.Equal(t, firstHash, secondHash)
}

func TestCanonicalManifestFixtureHash(t *testing.T) {
	decoder := json.NewDecoder(bytes.NewReader(fixtureFile(t, "manifest.json")))
	decoder.UseNumber()
	var manifest any
	assert.NilError(t, decoder.Decode(&manifest))
	var version struct {
		ManifestSHA256 string `json:"manifest_sha256"`
	}
	assert.NilError(t, json.Unmarshal(fixtureFile(t, "version.json"), &version))
	digest, encoded, err := canonicalSHA256(manifest)
	assert.NilError(t, err)
	assert.Equal(t, digest, "sha256:"+version.ManifestSHA256)
	assert.Equal(t, encoded[len(encoded)-1], byte('\n'))
}

func TestProjectionIDIncludesSelectedSchema(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Nullable: "YES", Type: "VARCHAR"},
		{Name: "geometry", Nullable: "YES", Type: "GEOMETRY('OGC:CRS84')"},
		{Name: "names", Nullable: "YES", Type: "STRUCT(...)"},
	}}
	first, err := projectionID([]string{"id", "geometry", "names"}, schema)
	assert.NilError(t, err)
	second, err := projectionID([]string{"id", "geometry"}, schema)
	assert.NilError(t, err)
	reordered, err := projectionID([]string{"geometry", "id", "names"}, schema)
	assert.NilError(t, err)
	assert.Assert(t, first != second)
	assert.Assert(t, first != reordered)
}

func TestReadResponseLimit(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		contentLength int64
		maxBytes      int64
		wantError     bool
	}{
		{name: "exact", body: "four", contentLength: 4, maxBytes: 4},
		{name: "streamed over limit", body: "four", contentLength: -1, maxBytes: 3, wantError: true},
		{name: "declared over limit", body: "four", contentLength: 4, maxBytes: 3, wantError: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{Body: io.NopCloser(strings.NewReader(test.body)), ContentLength: test.contentLength}
			body, err := readResponse(response, test.maxBytes)
			if test.wantError {
				assert.Assert(t, err != nil)
				return
			}
			assert.NilError(t, err)
			assert.Equal(t, string(body), test.body)
		})
	}
}

func TestValidateBBox(t *testing.T) {
	cases := []struct {
		name    string
		bbox    [4]float64
		message string
	}{
		{name: "antimeridian", bbox: [4]float64{170, -10, -170, 10}},
		{name: "latitude inversion", bbox: [4]float64{-10, 10, 10, -10}, message: "latitude"},
		{name: "longitude out of range", bbox: [4]float64{-181, -10, 10, 10}, message: "longitude"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := validateBBox(test.bbox)
			if test.message == "" {
				assert.NilError(t, err)
				return
			}
			assert.ErrorContains(t, err, test.message)
		})
	}
}
