package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/fingon/overturetunkki/internal/worker"
	"gotest.tools/v3/assert"
)

func TestXYZDownloadsAndConditionalRequests(t *testing.T) {
	body := []byte("square parquet data")
	path := filepath.Join(t.TempDir(), "square.parquet")
	assert.NilError(t, os.WriteFile(path, body, 0o600))
	digest := sha256.Sum256(body)
	provider := &testProvider{tile: Tile{Path: path, SizeBytes: int64(len(body)), Digest: "sha256:" + hex.EncodeToString(digest[:])}}
	observer := &testObserver{snapshot: testSnapshot()}
	server, err := New(testOptions(observer, provider))
	assert.NilError(t, err)
	endpoint := "/v1/tiles/places/xyz/10/583/296?catalog_version=" + url.QueryEscape(observer.snapshot.CatalogVersion)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
	assert.Equal(t, response.Code, http.StatusOK)
	assert.DeepEqual(t, response.Body.Bytes(), body)
	assert.DeepEqual(t, provider.lastQuery.Square, &tilequery.Square{Z: 10, X: 583, Y: 296})
	conditional := httptest.NewRequest(http.MethodGet, endpoint, nil)
	conditional.Header.Set("If-None-Match", response.Header().Get("ETag"))
	response = httptest.NewRecorder()
	server.ServeHTTP(response, conditional)
	assert.Equal(t, response.Code, http.StatusNotModified)
	assert.Equal(t, response.Body.Len(), 0)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/catalog", nil))
	var catalog catalogResponse
	assert.NilError(t, json.Unmarshal(response.Body.Bytes(), &catalog))
	assert.Equal(t, len(catalog.SupportedXYZZooms), tilequery.MaxZoom+1)
}

func TestXYZValidationAndRefinementErrors(t *testing.T) {
	for _, test := range []struct {
		path   string
		status int
		zoom   int
		refine bool
	}{
		{"xyz/23/0/0", http.StatusBadRequest, 0, false},
		{"xyz/1/2/0", http.StatusBadRequest, 0, false},
		{"xyz/01/0/0", http.StatusBadRequest, 0, false},
		{"xyz/1/0", http.StatusBadRequest, 0, false},
		{"xyz/0/0/0", http.StatusUnprocessableEntity, 0, true},
		{"xyz/22/0/0", http.StatusUnprocessableEntity, 22, false},
	} {
		t.Run(test.path, func(t *testing.T) {
			observer := &testObserver{snapshot: testSnapshot()}
			provider := &testProvider{err: worker.ErrTooManyRows}
			server, err := New(testOptions(observer, provider))
			assert.NilError(t, err)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/tiles/places/"+test.path+"?catalog_version="+url.QueryEscape(observer.snapshot.CatalogVersion), nil))
			assert.Equal(t, response.Code, test.status)
			var result errorResponse
			assert.NilError(t, json.Unmarshal(response.Body.Bytes(), &result))
			if test.status == http.StatusBadRequest {
				assert.Equal(t, provider.calls, 0)
				return
			}
			assert.Equal(t, *result.Zoom, test.zoom)
			assert.Equal(t, *result.CanRefine, test.refine)
			assert.Assert(t, result.Resolution == nil && result.SuggestedResolution == nil)
			if test.refine {
				assert.Equal(t, *result.SuggestedZoom, test.zoom+1)
			} else {
				assert.Assert(t, result.SuggestedZoom == nil)
			}
		})
	}
}

func TestH3SourceSizeErrorPreservesRequestedCell(t *testing.T) {
	cell := testCell(t)
	square := tilequery.Square{Z: 10, X: 163, Y: 395}
	status, response := classifyQueryError(&worker.SourceSquareError{Square: square, Err: worker.ErrTooManyRows}, tilequery.Query{Cell: cell.String()}, 1024)
	assert.Equal(t, status, http.StatusUnprocessableEntity)
	assert.Equal(t, response.Cell, cell.String())
	assert.Equal(t, *response.SuggestedResolution, cell.Resolution()+1)
	assert.Equal(t, response.FailedLimit, "rows")
	assert.Assert(t, response.Zoom == nil)
	assert.Assert(t, len(response.Message) > len(square.String()))
}
