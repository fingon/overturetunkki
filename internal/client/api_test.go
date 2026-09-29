package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestFetchCatalog(t *testing.T) {
	catalogBody := `{"release":"2026-09-23.1","catalog_version":"2026-09-23.1+sha256:test","projection_id":"sha256:projection","fields":["id","geometry"],"max_tile_bytes":1024,"max_tile_rows":10,"supported_h3_resolutions":[0,1],"attribution":["https://overturemaps.org"]}`
	cases := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantError  string
	}{
		{name: "success", status: http.StatusOK, body: catalogBody},
		{name: "server error", status: http.StatusServiceUnavailable, body: `{"code":"catalog_unavailable","message":"try later"}`, wantStatus: http.StatusServiceUnavailable, wantError: "catalog_unavailable"},
		{name: "malformed success", status: http.StatusOK, body: "not JSON", wantStatus: http.StatusOK, wantError: "decode catalog"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
				responseWriter.WriteHeader(test.status)
				_, err := responseWriter.Write([]byte(test.body))
				assert.NilError(t, err)
			}))
			defer server.Close()
			httpClient, err := New(Options{ServerURL: server.URL})
			assert.NilError(t, err)
			if err != nil {
				return
			}
			catalogResponse, meta, err := httpClient.FetchCatalog(context.Background())
			if test.wantError == "" {
				assert.NilError(t, err)
				assert.Equal(t, catalogResponse.Release, "2026-09-23.1")
				assert.Equal(t, meta.StatusCode, http.StatusOK)
				return
			}
			assert.Equal(t, meta.StatusCode, test.wantStatus)
			assert.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestRequestTileUsesExplicitVersionAndETag(t *testing.T) {
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		assert.Equal(t, request.URL.Path, TilePathPrefix+cell.String())
		assert.Equal(t, request.URL.Query().Get("catalog_version"), "release+sha256:test")
		assert.Equal(t, request.Header.Get("If-None-Match"), `"sha256:test"`)
		responseWriter.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	httpClient, err := New(Options{ServerURL: server.URL})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	response, err := httpClient.RequestTile(context.Background(), cell.String(), "release+sha256:test", `"sha256:test"`)
	assert.NilError(t, err)
	if response != nil {
		assert.Equal(t, response.StatusCode, http.StatusNotModified)
		assert.NilError(t, CloseResponse(response))
	}
}

func TestRequestTileRejectsInvalidInputs(t *testing.T) {
	httpClient, err := New(Options{ServerURL: "http://localhost:8080"})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cases := []struct {
		name    string
		cell    string
		version string
		message string
	}{
		{name: "invalid cell", cell: "not-a-cell", version: "version", message: "invalid"},
		{name: "empty version", cell: testCellString(t), message: "catalog version"},
		{name: "noncanonical cell", cell: strings.ToUpper(testCellString(t)), version: "version", message: "canonical"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := httpClient.RequestTile(context.Background(), test.cell, test.version, "")
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestHTTPErrorPreservesStructuredBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
		responseWriter.Header().Set(RequestIDHeader, "request-42")
		responseWriter.Header().Set(RetryAfterHeader, "1")
		responseWriter.WriteHeader(http.StatusConflict)
		assert.NilError(t, json.NewEncoder(responseWriter).Encode(ErrorResponse{Code: "catalog_changed", Message: "refresh", Retryable: true}))
	}))
	defer server.Close()
	httpClient, err := New(Options{ServerURL: server.URL})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	response, err := httpClient.Request(context.Background(), http.MethodGet, "/failure", nil, nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	httpError, ok := ParseHTTPError(response, DefaultErrorBodyBytes).(*HTTPError)
	assert.Assert(t, ok)
	if !ok {
		return
	}
	assert.Equal(t, httpError.Meta.StatusCode, http.StatusConflict)
	assert.Equal(t, httpError.RequestID, "request-42")
	assert.Equal(t, httpError.RetryAfter, "1")
	assert.Equal(t, httpError.API.Code, "catalog_changed")
}

func testCellString(t *testing.T) string {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(t, err)
	return cell.String()
}
