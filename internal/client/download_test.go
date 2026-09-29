package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestDownloadTileResponsePublishesValidatedParquet(t *testing.T) {
	body := []byte("PAR1tilePAR1")
	destination := filepath.Join(t.TempDir(), "tile.parquet")
	response := testDownloadResponse(body)
	response.ContentLength = -1

	result, err := DownloadTileResponse(context.Background(), response, testDownloadOptions(destination))
	assert.NilError(t, err)
	assert.Equal(t, result.StatusCode, http.StatusOK)
	assert.Equal(t, result.Release, "release")
	assert.Equal(t, result.CatalogVersion, "release+sha256:test")
	assert.Equal(t, result.ProjectionID, "sha256:projection")
	assert.Equal(t, result.DownloadedBytes, int64(len(body)))
	assert.Equal(t, result.PublishedPath, destination)
	actual, err := os.ReadFile(destination)
	assert.NilError(t, err)
	assert.DeepEqual(t, actual, body)
	assertNoPartialTile(t, destination)
}

func TestDownloadTileResponse304PreservesDestination(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "tile.parquet")
	original := []byte("existing tile")
	assert.NilError(t, os.WriteFile(destination, original, 0o600))
	response := &http.Response{
		StatusCode: http.StatusNotModified,
		Status:     "304 Not Modified",
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}

	result, err := DownloadTileResponse(context.Background(), response, testDownloadOptions(destination))
	assert.NilError(t, err)
	assert.Assert(t, result.NotModified)
	actual, err := os.ReadFile(destination)
	assert.NilError(t, err)
	assert.DeepEqual(t, actual, original)
	assertNoPartialTile(t, destination)
}

func TestDownloadTileResponseRefusesOverwriteWithoutForce(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "tile.parquet")
	original := []byte("existing tile")
	assert.NilError(t, os.WriteFile(destination, original, 0o600))

	result, err := DownloadTileResponse(context.Background(), testDownloadResponse([]byte("PAR1tilePAR1")), testDownloadOptions(destination))
	assert.Assert(t, result == (DownloadResult{}))
	assert.ErrorContains(t, err, "already exists")
	actual, readErr := os.ReadFile(destination)
	assert.NilError(t, readErr)
	assert.DeepEqual(t, actual, original)
	assertNoPartialTile(t, destination)
}

func TestDownloadTileResponseForceReplacesDestination(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(destination, []byte("existing tile"), 0o600))
	options := testDownloadOptions(destination)
	options.Force = true
	body := []byte("PAR1new tilePAR1")

	_, err := DownloadTileResponse(context.Background(), testDownloadResponse(body), options)
	assert.NilError(t, err)
	actual, err := os.ReadFile(destination)
	assert.NilError(t, err)
	assert.DeepEqual(t, actual, body)
	assertNoPartialTile(t, destination)
}

func TestDownloadTileResponseValidatesHeaders(t *testing.T) {
	cases := []struct {
		name   string
		change func(*http.Response)
		want   string
	}{
		{name: "missing content type", change: func(response *http.Response) { response.Header.Del("Content-Type") }, want: "Content-Type"},
		{name: "wrong content type", change: func(response *http.Response) { response.Header.Set("Content-Type", "application/octet-stream") }, want: "Content-Type"},
		{name: "missing content length", change: func(response *http.Response) { response.Header.Del("Content-Length") }, want: "Content-Length"},
		{name: "invalid content length", change: func(response *http.Response) { response.Header.Set("Content-Length", "not-a-number") }, want: "Content-Length"},
		{name: "missing release", change: func(response *http.Response) { response.Header.Del(ReleaseHeader) }, want: ReleaseHeader},
		{name: "missing catalog version", change: func(response *http.Response) { response.Header.Del(CatalogVersionHeader) }, want: CatalogVersionHeader},
		{name: "wrong catalog version", change: func(response *http.Response) { response.Header.Set(CatalogVersionHeader, "old") }, want: "does not match"},
		{name: "missing projection", change: func(response *http.Response) { response.Header.Del(ProjectionHeader) }, want: ProjectionHeader},
		{name: "wrong projection", change: func(response *http.Response) { response.Header.Set(ProjectionHeader, "sha256:other") }, want: "does not match"},
		{name: "malformed etag", change: func(response *http.Response) { response.Header.Set("ETag", `"sha256:test"`) }, want: "ETag"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "tile.parquet")
			response := testDownloadResponse([]byte("PAR1tilePAR1"))
			test.change(response)
			_, err := DownloadTileResponse(context.Background(), response, testDownloadOptions(destination))
			assert.ErrorContains(t, err, test.want)
			assertNoPartialTile(t, destination)
		})
	}
}

func TestDownloadTileResponseValidatesLengthDigestAndMagic(t *testing.T) {
	cases := []struct {
		name      string
		body      []byte
		change    func(*http.Response)
		maxBytes  int64
		wantError string
	}{
		{name: "truncated body", body: []byte("PAR1tilePAR1"), change: func(response *http.Response) { response.Header.Set("Content-Length", "13") }, wantError: "Content-Length"},
		{name: "over limit header", body: []byte("PAR1tilePAR1"), maxBytes: 4, wantError: "exceeds"},
		{name: "over limit body", body: []byte("PAR1tilePAR1"), change: func(response *http.Response) { response.Header.Set("Content-Length", "4") }, maxBytes: 32, wantError: "Content-Length"},
		{name: "wrong digest", body: []byte("PAR1tilePAR1"), change: func(response *http.Response) { response.Header.Set("ETag", `"sha256:`+strings.Repeat("0", 64)+`"`) }, wantError: "does not match"},
		{name: "invalid parquet magic", body: []byte("not a parquet file"), wantError: "invalid Parquet header"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "tile.parquet")
			response := testDownloadResponse(test.body)
			if test.change != nil {
				test.change(response)
			}
			options := testDownloadOptions(destination)
			if test.maxBytes != 0 {
				options.MaxDownloadBytes = test.maxBytes
			}
			_, err := DownloadTileResponse(context.Background(), response, options)
			assert.ErrorContains(t, err, test.wantError)
			assertNoPartialTile(t, destination)
		})
	}
}

func TestDownloadTileResponseCancellationCleansTemporaryFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(t.TempDir(), "tile.parquet")

	_, err := DownloadTileResponse(ctx, testDownloadResponse([]byte("PAR1tilePAR1")), testDownloadOptions(destination))
	assert.ErrorContains(t, err, "context canceled")
	assertNoPartialTile(t, destination)
	_, statErr := os.Stat(destination)
	assert.Assert(t, os.IsNotExist(statErr))
}

func TestDownloadTileResponseParsesStructuredFailure(t *testing.T) {
	header := make(http.Header)
	header.Set(RequestIDHeader, "request-42")
	response := &http.Response{
		StatusCode: http.StatusUnprocessableEntity,
		Status:     "422 Unprocessable Entity",
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"code":"tile_too_large","message":"refine"}`))),
	}

	_, err := DownloadTileResponse(context.Background(), response, testDownloadOptions(filepath.Join(t.TempDir(), "tile.parquet")))
	var httpErr *HTTPError
	assert.Assert(t, errors.As(err, &httpErr))
	if httpErr != nil {
		assert.Equal(t, httpErr.API.Code, "tile_too_large")
		assert.Equal(t, httpErr.RequestID, "request-42")
	}
}

func TestDownloadTileResponseBoundsErrorBody(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", 32))),
	}
	options := testDownloadOptions(filepath.Join(t.TempDir(), "tile.parquet"))
	options.ErrorBodyBytes = 8

	_, err := DownloadTileResponse(context.Background(), response, options)
	assert.ErrorContains(t, err, "exceeds 8 bytes")
	var httpErr *HTTPError
	assert.Assert(t, errors.As(err, &httpErr))
}

func testDownloadOptions(destination string) DownloadOptions {
	return DownloadOptions{
		Destination:            destination,
		MaxDownloadBytes:       1024,
		ExpectedCatalogVersion: "release+sha256:test",
		ExpectedProjectionID:   "sha256:projection",
	}
}

func testDownloadResponse(body []byte) *http.Response {
	digest := sha256.Sum256(body)
	header := make(http.Header)
	header.Set("Content-Type", ContentTypeParquet)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set(ReleaseHeader, "release")
	header.Set(CatalogVersionHeader, "release+sha256:test")
	header.Set(ProjectionHeader, "sha256:projection")
	header.Set("ETag", `"sha256:`+hex.EncodeToString(digest[:])+`"`)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		ContentLength: int64(len(body)),
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
	}
}

func assertNoPartialTile(t *testing.T, destination string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(destination), "."+filepath.Base(destination)+".partial-*"))
	assert.NilError(t, err)
	assert.Equal(t, len(matches), 0)
}
