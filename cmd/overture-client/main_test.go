package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mstenber/overturetunkki/internal/client"
	"gotest.tools/v3/assert"
)

func TestRunDiscoversCatalogForTile(t *testing.T) {
	cell := "8928308280fffff"
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
	tileBody := readTestFixture(t, "geoparquet/empty.parquet")
	tileDigest := sha256.Sum256(tileBody)
	var catalogCalls, tileCalls int
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/catalog":
			catalogCalls++
			responseWriter.Header().Set("Content-Type", "application/json")
			_, err := responseWriter.Write(readTestFixture(t, "client/catalog.json"))
			assert.NilError(t, err)
		case "/v1/tiles/places/" + cell:
			tileCalls++
			assert.Equal(t, request.URL.Query().Get("catalog_version"), "2026-09-23.1+sha256:test")
			writeTileResponse(t, responseWriter, tileBody, "2026-09-23.1", "2026-09-23.1+sha256:test", "sha256:projection", `"sha256:`+hex.EncodeToString(tileDigest[:])+`"`)
		default:
			http.NotFound(responseWriter, request)
		}
	}))
	defer server.Close()
	output := captureOutput(t, func() error {
		return run(context.Background(), []string{"--server-url=" + server.URL, "tile", cell, "--output=" + outputPath})
	})
	assert.NilError(t, output.err)
	assert.Equal(t, catalogCalls, 1)
	assert.Equal(t, tileCalls, 1)
	assert.Assert(t, bytes.Contains(output.body, []byte(`"status":200`)))
	actual, err := os.ReadFile(outputPath)
	assert.NilError(t, err)
	assert.DeepEqual(t, actual, tileBody)
}

func TestRunCatalogWritesGoldenJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		assert.Equal(t, request.URL.Path, client.CatalogPath)
		responseWriter.Header().Set("Content-Type", "application/json")
		_, err := responseWriter.Write(readTestFixture(t, "client/catalog.json"))
		assert.NilError(t, err)
	}))
	defer server.Close()

	output := captureOutput(t, func() error {
		return run(context.Background(), []string{"--server-url=" + server.URL, "catalog"})
	})
	assert.NilError(t, output.err)
	expected := readTestFixture(t, "client/catalog.golden.json")
	assert.DeepEqual(t, output.body, expected)
}

func TestRunTileExplicitVersionDoesNotDiscover(t *testing.T) {
	cell := "8928308280fffff"
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
	original := []byte("existing tile")
	assert.NilError(t, os.WriteFile(outputPath, original, 0o600))
	var catalogCalls, tileCalls int
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/catalog":
			catalogCalls++
			http.Error(responseWriter, "unexpected discovery", http.StatusInternalServerError)
		case "/v1/tiles/places/" + cell:
			tileCalls++
			assert.Equal(t, request.URL.Query().Get("catalog_version"), "explicit+sha256:test")
			assert.Equal(t, request.Header.Get("If-None-Match"), `"sha256:old"`)
			responseWriter.Header().Set(client.ReleaseHeader, "release")
			responseWriter.Header().Set(client.CatalogVersionHeader, "explicit+sha256:test")
			responseWriter.Header().Set(client.ProjectionHeader, "sha256:projection")
			responseWriter.Header().Set("ETag", `"sha256:`+strings.Repeat("0", 64)+`"`)
			responseWriter.WriteHeader(http.StatusNotModified)
		default:
			http.NotFound(responseWriter, request)
		}
	}))
	defer server.Close()
	output := captureOutput(t, func() error {
		return run(context.Background(), []string{
			"--server-url=" + server.URL,
			"tile", cell,
			"--catalog-version=explicit+sha256:test",
			"--if-none-match=\"sha256:old\"",
			"--output=" + outputPath,
		})
	})
	assert.NilError(t, output.err)
	assert.Equal(t, catalogCalls, 0)
	assert.Equal(t, tileCalls, 1)
	assert.Assert(t, bytes.Contains(output.body, []byte(`"status":304`)))
	assert.Assert(t, bytes.Contains(output.body, []byte(`"projection_id":"sha256:projection"`)))
	actual, err := os.ReadFile(outputPath)
	assert.NilError(t, err)
	assert.DeepEqual(t, actual, original)
}

func writeTileResponse(t *testing.T, responseWriter http.ResponseWriter, body []byte, release, catalogVersion, projectionID, etag string) {
	t.Helper()
	responseWriter.Header().Set("Content-Type", client.ContentTypeParquet)
	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	responseWriter.Header().Set(client.ReleaseHeader, release)
	responseWriter.Header().Set(client.CatalogVersionHeader, catalogVersion)
	responseWriter.Header().Set(client.ProjectionHeader, projectionID)
	responseWriter.Header().Set("ETag", etag)
	responseWriter.WriteHeader(http.StatusOK)
	_, err := responseWriter.Write(body)
	assert.NilError(t, err)
}

func TestRunTileHTTPFailuresExposeExitCodes(t *testing.T) {
	cell := "8928308280fffff"
	cases := []struct {
		name    string
		status  int
		fixture string
		body    string
		code    int
		wants   []string
	}{
		{name: "bad request", status: http.StatusBadRequest, body: "bad request", code: exitInvalidArguments, wants: []string{"400 Bad Request"}},
		{name: "catalog changed", status: http.StatusConflict, fixture: "client/catalog-changed.json", code: exitCatalogChanged, wants: []string{"current catalog version", "--catalog-version"}},
		{name: "tile too large", status: http.StatusUnprocessableEntity, fixture: "client/tile-too-large.json", code: exitTileTooLarge, wants: []string{"finer resolution", "coverage guidance"}},
		{name: "service unavailable", status: http.StatusServiceUnavailable, fixture: "client/service-unavailable.json", code: exitTemporaryFailure, wants: []string{"retry later", "request ID"}},
		{name: "gateway timeout malformed body", status: http.StatusGatewayTimeout, body: "not JSON", code: exitTemporaryFailure, wants: []string{"retry later", "does not prove the tile is oversized"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
				assert.Equal(t, request.URL.Path, client.TilePathPrefix+cell)
				responseWriter.Header().Set(client.RequestIDHeader, "request-42")
				responseWriter.Header().Set(client.RetryAfterHeader, "1")
				responseWriter.WriteHeader(test.status)
				body := []byte(test.body)
				if test.fixture != "" {
					body = readTestFixture(t, test.fixture)
				}
				_, err := responseWriter.Write(body)
				assert.NilError(t, err)
			}))
			outputPath := filepath.Join(t.TempDir(), "tile.parquet")
			err := run(context.Background(), []string{
				"--server-url=" + server.URL,
				"tile", cell,
				"--catalog-version=release+sha256:test",
				"--output=" + outputPath,
			})
			server.Close()
			assert.Assert(t, err != nil)
			assert.Equal(t, exitCode(err), test.code)
			diagnostic := diagnostic(err)
			for _, want := range test.wants {
				assert.Assert(t, strings.Contains(diagnostic, want), diagnostic)
			}
			_, statErr := os.Stat(outputPath)
			assert.Assert(t, os.IsNotExist(statErr))
		})
	}
}

func TestRunTileTimeoutDoesNotPublish(t *testing.T) {
	cell := "8928308280fffff"
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
	err := run(context.Background(), []string{
		"--server-url=" + server.URL,
		"--timeout=20ms",
		"tile", cell,
		"--catalog-version=release+sha256:test",
		"--output=" + outputPath,
	})
	server.Close()
	assert.ErrorContains(t, err, "deadline exceeded")
	assert.Equal(t, exitCode(err), exitFailure)
	_, statErr := os.Stat(outputPath)
	assert.Assert(t, os.IsNotExist(statErr))
}

func TestRunValidatesClientConfiguration(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "invalid URL", args: []string{"--server-url=ftp://example.com", "catalog"}},
		{name: "invalid timeout", args: []string{"--timeout=0s", "catalog"}},
		{name: "command required", args: []string{"--server-url=http://localhost:8080"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			assert.Assert(t, run(context.Background(), test.args) != nil)
		})
	}
}

func TestExitCodeMapsCLIAndHTTPFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", want: exitSuccess},
		{name: "invalid arguments", err: markArgumentError(fmt.Errorf("invalid")), want: exitInvalidArguments},
		{name: "bad request", err: fmt.Errorf("request: %w", &client.HTTPError{Meta: client.ResponseMeta{StatusCode: http.StatusBadRequest}}), want: exitInvalidArguments},
		{name: "catalog changed", err: &client.HTTPError{Meta: client.ResponseMeta{StatusCode: http.StatusConflict}}, want: exitCatalogChanged},
		{name: "tile too large", err: &client.HTTPError{Meta: client.ResponseMeta{StatusCode: http.StatusUnprocessableEntity}}, want: exitTileTooLarge},
		{name: "service unavailable", err: &client.HTTPError{Meta: client.ResponseMeta{StatusCode: http.StatusServiceUnavailable}}, want: exitTemporaryFailure},
		{name: "timeout", err: &client.HTTPError{Meta: client.ResponseMeta{StatusCode: http.StatusGatewayTimeout}}, want: exitTemporaryFailure},
		{name: "unexpected", err: fmt.Errorf("unexpected"), want: exitFailure},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, exitCode(test.err), test.want)
		})
	}
}

func TestRunUsesCommandDeadline(t *testing.T) {
	err := run(context.Background(), []string{"--timeout=1ns", "catalog"})
	assert.ErrorContains(t, err, "deadline exceeded")
}

func TestRunUsesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"--timeout=1h", "catalog"})
	assert.ErrorContains(t, err, "canceled")

	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	err = run(ctx, []string{"--timeout=1h", "catalog"})
	assert.ErrorContains(t, err, "deadline exceeded")
}

type capturedOutput struct {
	body []byte
	err  error
}

func captureOutput(t *testing.T, function func() error) capturedOutput {
	t.Helper()
	read, write, err := os.Pipe()
	assert.NilError(t, err)
	if err != nil {
		return capturedOutput{err: err}
	}
	old := os.Stdout
	os.Stdout = write
	functionErr := function()
	assert.NilError(t, write.Close())
	os.Stdout = old
	body, readErr := io.ReadAll(read)
	assert.NilError(t, read.Close())
	if functionErr != nil {
		return capturedOutput{body: body, err: functionErr}
	}
	return capturedOutput{body: body, err: readErr}
}

func readTestFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	assert.NilError(t, err)
	return body
}
