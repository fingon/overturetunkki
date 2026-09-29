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
	"testing"
	"time"

	"github.com/mstenber/overturetunkki/internal/client"
	"gotest.tools/v3/assert"
)

func TestRunDiscoversCatalogForTile(t *testing.T) {
	cell := "8928308280fffff"
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
	tileBody := []byte("PAR1tilePAR1")
	tileDigest := sha256.Sum256(tileBody)
	var catalogCalls, tileCalls int
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/catalog":
			catalogCalls++
			responseWriter.Header().Set("Content-Type", "application/json")
			_, err := io.WriteString(responseWriter, `{"release":"release","catalog_version":"release+sha256:test","projection_id":"sha256:projection","fields":["id","geometry"],"max_tile_bytes":1024,"max_tile_rows":10}`)
			assert.NilError(t, err)
		case "/v1/tiles/places/" + cell:
			tileCalls++
			assert.Equal(t, request.URL.Query().Get("catalog_version"), "release+sha256:test")
			writeTileResponse(t, responseWriter, tileBody, "release", "release+sha256:test", "sha256:projection", `"sha256:`+hex.EncodeToString(tileDigest[:])+`"`)
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

func TestRunTileExplicitVersionDoesNotDiscover(t *testing.T) {
	cell := "8928308280fffff"
	outputPath := filepath.Join(t.TempDir(), "tile.parquet")
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
