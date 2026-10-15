package app

import (
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/fingon/overturetunkki/internal/cache"
	"github.com/fingon/overturetunkki/internal/config"
	"gotest.tools/v3/assert"
)

func TestOpenPreparesExtensionsUnderCacheLock(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	cfg.CacheDir = t.TempDir()
	cfg.ExtensionDirectory = ""
	cause := errors.New("download failed")
	prepare := func(_ context.Context, directory string) (string, error) {
		assert.Equal(t, directory, cfg.CacheDir)
		_, lockErr := cache.New(cache.Options{Root: directory, MaxBytes: cfg.CacheMaxBytes, MaxEntries: cfg.CacheMaxEntries})
		assert.Assert(t, errors.Is(lockErr, cache.ErrCacheInUse))
		return "", cause
	}
	_, err = open(t.Context(), cfg, prepare)
	assert.Assert(t, errors.Is(err, cause))
	service, err := open(t.Context(), cfg, func(_ context.Context, directory string) (string, error) {
		return filepath.Join(directory, "extensions"), nil
	})
	assert.NilError(t, err)
	assert.Equal(t, service.config.ExtensionDirectory, filepath.Join(cfg.CacheDir, "extensions"))
	assert.NilError(t, service.Close())
}

func TestOpenExplicitExtensionsBypassPreparation(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	cfg.CacheDir = t.TempDir()
	cfg.ExtensionDirectory = filepath.Join(t.TempDir(), "provided")
	service, err := open(t.Context(), cfg, func(context.Context, string) (string, error) {
		t.Fatal("explicit directory must bypass preparation")
		return "", errors.New("unexpected preparation")
	})
	assert.NilError(t, err)
	assert.Equal(t, service.config.ExtensionDirectory, cfg.ExtensionDirectory)
	assert.NilError(t, service.Close())
}

func TestPrepareExtensionsWithDuckDBInstaller(t *testing.T) {
	source := os.Getenv(duckdbExtensionDirectoryEnv)
	if source == "" {
		t.Skip("make test supplies real extensions")
	}
	database, err := sql.Open("duckdb", "")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, database.Close()) })
	var version, platform string
	assert.NilError(t, database.QueryRowContext(t.Context(), "SELECT version()").Scan(&version))
	assert.NilError(t, database.QueryRowContext(t.Context(), "PRAGMA platform").Scan(&platform))
	repository := t.TempDir()
	for _, extension := range []string{duckdbHTTPFSExtension, duckdbSpatialExtension} {
		name := extension + ".duckdb_extension"
		input, err := os.Open(filepath.Join(source, version, platform, name))
		assert.NilError(t, err)
		target := filepath.Join(repository, version, platform, name+".gz")
		assert.NilError(t, os.MkdirAll(filepath.Dir(target), 0o750))
		output, err := os.Create(target)
		assert.NilError(t, err)
		archive := gzip.NewWriter(output)
		_, err = io.Copy(archive, input)
		assert.NilError(t, err)
		assert.NilError(t, archive.Close())
		assert.NilError(t, output.Close())
		assert.NilError(t, input.Close())
	}
	var requests atomic.Int64
	files := http.FileServer(http.Dir(repository))
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		files.ServeHTTP(writer, request)
	}))
	t.Cleanup(server.Close)
	cacheDir := t.TempDir()
	directory, err := prepareExtensionsFrom(t.Context(), cacheDir, server.URL)
	assert.NilError(t, err)
	assert.Equal(t, directory, filepath.Join(cacheDir, "extensions"))
	assert.Equal(t, requests.Load(), int64(2))
	for _, extension := range []string{duckdbHTTPFSExtension, duckdbSpatialExtension} {
		_, err := os.Stat(filepath.Join(directory, version, platform, extension+".duckdb_extension"))
		assert.NilError(t, err)
	}
	server.Close()
	// INSTALL must reuse the existing signed files without consulting the server.
	again, err := prepareExtensionsFrom(t.Context(), cacheDir, server.URL)
	assert.NilError(t, err)
	assert.Equal(t, again, directory)
	assert.Equal(t, requests.Load(), int64(2))
}

func TestPrepareExtensionsFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		body   []byte
		status int
	}{
		{"http failure", nil, http.StatusNotFound},
		{"invalid archive", []byte("not a gzip archive"), http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
				_, err := writer.Write(test.body)
				assert.NilError(t, err)
			}))
			t.Cleanup(server.Close)
			_, err := prepareExtensionsFrom(t.Context(), t.TempDir(), server.URL)
			assert.ErrorContains(t, err, "install DuckDB extension")
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := prepareExtensions(ctx, t.TempDir())
		assert.Assert(t, errors.Is(err, context.Canceled))
	})
	t.Run("filesystem", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NilError(t, os.WriteFile(filepath.Join(cacheDir, "extensions"), nil, 0o600))
		_, err := prepareExtensions(t.Context(), cacheDir)
		assert.ErrorContains(t, err, "create DuckDB extension directory")
	})
}
