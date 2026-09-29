//nolint:goconst // Repeated literals keep independent test cases readable.
package config

import (
	"os"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, cfg.Mode, ModeSupervisor)
	assert.Equal(t, cfg.Listen, DefaultListen)
	assert.Equal(t, cfg.CatalogURL, DefaultCatalogURL)
	assert.Equal(t, cfg.CatalogHost, "stac.overturemaps.org")
	assert.Equal(t, cfg.AssetHost, "overturemaps-us-west-2.s3.us-west-2.amazonaws.com")
	assert.Equal(t, cfg.CatalogPollInterval, DefaultCatalogPollInterval)
	assert.Equal(t, cfg.CatalogTimeout, DefaultCatalogTimeout)
	assert.DeepEqual(t, cfg.Fields, []string{"id", "geometry", "names", "basic_category"})
	assert.Equal(t, cfg.MaxTileBytes, DefaultMaxTileBytes)
	assert.Equal(t, cfg.MaxTileRows, DefaultMaxTileRows)
	assert.Equal(t, cfg.CacheDir, DefaultCacheDir)
	assert.Equal(t, cfg.CacheMaxBytes, DefaultCacheMaxBytes)
	assert.Equal(t, cfg.CacheMaxEntries, DefaultCacheMaxEntries)
	assert.Equal(t, cfg.ScratchMaxBytes, DefaultScratchMaxBytes)
	assert.Equal(t, cfg.WorkerCount, DefaultWorkerCount)
	assert.Equal(t, cfg.WorkerMemoryBytes, DefaultWorkerMemoryBytes)
	assert.Equal(t, cfg.WorkerThreads, DefaultWorkerThreads)
	assert.Equal(t, cfg.QueueCapacity, DefaultQueueCapacity)
	assert.Equal(t, cfg.TileTimeout, DefaultTileTimeout)
	assert.Equal(t, cfg.NegativeCacheEntries, DefaultNegativeCacheEntries)
	assert.Equal(t, cfg.NegativeCacheTTL, DefaultNegativeCacheTTL)
	assert.Equal(t, cfg.WriteTimeout, DefaultWriteTimeout)
	assert.Assert(t, !cfg.Verbose)
}

func TestParseCommandLineOverrides(t *testing.T) {
	cfg, err := Parse([]string{
		"--mode=worker",
		"--listen=127.0.0.1:9090",
		"--catalog-url=http://localhost:8081/catalog.json",
		"--catalog-host=localhost:8081",
		"--asset-host=localhost:8081",
		"--catalog-poll-interval=2m",
		"--catalog-timeout=11s",
		"--fields=id,geometry,names",
		"--max-tile-bytes=1000",
		"--max-tile-rows=20",
		"--cache-dir=/tmp/overture-cache",
		"--cache-max-bytes=2000",
		"--cache-max-entries=3",
		"--scratch-max-bytes=3000",
		"--worker-count=2",
		"--worker-memory-bytes=1000",
		"--worker-threads=3",
		"--queue-capacity=4",
		"--tile-timeout=12s",
		"--negative-cache-entries=5",
		"--negative-cache-ttl=13s",
		"--write-timeout=14s",
		"-v",
	})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, cfg.Mode, ModeWorker)
	assert.Equal(t, cfg.Listen, "127.0.0.1:9090")
	assert.Equal(t, cfg.CatalogURL, "http://localhost:8081/catalog.json")
	assert.Equal(t, cfg.CatalogHost, "localhost:8081")
	assert.Equal(t, cfg.AssetHost, "localhost:8081")
	assert.Equal(t, cfg.CatalogPollInterval, 2*time.Minute)
	assert.Equal(t, cfg.CatalogTimeout, 11*time.Second)
	assert.DeepEqual(t, cfg.Fields, []string{"id", "geometry", "names"})
	assert.Equal(t, cfg.MaxTileBytes, int64(1000))
	assert.Equal(t, cfg.MaxTileRows, int64(20))
	assert.Equal(t, cfg.CacheDir, "/tmp/overture-cache")
	assert.Equal(t, cfg.CacheMaxBytes, int64(2000))
	assert.Equal(t, cfg.CacheMaxEntries, int64(3))
	assert.Equal(t, cfg.ScratchMaxBytes, int64(3000))
	assert.Equal(t, cfg.WorkerCount, int64(2))
	assert.Equal(t, cfg.WorkerMemoryBytes, int64(1000))
	assert.Equal(t, cfg.WorkerThreads, int64(3))
	assert.Equal(t, cfg.QueueCapacity, int64(4))
	assert.Equal(t, cfg.TileTimeout, 12*time.Second)
	assert.Equal(t, cfg.NegativeCacheEntries, int64(5))
	assert.Equal(t, cfg.NegativeCacheTTL, 13*time.Second)
	assert.Equal(t, cfg.WriteTimeout, 14*time.Second)
	assert.Assert(t, cfg.Verbose)
}

func TestParseEnvironmentOverrides(t *testing.T) {
	t.Setenv("OVERTURE_MODE", "worker")
	t.Setenv("OVERTURE_LISTEN", ":9091")
	t.Setenv("OVERTURE_FIELDS", "id,geometry,names")
	t.Setenv("OVERTURE_MAX_TILE_BYTES", "1000")
	t.Setenv("OVERTURE_CACHE_MAX_BYTES", "2000")
	t.Setenv("OVERTURE_SCRATCH_MAX_BYTES", "3000")
	t.Setenv("OVERTURE_WORKER_COUNT", "2")
	t.Setenv("OVERTURE_VERBOSE", "true")

	cfg, err := Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, cfg.Mode, ModeWorker)
	assert.Equal(t, cfg.Listen, ":9091")
	assert.DeepEqual(t, cfg.Fields, []string{"id", "geometry", "names"})
	assert.Equal(t, cfg.MaxTileBytes, int64(1000))
	assert.Equal(t, cfg.CacheMaxBytes, int64(2000))
	assert.Equal(t, cfg.ScratchMaxBytes, int64(3000))
	assert.Assert(t, cfg.Verbose)
}

func TestCommandLineOverridesEnvironment(t *testing.T) {
	t.Setenv("OVERTURE_LISTEN", ":9000")
	t.Setenv("OVERTURE_MAX_TILE_BYTES", "1000")
	t.Setenv("OVERTURE_CACHE_MAX_BYTES", "2000")
	t.Setenv("OVERTURE_SCRATCH_MAX_BYTES", "3000")

	cfg, err := Parse([]string{"--listen=:9001", "--max-tile-bytes=2000", "--worker-count=1"})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, cfg.Listen, ":9001")
	assert.Equal(t, cfg.MaxTileBytes, int64(2000))
}

func TestValidate(t *testing.T) {
	base, err := Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cases := []struct {
		name    string
		mutate  func(*Config)
		message string
	}{
		{name: "invalid mode", mutate: func(cfg *Config) { cfg.Mode = "other" }, message: "mode"},
		{name: "invalid listen port", mutate: func(cfg *Config) { cfg.Listen = ":0" }, message: "nonzero port"},
		{name: "invalid listen range", mutate: func(cfg *Config) { cfg.Listen = ":65536" }, message: "1 through 65535"},
		{name: "invalid catalog scheme", mutate: func(cfg *Config) { cfg.CatalogURL = "ftp://example.com/catalog.json" }, message: "HTTP(S)"},
		{name: "invalid catalog host", mutate: func(cfg *Config) { cfg.CatalogHost = "https://example.com" }, message: "catalog-host"},
		{name: "empty cache directory", mutate: func(cfg *Config) { cfg.CacheDir = "" }, message: "cache-dir"},
		{name: "relative cache directory", mutate: func(cfg *Config) { cfg.CacheDir = "cache" }, message: "absolute"},
		{name: "nonpositive limit", mutate: func(cfg *Config) { cfg.MaxTileBytes = 0 }, message: "max-tile-bytes"},
		{name: "cache smaller than tile", mutate: func(cfg *Config) { cfg.CacheMaxBytes = cfg.MaxTileBytes - 1 }, message: "smaller"},
		{name: "workers exceed scratch", mutate: func(cfg *Config) { cfg.WorkerCount = cfg.ScratchMaxBytes/cfg.MaxTileBytes + 1 }, message: "scratch capacity"},
		{name: "empty fields", mutate: func(cfg *Config) { cfg.Fields = nil }, message: "fields must not be empty"},
		{name: "unsupported field", mutate: func(cfg *Config) { cfg.Fields = []string{"id", "geometry", "unknown"} }, message: "not supported"},
		{name: "duplicate field", mutate: func(cfg *Config) { cfg.Fields = []string{"id", "geometry", "geometry"} }, message: "repeated"},
		{name: "missing id", mutate: func(cfg *Config) { cfg.Fields = []string{"geometry"} }, message: "include \"id\""},
		{name: "missing geometry", mutate: func(cfg *Config) { cfg.Fields = []string{"id"} }, message: "include \"geometry\""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			test.mutate(&cfg)
			err := cfg.Validate()
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestValidateCacheDirectory(t *testing.T) {
	cacheDir := t.TempDir() + "/nested/cache"
	assert.NilError(t, ValidateCacheDirectory(cacheDir))
	filePath := t.TempDir() + "/cache-file"
	file, err := os.Create(filePath)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.NilError(t, file.Close())
	assert.ErrorContains(t, ValidateCacheDirectory(filePath), "create cache directory")
}
