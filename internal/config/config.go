package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/uber/h3-go/v4"
)

type Mode string

const (
	ModeSupervisor Mode = "supervisor"
	ModeWorker     Mode = "worker"

	DefaultListen                     = ":8080"
	DefaultCatalogURL                 = "https://stac.overturemaps.org/catalog.json"
	DefaultCatalogPollInterval        = time.Minute
	DefaultCatalogTimeout             = 10 * time.Second
	DefaultMinTileResolution          = 2
	DefaultMaxTileBytes         int64 = 8 * 1024 * 1024
	DefaultMaxTileRows          int64 = 100_000
	DefaultCacheDir                   = "/var/cache/overture"
	DefaultCacheMaxBytes        int64 = 10 * 1024 * 1024 * 1024
	DefaultCacheMaxEntries      int64 = 100_000
	DefaultScratchMaxBytes      int64 = 2 * 1024 * 1024 * 1024
	DefaultWorkerCount          int64 = 2
	DefaultWorkerMemoryBytes    int64 = 512 * 1024 * 1024
	DefaultWorkerThreads        int64 = 2
	DefaultQueueCapacity        int64 = 32
	DefaultTileTimeout                = 30 * time.Second
	DefaultNegativeCacheEntries int64 = 10_000
	DefaultNegativeCacheTTL           = 5 * time.Minute
	DefaultWriteTimeout               = 30 * time.Second
	geometryField                     = "geometry"
)

type Config struct {
	Mode                 Mode          `default:"supervisor" enum:"supervisor,worker" env:"OVERTURE_MODE" help:"Process mode." name:"mode"`
	Listen               string        `default:":8080" env:"OVERTURE_LISTEN" help:"HTTP listen address." name:"listen"`
	CatalogURL           string        `default:"https://stac.overturemaps.org/catalog.json" env:"OVERTURE_CATALOG_URL" help:"Trusted STAC catalog endpoint." name:"catalog-url"`
	CatalogHost          string        `default:"stac.overturemaps.org" env:"OVERTURE_CATALOG_HOST" help:"Trusted STAC catalog host." name:"catalog-host"`
	AssetHost            string        `default:"overturemaps-us-west-2.s3.us-west-2.amazonaws.com" env:"OVERTURE_ASSET_HOST" help:"Trusted places asset host." name:"asset-host"`
	CatalogPollInterval  time.Duration `default:"1m" env:"OVERTURE_CATALOG_POLL_INTERVAL" help:"Additional idle catalog refresh interval." name:"catalog-poll-interval"`
	CatalogTimeout       time.Duration `default:"10s" env:"OVERTURE_CATALOG_TIMEOUT" help:"Complete catalog freshness-check deadline." name:"catalog-timeout"`
	Fields               []string      `default:"id,geometry,names,basic_category" env:"OVERTURE_FIELDS" help:"Comma-separated top-level output fields." name:"fields" sep:","`
	MinTileResolution    int           `default:"2" env:"OVERTURE_MIN_TILE_RESOLUTION" help:"Minimum H3 tile resolution (0-15); coarser requests are rejected without building." name:"min-tile-resolution"`
	MaxTileBytes         int64         `default:"8388608" env:"OVERTURE_MAX_TILE_BYTES" help:"Maximum complete zstd Parquet tile size in bytes." name:"max-tile-bytes"`
	MaxTileRows          int64         `default:"100000" env:"OVERTURE_MAX_TILE_ROWS" help:"Additional early tile row rejection threshold." name:"max-tile-rows"`
	CacheDir             string        `default:"/var/cache/overture" env:"OVERTURE_CACHE_DIR" help:"Exclusive writable cache root." name:"cache-dir"`
	CacheMaxBytes        int64         `default:"10737418240" env:"OVERTURE_CACHE_MAX_BYTES" help:"Complete files and reservations cache limit in bytes." name:"cache-max-bytes"`
	CacheMaxEntries      int64         `default:"100000" env:"OVERTURE_CACHE_MAX_ENTRIES" help:"Maximum cache file and metadata entries." name:"cache-max-entries"`
	ScratchMaxBytes      int64         `default:"2147483648" env:"OVERTURE_SCRATCH_MAX_BYTES" help:"Total worker scratch allowance in bytes." name:"scratch-max-bytes"`
	WorkerCount          int64         `default:"2" env:"OVERTURE_WORKER_COUNT" help:"Concurrent DuckDB jobs." name:"worker-count"`
	WorkerMemoryBytes    int64         `default:"536870912" env:"OVERTURE_WORKER_MEMORY_BYTES" help:"DuckDB memory limit per worker in bytes." name:"worker-memory-bytes"`
	WorkerThreads        int64         `default:"2" env:"OVERTURE_WORKER_THREADS" help:"DuckDB threads per worker." name:"worker-threads"`
	QueueCapacity        int64         `default:"32" env:"OVERTURE_QUEUE_CAPACITY" help:"Maximum waiting tile builds." name:"queue-capacity"`
	TileTimeout          time.Duration `default:"30s" env:"OVERTURE_TILE_TIMEOUT" help:"Queue and tile query deadline." name:"tile-timeout"`
	NegativeCacheEntries int64         `default:"10000" env:"OVERTURE_NEGATIVE_CACHE_ENTRIES" help:"Maximum retained size rejections." name:"negative-cache-entries"`
	NegativeCacheTTL     time.Duration `default:"5m" env:"OVERTURE_NEGATIVE_CACHE_TTL" help:"Size rejection lifetime." name:"negative-cache-ttl"`
	WriteTimeout         time.Duration `default:"30s" env:"OVERTURE_WRITE_TIMEOUT" help:"Maximum response transmission time." name:"write-timeout"`
	Verbose              bool          `env:"OVERTURE_VERBOSE" help:"Set the default slog level to debug." short:"v"`
}

var supportedFields = map[string]struct{}{
	"id":               {},
	geometryField:      {},
	"confidence":       {},
	"websites":         {},
	"emails":           {},
	"socials":          {},
	"phones":           {},
	"brand":            {},
	"addresses":        {},
	"names":            {},
	"sources":          {},
	"operating_status": {},
	"basic_category":   {},
	"categories":       {},
	"taxonomy":         {},
	"version":          {},
	"bbox":             {},
}

func Parse(args []string) (Config, error) {
	var cfg Config
	parser, err := kong.New(
		&cfg,
		kong.Name("overturetunkki"),
		kong.Description("Serve bounded Overture GeoParquet tiles."),
	)
	if err != nil {
		return Config{}, fmt.Errorf("build command parser: %w", err)
	}
	if _, err := parser.Parse(args); err != nil {
		return Config{}, fmt.Errorf("parse command line: %w", err)
	}
	cfg = normalize(cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Mode != ModeSupervisor && c.Mode != ModeWorker {
		return fmt.Errorf("mode %q is not supported", c.Mode)
	}
	if c.MinTileResolution < 0 || c.MinTileResolution > h3.MaxResolution {
		return fmt.Errorf("min-tile-resolution must be between 0 and %d, got %d", h3.MaxResolution, c.MinTileResolution)
	}
	if err := validateListenAddress(c.Listen); err != nil {
		return err
	}
	if err := validateCatalogURL(c.CatalogURL); err != nil {
		return err
	}
	if err := validateTrustedHost(c.CatalogHost, "catalog-host"); err != nil {
		return err
	}
	if err := validateTrustedHost(c.AssetHost, "asset-host"); err != nil {
		return err
	}
	positiveDurations := []struct {
		name  string
		value time.Duration
	}{
		{name: "catalog-poll-interval", value: c.CatalogPollInterval},
		{name: "catalog-timeout", value: c.CatalogTimeout},
		{name: "tile-timeout", value: c.TileTimeout},
		{name: "negative-cache-ttl", value: c.NegativeCacheTTL},
		{name: "write-timeout", value: c.WriteTimeout},
	}
	for _, duration := range positiveDurations {
		if duration.value <= 0 {
			return fmt.Errorf("%s must be positive, got %s", duration.name, duration.value)
		}
	}
	positiveLimits := []struct {
		name  string
		value int64
	}{
		{name: "max-tile-bytes", value: c.MaxTileBytes},
		{name: "max-tile-rows", value: c.MaxTileRows},
		{name: "cache-max-bytes", value: c.CacheMaxBytes},
		{name: "cache-max-entries", value: c.CacheMaxEntries},
		{name: "scratch-max-bytes", value: c.ScratchMaxBytes},
		{name: "worker-count", value: c.WorkerCount},
		{name: "worker-memory-bytes", value: c.WorkerMemoryBytes},
		{name: "worker-threads", value: c.WorkerThreads},
		{name: "queue-capacity", value: c.QueueCapacity},
		{name: "negative-cache-entries", value: c.NegativeCacheEntries},
	}
	for _, limit := range positiveLimits {
		if limit.value <= 0 {
			return fmt.Errorf("%s must be positive, got %d", limit.name, limit.value)
		}
	}
	if c.CacheDir == "" {
		return errors.New("cache-dir must not be empty")
	}
	if !filepath.IsAbs(c.CacheDir) {
		return fmt.Errorf("cache-dir must be absolute, got %q", c.CacheDir)
	}
	if c.CacheMaxBytes < c.MaxTileBytes {
		return fmt.Errorf("cache-max-bytes %d is smaller than max-tile-bytes %d", c.CacheMaxBytes, c.MaxTileBytes)
	}
	if c.WorkerCount > c.ScratchMaxBytes/c.MaxTileBytes {
		return fmt.Errorf("worker-count %d exceeds scratch capacity for max-tile-bytes %d", c.WorkerCount, c.MaxTileBytes)
	}
	if len(c.Fields) == 0 {
		return errors.New("fields must not be empty")
	}
	seenFields := make(map[string]struct{}, len(c.Fields))
	for _, field := range c.Fields {
		if field == "" {
			return errors.New("fields contains an empty name")
		}
		if _, ok := supportedFields[field]; !ok {
			return fmt.Errorf("field %q is not supported", field)
		}
		if _, ok := seenFields[field]; ok {
			return fmt.Errorf("field %q is repeated", field)
		}
		seenFields[field] = struct{}{}
	}
	for _, requiredField := range []string{"id", geometryField} {
		if _, ok := seenFields[requiredField]; !ok {
			return fmt.Errorf("fields must include %q", requiredField)
		}
	}
	return nil
}

func ValidateCacheDirectory(cacheDir string) error {
	if cacheDir == "" {
		return errors.New("cache-dir must not be empty")
	}
	if !filepath.IsAbs(cacheDir) {
		return fmt.Errorf("cache-dir must be absolute, got %q", cacheDir)
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("create cache directory %q: %w", cacheDir, err)
	}
	probe, err := os.CreateTemp(cacheDir, ".overture-write-test-*")
	if err != nil {
		return fmt.Errorf("write cache directory %q: %w", cacheDir, err)
	}
	probePath := probe.Name()
	if err := probe.Close(); err != nil {
		removeErr := os.Remove(probePath)
		if removeErr != nil {
			return fmt.Errorf("close cache write probe %q: %w; remove probe: %w", probePath, err, removeErr)
		}
		return fmt.Errorf("close cache write probe %q: %w", probePath, err)
	}
	if err := os.Remove(probePath); err != nil {
		return fmt.Errorf("remove cache write probe %q: %w", probePath, err)
	}
	return nil
}

func normalize(c Config) Config {
	fields := make([]string, len(c.Fields))
	for index, field := range c.Fields {
		fields[index] = strings.TrimSpace(field)
	}
	c.Fields = fields
	c.Listen = strings.TrimSpace(c.Listen)
	c.CatalogURL = strings.TrimSpace(c.CatalogURL)
	c.CatalogHost = strings.TrimSpace(c.CatalogHost)
	c.AssetHost = strings.TrimSpace(c.AssetHost)
	c.CacheDir = strings.TrimSpace(c.CacheDir)
	return c
}

func validateListenAddress(listen string) error {
	if listen == "" {
		return errors.New("listen must not be empty")
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("listen address %q must include a valid port: %w", listen, err)
	}
	if port == "" || port == "0" {
		return fmt.Errorf("listen address %q must use a nonzero port", listen)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("listen address %q must use a port from 1 through 65535", listen)
	}
	return nil
}

func validateCatalogURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("catalog-url %q is invalid: %w", rawURL, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("catalog-url %q must be an HTTP(S) URL", rawURL)
	}
	if parsed.User != nil {
		return errors.New("catalog-url must not contain user information")
	}
	return nil
}

func validateTrustedHost(host, name string) error {
	if host == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	parsed, err := url.Parse("https://" + host)
	if err != nil || parsed.Host != host || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s %q is invalid", name, host)
	}
	return nil
}
