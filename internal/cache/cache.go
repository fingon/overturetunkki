//nolint:tagliatelle // Persisted cache metadata uses the documented snake_case schema.
package cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	lockFileName     = ".lock"
	filesDirectory   = "files"
	stagingDirectory = "staging"
	fileSuffix       = ".parquet"
	sidecarSuffix    = ".json"
	stagingPrefix    = ".tile-"
	maxSidecarBytes  = 1024 * 1024
)

var (
	ErrCacheInUse          = errors.New("cache directory is already owned")
	ErrCacheClosed         = errors.New("cache is closed")
	ErrCapacityUnavailable = errors.New("cache capacity is unavailable")
	ErrInvalidReservation  = errors.New("cache reservation is invalid")
	ErrQueueFull           = errors.New("cache job queue is full")
	ErrSchedulerClosed     = errors.New("cache scheduler is closed")
	ErrEntryNotFound       = errors.New("cache entry was not found")
	ErrEntryPinned         = errors.New("cache entry is pinned")
)

type Key struct {
	CatalogVersion string `json:"catalog_version"`
	ProjectionID   string `json:"projection_id"`
	Cell           string `json:"cell"`
	SizePolicyID   string `json:"size_policy_id"`
}

type Options struct {
	Root       string
	MaxBytes   int64
	MaxEntries int64
}

type Stats struct {
	UsedBytes       int64
	ReservedBytes   int64
	UsedEntries     int64
	ReservedEntries int64
}

type Entry struct {
	Key         Key
	Path        string
	SidecarPath string
	SizeBytes   int64
	Digest      string
	LastAccess  time.Time
}

type sidecarMetadata struct {
	Key              Key    `json:"key"`
	SizeBytes        int64  `json:"size_bytes"`
	Digest           string `json:"digest"`
	LastAccessUnixNS int64  `json:"last_access_unix_ns"`
}

type cacheEntry struct {
	Entry
	readers int
	dirty   bool
	element *list.Element
}

type Cache struct {
	root       string
	lockFile   *os.File
	maxBytes   int64
	maxEntries int64

	mu              sync.Mutex
	closed          bool
	usedBytes       int64
	reservedBytes   int64
	usedEntries     int64
	reservedEntries int64
	entries         map[string]*cacheEntry
	lru             *list.List
	openReaders     int
	capacityChanged chan struct{}
}

type Reservation struct {
	cache          *Cache
	reservedBytes  int64
	committedBytes int64
	committed      bool
	published      bool
	released       bool
}

func New(options Options) (*Cache, error) {
	if options.Root == "" {
		return nil, errors.New("cache root must not be empty")
	}
	if !filepath.IsAbs(options.Root) {
		return nil, fmt.Errorf("cache root must be absolute, got %q", options.Root)
	}
	if options.MaxBytes <= 0 {
		return nil, fmt.Errorf("cache max bytes must be positive, got %d", options.MaxBytes)
	}
	if options.MaxEntries <= 0 {
		return nil, fmt.Errorf("cache max entries must be positive, got %d", options.MaxEntries)
	}
	if err := os.MkdirAll(options.Root, 0o750); err != nil {
		return nil, fmt.Errorf("create cache root %q: %w", options.Root, err)
	}
	lockPath := filepath.Join(options.Root, lockFileName)
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open cache lock %q: %w", lockPath, err)
	}
	if err := acquireCacheLock(lockFile); err != nil {
		closeErr := lockFile.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("acquire cache lock %q: %w; close lock: %w", lockPath, err, closeErr)
		}
		if errors.Is(err, ErrCacheInUse) {
			return nil, err
		}
		return nil, fmt.Errorf("acquire cache lock %q: %w", lockPath, err)
	}
	cache := &Cache{
		root:            filepath.Clean(options.Root),
		lockFile:        lockFile,
		maxBytes:        options.MaxBytes,
		maxEntries:      options.MaxEntries,
		entries:         make(map[string]*cacheEntry),
		lru:             list.New(),
		capacityChanged: make(chan struct{}),
	}
	if err := cache.initialize(); err != nil {
		closeErr := releaseCacheLock(lockFile)
		fileCloseErr := lockFile.Close()
		if closeErr != nil || fileCloseErr != nil {
			return nil, fmt.Errorf("initialize cache: %w; release lock: %w; close lock: %w", err, closeErr, fileCloseErr)
		}
		return nil, err
	}
	return cache, nil
}

func (c *Cache) Close() error {
	if c == nil {
		return errors.New("close cache: cache is nil")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if err := c.FlushRecency(); err != nil {
		return fmt.Errorf("close cache: %w", err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.reservedBytes != 0 || c.reservedEntries != 0 || c.openReaders != 0 {
		stats := Stats{
			UsedBytes:       c.usedBytes,
			ReservedBytes:   c.reservedBytes,
			UsedEntries:     c.usedEntries,
			ReservedEntries: c.reservedEntries,
		}
		c.mu.Unlock()
		return fmt.Errorf("close cache: active reservations/readers remain: stats=%+v readers=%d", stats, c.openReaders)
	}
	c.closed = true
	close(c.capacityChanged)
	lockFile := c.lockFile
	c.lockFile = nil
	c.mu.Unlock()
	if err := releaseCacheLock(lockFile); err != nil {
		closeErr := lockFile.Close()
		if closeErr != nil {
			return fmt.Errorf("release cache lock: %w; close lock: %w", err, closeErr)
		}
		return fmt.Errorf("release cache lock: %w", err)
	}
	if err := lockFile.Close(); err != nil {
		return fmt.Errorf("close cache lock: %w", err)
	}
	return nil
}

func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		UsedBytes:       c.usedBytes,
		ReservedBytes:   c.reservedBytes,
		UsedEntries:     c.usedEntries,
		ReservedEntries: c.reservedEntries,
	}
}

func (c *Cache) Path(key Key) (string, error) {
	digest, err := key.digest()
	if err != nil {
		return "", err
	}
	if err := c.ensureOpen(); err != nil {
		return "", err
	}
	return filepath.Join(c.root, filesDirectory, digest[:2], digest+fileSuffix), nil
}

func (c *Cache) CreateStaging(key Key) (*os.File, error) {
	digest, err := key.digest()
	if err != nil {
		return nil, err
	}
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	directory := filepath.Join(c.root, stagingDirectory, digest[:2])
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create cache staging directory %q: %w", directory, err)
	}
	file, err := os.CreateTemp(directory, stagingPrefix+digest[:12]+"-*")
	if err != nil {
		return nil, fmt.Errorf("create cache staging file: %w", err)
	}
	return file, nil
}

func (c *Cache) RemoveStaging(path string) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if path == "" {
		return errors.New("remove staging: path is empty")
	}
	stagingRoot := filepath.Join(c.root, stagingDirectory) + string(filepath.Separator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath, stagingRoot) {
		return fmt.Errorf("remove staging: path %q is outside cache staging", path)
	}
	if err := os.Remove(cleanPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove staging %q: %w", cleanPath, err)
	}
	return nil
}

func (c *Cache) Reserve(ctx context.Context, bytes int64) (*Reservation, error) {
	if c == nil {
		return nil, errors.New("reserve cache capacity: cache is nil")
	}
	if ctx == nil {
		return nil, errors.New("reserve cache capacity: context is nil")
	}
	if bytes <= 0 {
		return nil, fmt.Errorf("reserve cache capacity: bytes must be positive, got %d", bytes)
	}
	if bytes > c.maxBytes {
		return nil, fmt.Errorf("%w: request is %d bytes, cache limit is %d bytes", ErrCapacityUnavailable, bytes, c.maxBytes)
	}
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrCacheClosed
		}
		if c.usedBytes+c.reservedBytes+bytes <= c.maxBytes && c.usedEntries+c.reservedEntries < c.maxEntries {
			c.reservedBytes += bytes
			c.reservedEntries++
			c.mu.Unlock()
			return &Reservation{cache: c, reservedBytes: bytes}, nil
		}
		changed := c.capacityChanged
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("reserve cache capacity: %w", ctx.Err())
		case <-changed:
		}
	}
}

func (reservation *Reservation) Commit(actualBytes int64) error {
	if reservation == nil || reservation.cache == nil {
		return fmt.Errorf("commit cache reservation: %w", ErrInvalidReservation)
	}
	if actualBytes < 0 || actualBytes > reservation.reservedBytes {
		return fmt.Errorf("commit cache reservation: actual bytes %d exceed reserved bytes %d: %w", actualBytes, reservation.reservedBytes, ErrInvalidReservation)
	}
	c := reservation.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if reservation.released || reservation.committed || reservation.published || c.closed {
		return fmt.Errorf("commit cache reservation: %w", ErrInvalidReservation)
	}
	c.reservedBytes -= reservation.reservedBytes
	c.reservedEntries--
	c.usedBytes += actualBytes
	c.usedEntries++
	reservation.committedBytes = actualBytes
	reservation.committed = true
	return nil
}

func (reservation *Reservation) Release() error {
	if reservation == nil || reservation.cache == nil {
		return fmt.Errorf("release cache reservation: %w", ErrInvalidReservation)
	}
	c := reservation.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if reservation.released || reservation.published {
		return nil
	}
	if reservation.committed {
		c.usedBytes -= reservation.committedBytes
		c.usedEntries--
	} else {
		c.reservedBytes -= reservation.reservedBytes
		c.reservedEntries--
	}
	reservation.released = true
	close(c.capacityChanged)
	c.capacityChanged = make(chan struct{})
	return nil
}

func (key Key) digest() (string, error) {
	if key.CatalogVersion == "" || key.ProjectionID == "" || key.Cell == "" || key.SizePolicyID == "" {
		return "", errors.New("cache key fields must not be empty")
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("encode cache key: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (c *Cache) ensureOpen() error {
	if c == nil {
		return errors.New("cache is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	return nil
}
