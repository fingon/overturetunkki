package cache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"
)

type RejectionLimit string

const (
	RejectionLimitRows  RejectionLimit = "rows"
	RejectionLimitBytes RejectionLimit = "compressed_bytes"
)

var (
	ErrNegativeCacheClosed = errors.New("negative cache is closed")
)

type SizeRejection struct {
	key       Key
	limit     RejectionLimit
	actual    int64
	threshold int64
	proof     *struct{}
}

type NegativeCache struct {
	maxEntries int
	ttl        time.Duration

	mu      sync.Mutex
	closed  bool
	entries map[string]*negativeEntry
	lru     *list.List
}

type negativeEntry struct {
	rejection SizeRejection
	expiresAt time.Time
	element   *list.Element
}

func NewSizeRejection(key Key, limit RejectionLimit, actual, threshold int64) (SizeRejection, error) {
	if _, err := key.digest(); err != nil {
		return SizeRejection{}, fmt.Errorf("create size rejection: %w", err)
	}
	if limit != RejectionLimitRows && limit != RejectionLimitBytes {
		return SizeRejection{}, fmt.Errorf("create size rejection: limit %q is unsupported", limit)
	}
	if actual <= 0 || threshold < 0 || actual <= threshold {
		return SizeRejection{}, fmt.Errorf("create size rejection: actual %d must exceed nonnegative threshold %d", actual, threshold)
	}
	return SizeRejection{key: key, limit: limit, actual: actual, threshold: threshold, proof: &struct{}{}}, nil
}

func (rejection SizeRejection) Key() Key {
	return rejection.key
}

func (rejection SizeRejection) Limit() RejectionLimit {
	return rejection.limit
}

func (rejection SizeRejection) Actual() int64 {
	return rejection.actual
}

func (rejection SizeRejection) Threshold() int64 {
	return rejection.threshold
}

func NewNegativeCache(maxEntries int, ttl time.Duration) (*NegativeCache, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("negative cache max entries must be positive, got %d", maxEntries)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("negative cache TTL must be positive, got %s", ttl)
	}
	return &NegativeCache{maxEntries: maxEntries, ttl: ttl, entries: make(map[string]*negativeEntry), lru: list.New()}, nil
}

func (cache *NegativeCache) Put(rejection SizeRejection) error {
	if cache == nil {
		return fmt.Errorf("put negative cache entry: cache is nil")
	}
	if rejection.proof == nil {
		return fmt.Errorf("put negative cache entry: rejection is not proven")
	}
	digest, err := rejection.key.digest()
	if err != nil {
		return fmt.Errorf("put negative cache entry: %w", err)
	}
	now := time.Now()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return ErrNegativeCacheClosed
	}
	if existing := cache.entries[digest]; existing != nil {
		existing.rejection = rejection
		existing.expiresAt = now.Add(cache.ttl)
		cache.lru.MoveToFront(existing.element)
		return nil
	}
	entry := &negativeEntry{rejection: rejection, expiresAt: now.Add(cache.ttl)}
	entry.element = cache.lru.PushFront(entry)
	cache.entries[digest] = entry
	for len(cache.entries) > cache.maxEntries {
		oldest := cache.lru.Back()
		if oldest == nil {
			return fmt.Errorf("negative cache LRU is empty")
		}
		cache.lru.Remove(oldest)
		delete(cache.entries, oldest.Value.(*negativeEntry).rejection.keyDigest())
	}
	return nil
}

func (cache *NegativeCache) Get(key Key) (SizeRejection, bool, error) {
	if cache == nil {
		return SizeRejection{}, false, fmt.Errorf("get negative cache entry: cache is nil")
	}
	digest, err := key.digest()
	if err != nil {
		return SizeRejection{}, false, err
	}
	now := time.Now()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return SizeRejection{}, false, ErrNegativeCacheClosed
	}
	entry := cache.entries[digest]
	if entry == nil {
		return SizeRejection{}, false, nil
	}
	if !now.Before(entry.expiresAt) {
		cache.removeEntryLocked(digest, entry)
		return SizeRejection{}, false, nil
	}
	cache.lru.MoveToFront(entry.element)
	return entry.rejection, true, nil
}

func (cache *NegativeCache) Delete(key Key) error {
	if cache == nil {
		return fmt.Errorf("delete negative cache entry: cache is nil")
	}
	digest, err := key.digest()
	if err != nil {
		return err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return ErrNegativeCacheClosed
	}
	if entry := cache.entries[digest]; entry != nil {
		cache.removeEntryLocked(digest, entry)
	}
	return nil
}

func (cache *NegativeCache) DeleteCatalogVersion(catalogVersion string) error {
	if cache == nil {
		return fmt.Errorf("delete negative catalog entries: cache is nil")
	}
	if catalogVersion == "" {
		return fmt.Errorf("delete negative catalog entries: catalog version is empty")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return ErrNegativeCacheClosed
	}
	for digest, entry := range cache.entries {
		if entry.rejection.key.CatalogVersion == catalogVersion {
			cache.removeEntryLocked(digest, entry)
		}
	}
	return nil
}

func (cache *NegativeCache) Close() error {
	if cache == nil {
		return fmt.Errorf("close negative cache: cache is nil")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return nil
	}
	cache.closed = true
	cache.entries = make(map[string]*negativeEntry)
	cache.lru.Init()
	return nil
}

func (rejection SizeRejection) keyDigest() string {
	digest, _ := rejection.key.digest()
	return digest
}

func (cache *NegativeCache) removeEntryLocked(digest string, entry *negativeEntry) {
	cache.lru.Remove(entry.element)
	delete(cache.entries, digest)
}
