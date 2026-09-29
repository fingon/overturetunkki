package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

func (c *Cache) initialize() error {
	filesRoot := filepath.Join(c.root, filesDirectory)
	if err := os.MkdirAll(filesRoot, 0o750); err != nil {
		return fmt.Errorf("create cache files directory: %w", err)
	}
	stagingRoot := filepath.Join(c.root, stagingDirectory)
	if err := os.RemoveAll(stagingRoot); err != nil {
		return fmt.Errorf("remove stale cache staging: %w", err)
	}
	if err := os.MkdirAll(stagingRoot, 0o750); err != nil {
		return fmt.Errorf("create cache staging directory: %w", err)
	}
	if err := c.reconcileFiles(filesRoot); err != nil {
		return err
	}
	entries := make([]*cacheEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].LastAccess.Before(entries[right].LastAccess)
	})
	c.lru.Init()
	for _, entry := range entries {
		entry.element = c.lru.PushBack(entry)
	}
	return c.evictOverLimitLocked("")
}

func (c *Cache) reconcileFiles(filesRoot string) error {
	tilePaths := make(map[string]string)
	sidecarPaths := make(map[string]string)
	err := filepath.WalkDir(filesRoot, func(path string, directory os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk cache files %q: %w", path, walkErr)
		}
		if directory.IsDir() {
			return nil
		}
		baseName := filepath.Base(path)
		switch {
		case strings.HasSuffix(baseName, fileSuffix):
			digest := strings.TrimSuffix(baseName, fileSuffix)
			tilePaths[digest] = path
		case strings.HasSuffix(baseName, sidecarSuffix):
			digest := strings.TrimSuffix(baseName, sidecarSuffix)
			sidecarPaths[digest] = path
		default:
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove unknown cache file %q: %w", path, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for digest, tilePath := range tilePaths {
		sidecarPath, ok := sidecarPaths[digest]
		if !ok {
			if err := removeCachePath(tilePath); err != nil {
				return fmt.Errorf("remove cache tile without sidecar %q: %w", tilePath, err)
			}
			slog.Warn("removed cache tile without sidecar", "path", tilePath)
			continue
		}
		metadata, err := readSidecar(sidecarPath)
		if err != nil {
			if cleanupErr := removeCachePair(tilePath, sidecarPath); cleanupErr != nil {
				return fmt.Errorf("reconcile corrupt cache entry %q: %w; cleanup: %w", tilePath, err, cleanupErr)
			}
			slog.Warn("removed corrupt cache entry", "path", tilePath, "error", err)
			continue
		}
		keyDigest, err := metadata.Key.digest()
		if err != nil || keyDigest != digest || metadata.SizeBytes <= 0 || metadata.Digest == "" {
			if cleanupErr := removeCachePair(tilePath, sidecarPath); cleanupErr != nil {
				return fmt.Errorf("reconcile invalid cache metadata %q: %w; cleanup: %w", tilePath, err, cleanupErr)
			}
			slog.Warn("removed invalid cache metadata", "path", tilePath, "error", err)
			continue
		}
		fileInfo, err := os.Stat(tilePath)
		validationErr := err
		if validationErr == nil && !fileInfo.Mode().IsRegular() {
			validationErr = errors.New("cache tile is not a regular file")
		}
		if validationErr == nil && fileInfo.Size() != metadata.SizeBytes {
			validationErr = fmt.Errorf("sidecar size is %d bytes, file is %d bytes", metadata.SizeBytes, fileInfo.Size())
		}
		if validationErr != nil {
			if cleanupErr := removeCachePair(tilePath, sidecarPath); cleanupErr != nil {
				return fmt.Errorf("reconcile cache file %q: %w; cleanup: %w", tilePath, validationErr, cleanupErr)
			}
			slog.Warn("removed missing or size-mismatched cache entry", "path", tilePath, "error", validationErr)
			continue
		}
		actualDigest, _, err := hashFile(tilePath)
		validationErr = err
		if validationErr == nil && actualDigest != metadata.Digest {
			validationErr = fmt.Errorf("sidecar digest %q does not match file digest %q", metadata.Digest, actualDigest)
		}
		if validationErr != nil {
			if cleanupErr := removeCachePair(tilePath, sidecarPath); cleanupErr != nil {
				return fmt.Errorf("reconcile cache digest %q: %w; cleanup: %w", tilePath, validationErr, cleanupErr)
			}
			slog.Warn("removed cache entry with mismatched digest", "path", tilePath, "error", validationErr)
			continue
		}
		lastAccess := time.Unix(0, metadata.LastAccessUnixNS)
		if metadata.LastAccessUnixNS <= 0 {
			lastAccess = time.Unix(0, 1)
		}
		entry := &cacheEntry{
			Key:         metadata.Key,
			Path:        tilePath,
			SidecarPath: sidecarPath,
			SizeBytes:   metadata.SizeBytes,
			Digest:      metadata.Digest,
			LastAccess:  lastAccess,
		}
		entry.element = c.lru.PushBack(entry)
		c.entries[digest] = entry
		c.usedBytes += entry.SizeBytes
		c.usedEntries++
	}
	for digest, sidecarPath := range sidecarPaths {
		if _, ok := tilePaths[digest]; ok {
			continue
		}
		if err := removeCachePath(sidecarPath); err != nil {
			return fmt.Errorf("remove cache sidecar without tile %q: %w", sidecarPath, err)
		}
		slog.Warn("removed cache sidecar without tile", "path", sidecarPath)
	}
	return nil
}

func (c *Cache) Publish(key Key, stagingPath, currentCatalogVersion string) (Entry, error) {
	return c.publish(key, stagingPath, currentCatalogVersion, nil)
}

func (reservation *Reservation) Publish(key Key, stagingPath, currentCatalogVersion string) (Entry, error) {
	if reservation == nil || reservation.cache == nil {
		return Entry{}, fmt.Errorf("publish cache reservation: %w", ErrInvalidReservation)
	}
	if reservation.released || reservation.committed || reservation.published {
		return Entry{}, fmt.Errorf("publish cache reservation: %w", ErrInvalidReservation)
	}
	return reservation.cache.publish(key, stagingPath, currentCatalogVersion, reservation)
}

func (c *Cache) publish(key Key, stagingPath, currentCatalogVersion string, reservation *Reservation) (Entry, error) {
	digest, err := key.digest()
	if err != nil {
		return Entry{}, err
	}
	if err := c.ensureOpen(); err != nil {
		return Entry{}, err
	}
	if err := c.validateStagingPath(key, stagingPath, digest); err != nil {
		return Entry{}, err
	}
	fileInfo, err := os.Stat(stagingPath)
	if err != nil {
		return Entry{}, fmt.Errorf("stat cache staging %q: %w", stagingPath, err)
	}
	if !fileInfo.Mode().IsRegular() || fileInfo.Size() <= 0 {
		return Entry{}, fmt.Errorf("cache staging %q is not a nonempty regular file", stagingPath)
	}
	actualDigest, actualSize, err := hashFile(stagingPath)
	if err != nil {
		return Entry{}, fmt.Errorf("hash cache staging %q: %w", stagingPath, err)
	}
	if err := syncFile(stagingPath); err != nil {
		return Entry{}, fmt.Errorf("sync cache staging %q: %w", stagingPath, err)
	}
	finalPath := filepath.Join(c.root, filesDirectory, digest[:2], digest+fileSuffix)
	sidecarPath := filepath.Join(c.root, filesDirectory, digest[:2], digest+sidecarSuffix)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		return Entry{}, fmt.Errorf("create cache entry directory: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Entry{}, ErrCacheClosed
	}
	if reservation != nil && (reservation.cache != c || reservation.released || reservation.committed || reservation.published) {
		return Entry{}, fmt.Errorf("publish cache reservation: %w", ErrInvalidReservation)
	}
	if reservation != nil && actualSize > reservation.reservedBytes {
		return Entry{}, fmt.Errorf("publish cache tile is %d bytes, reservation is %d bytes: %w", actualSize, reservation.reservedBytes, ErrCapacityUnavailable)
	}
	if existing := c.entries[digest]; existing != nil {
		if err := removeCachePath(stagingPath); err != nil {
			return Entry{}, fmt.Errorf("remove duplicate cache staging %q: %w", stagingPath, err)
		}
		return existing.Entry, nil
	}
	reservedBytes := int64(0)
	reservedEntries := int64(0)
	if reservation != nil {
		reservedBytes = reservation.reservedBytes
		reservedEntries = 1
	}
	if err := c.makeRoomLocked(actualSize, currentCatalogVersion, reservedBytes, reservedEntries); err != nil {
		return Entry{}, err
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return Entry{}, fmt.Errorf("publish cache tile: %w", err)
	}
	if err := syncDirectory(filepath.Dir(finalPath)); err != nil {
		return Entry{}, fmt.Errorf("sync cache tile directory: %w", err)
	}
	lastAccess := time.Now()
	metadata := sidecarMetadata{Key: key, SizeBytes: actualSize, Digest: actualDigest, LastAccessUnixNS: lastAccess.UnixNano()}
	if err := writeSidecarAtomic(sidecarPath, metadata); err != nil {
		removeErr := removeCachePath(finalPath)
		if removeErr != nil {
			return Entry{}, fmt.Errorf("write cache sidecar: %w; remove tile: %w", err, removeErr)
		}
		return Entry{}, fmt.Errorf("write cache sidecar: %w", err)
	}
	entry := &cacheEntry{Key: key, Path: finalPath, SidecarPath: sidecarPath, SizeBytes: actualSize, Digest: actualDigest, LastAccess: lastAccess}
	entry.element = c.lru.PushFront(entry)
	c.entries[digest] = entry
	c.usedBytes += actualSize
	c.usedEntries++
	if reservation != nil {
		c.reservedBytes -= reservation.reservedBytes
		c.reservedEntries--
		reservation.committedBytes = actualSize
		reservation.published = true
		reservation.released = true
	}
	return entry.Entry, nil
}

func (c *Cache) Open(key Key) (*Reader, error) {
	digest, err := key.digest()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCacheClosed
	}
	entry := c.entries[digest]
	if entry == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("open cache entry: %w", ErrEntryNotFound)
	}
	file, err := os.Open(entry.Path)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("open cache entry %q: %w", entry.Path, err)
	}
	entry.readers++
	c.openReaders++
	entry.LastAccess = time.Now()
	entry.dirty = true
	c.lru.MoveToFront(entry.element)
	c.mu.Unlock()
	return &Reader{file: file, cache: c, entry: entry}, nil
}

func (c *Cache) Touch(key Key) error {
	digest, err := key.digest()
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	entry := c.entries[digest]
	if entry == nil {
		return fmt.Errorf("touch cache entry: %w", ErrEntryNotFound)
	}
	entry.LastAccess = time.Now()
	entry.dirty = true
	c.lru.MoveToFront(entry.element)
	return nil
}

func (c *Cache) FlushRecency() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	for _, entry := range c.entries {
		if !entry.dirty {
			continue
		}
		metadata := sidecarMetadata{Key: entry.Key, SizeBytes: entry.SizeBytes, Digest: entry.Digest, LastAccessUnixNS: entry.LastAccess.UnixNano()}
		if err := writeSidecarAtomic(entry.SidecarPath, metadata); err != nil {
			return fmt.Errorf("flush cache recency %q: %w", entry.Path, err)
		}
		entry.dirty = false
	}
	return nil
}

func (c *Cache) Evict(currentCatalogVersion string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	return c.evictOverLimitLocked(currentCatalogVersion)
}

func (c *Cache) Remove(key Key) error {
	digest, err := key.digest()
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	entry := c.entries[digest]
	if entry == nil {
		return fmt.Errorf("remove cache entry: %w", ErrEntryNotFound)
	}
	if entry.readers != 0 {
		return ErrEntryPinned
	}
	if err := c.removeEntryLocked(entry); err != nil {
		return err
	}
	c.signalCapacityLocked()
	return nil
}

type Reader struct {
	file      *os.File
	cache     *Cache
	entry     *cacheEntry
	closeOnce sync.Once
	closeErr  error
}

func (reader *Reader) Entry() (Entry, error) {
	if reader == nil || reader.cache == nil || reader.entry == nil {
		return Entry{}, errors.New("get cache reader entry: reader is nil")
	}
	reader.cache.mu.Lock()
	defer reader.cache.mu.Unlock()
	if reader.cache.closed {
		return Entry{}, ErrCacheClosed
	}
	return reader.entry.Entry, nil
}

func (reader *Reader) Read(bytes []byte) (int, error) {
	return reader.file.Read(bytes)
}

func (reader *Reader) Seek(offset int64, whence int) (int64, error) {
	return reader.file.Seek(offset, whence)
}

func (reader *Reader) Stat() (os.FileInfo, error) {
	return reader.file.Stat()
}

func (reader *Reader) Close() error {
	if reader == nil {
		return errors.New("close cache reader: reader is nil")
	}
	reader.closeOnce.Do(func() {
		reader.closeErr = reader.file.Close()
		reader.cache.mu.Lock()
		if reader.entry.readers > 0 {
			reader.entry.readers--
		}
		if reader.cache.openReaders > 0 {
			reader.cache.openReaders--
		}
		reader.cache.signalCapacityLocked()
		reader.cache.mu.Unlock()
	})
	if reader.closeErr != nil {
		return fmt.Errorf("close cache reader: %w", reader.closeErr)
	}
	return nil
}

func (c *Cache) evictOverLimitLocked(currentCatalogVersion string) error {
	for c.usedBytes > c.maxBytes || c.usedEntries > c.maxEntries {
		if err := c.evictOneLocked(currentCatalogVersion); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) evictOneLocked(currentCatalogVersion string) error {
	var candidate *cacheEntry
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		entry, ok := element.Value.(*cacheEntry)
		if !ok {
			return fmt.Errorf("cache LRU contains %T, want *cacheEntry", element.Value)
		}
		if entry.readers != 0 {
			continue
		}
		if currentCatalogVersion != "" && entry.Key.CatalogVersion == currentCatalogVersion {
			continue
		}
		candidate = entry
		break
	}
	if candidate == nil {
		for element := c.lru.Back(); element != nil; element = element.Prev() {
			entry, ok := element.Value.(*cacheEntry)
			if !ok {
				return fmt.Errorf("cache LRU contains %T, want *cacheEntry", element.Value)
			}
			if entry.readers == 0 {
				candidate = entry
				break
			}
		}
	}
	if candidate == nil {
		return ErrCapacityUnavailable
	}
	if err := c.removeEntryLocked(candidate); err != nil {
		return err
	}
	return nil
}

func (c *Cache) removeEntryLocked(entry *cacheEntry) error {
	if err := removeCachePair(entry.Path, entry.SidecarPath); err != nil {
		return fmt.Errorf("remove cache entry %q: %w", entry.Path, err)
	}
	digest, err := entry.Key.digest()
	if err != nil {
		return err
	}
	delete(c.entries, digest)
	c.lru.Remove(entry.element)
	c.usedBytes -= entry.SizeBytes
	c.usedEntries--
	return nil
}

func (c *Cache) makeRoomLocked(requiredBytes int64, currentCatalogVersion string, reservedBytes, reservedEntries int64) error {
	for c.usedBytes+c.reservedBytes-reservedBytes+requiredBytes > c.maxBytes || c.usedEntries+c.reservedEntries-reservedEntries+1 > c.maxEntries {
		if err := c.evictOneLocked(currentCatalogVersion); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) signalCapacityLocked() {
	close(c.capacityChanged)
	c.capacityChanged = make(chan struct{})
}

func (c *Cache) validateStagingPath(_ Key, stagingPath, digest string) error {
	if stagingPath == "" {
		return errors.New("cache staging path is empty")
	}
	root := filepath.Join(c.root, stagingDirectory, digest[:2]) + string(filepath.Separator)
	cleanPath := filepath.Clean(stagingPath)
	if !strings.HasPrefix(cleanPath, root) || !strings.HasPrefix(filepath.Base(cleanPath), stagingPrefix+digest[:12]+"-") {
		return fmt.Errorf("cache staging path %q is not owned by key", stagingPath)
	}
	return nil
}

func readSidecar(path string) (sidecarMetadata, error) {
	input, err := os.Open(path)
	if err != nil {
		return sidecarMetadata{}, fmt.Errorf("open sidecar: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(input, maxSidecarBytes+1))
	closeErr := input.Close()
	if readErr != nil {
		return sidecarMetadata{}, fmt.Errorf("read sidecar: %w", readErr)
	}
	if closeErr != nil {
		return sidecarMetadata{}, fmt.Errorf("close sidecar: %w", closeErr)
	}
	if len(body) > maxSidecarBytes {
		return sidecarMetadata{}, fmt.Errorf("sidecar exceeds %d bytes", maxSidecarBytes)
	}
	var metadata sidecarMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return sidecarMetadata{}, fmt.Errorf("decode sidecar: %w", err)
	}
	return metadata, nil
}

func writeSidecarAtomic(path string, metadata sidecarMetadata) error {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode sidecar: %w", err)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".sidecar-*")
	if err != nil {
		return fmt.Errorf("create sidecar temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func(cause error) error {
		closeErr := temporary.Close()
		removeErr := removeCachePath(temporaryPath)
		if closeErr != nil || removeErr != nil {
			return fmt.Errorf("%w; close temporary: %w; remove temporary: %w", cause, closeErr, removeErr)
		}
		return cause
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		return cleanup(fmt.Errorf("write sidecar temporary file: %w", err))
	}
	if err := temporary.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync sidecar temporary file: %w", err))
	}
	if err := temporary.Close(); err != nil {
		if removeErr := removeCachePath(temporaryPath); removeErr != nil {
			return fmt.Errorf("close sidecar temporary file: %w; remove temporary: %w", err, removeErr)
		}
		return fmt.Errorf("close sidecar temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		if removeErr := removeCachePath(temporaryPath); removeErr != nil {
			return fmt.Errorf("publish sidecar: %w; remove temporary: %w", err, removeErr)
		}
		return fmt.Errorf("publish sidecar: %w", err)
	}
	return syncDirectory(directory)
}

func hashFile(path string) (string, int64, error) {
	input, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	hasher := sha256.New()
	bytesRead, copyErr := io.Copy(hasher, input)
	closeErr := input.Close()
	if copyErr != nil {
		return "", 0, copyErr
	}
	if closeErr != nil {
		return "", 0, closeErr
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return "", 0, err
	}
	if fileInfo.Size() != bytesRead {
		return "", 0, errors.New("file changed while hashing")
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), bytesRead, nil
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func removeCachePair(tilePath, sidecarPath string) error {
	if err := removeCachePath(tilePath); err != nil {
		return err
	}
	return removeCachePath(sidecarPath)
}

func removeCachePath(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
