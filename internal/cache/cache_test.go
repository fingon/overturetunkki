package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestCacheOwnershipHashingAndStaging(t *testing.T) {
	root := t.TempDir()
	options := Options{Root: root, MaxBytes: 1024, MaxEntries: 4}
	cache, err := New(options)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() { assert.NilError(t, cache.Close()) }()

	second, err := New(options)
	assert.Assert(t, errors.Is(err, ErrCacheInUse))
	assert.Assert(t, second == nil)

	key := testKey()
	path, err := cache.Path(key)
	assert.NilError(t, err)
	assert.Assert(t, strings.HasSuffix(path, fileSuffix))
	assert.Assert(t, !strings.Contains(path, key.Cell))
	relative, err := filepath.Rel(root, path)
	assert.NilError(t, err)
	assert.Assert(t, !strings.HasPrefix(relative, ".."))

	staging, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	stagingPath := staging.Name()
	assert.NilError(t, staging.Close())
	assert.NilError(t, cache.RemoveStaging(stagingPath))
	_, err = os.Stat(stagingPath)
	assert.Assert(t, errors.Is(err, os.ErrNotExist))
	stale, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	stalePath := stale.Name()
	assert.NilError(t, stale.Close())

	assert.NilError(t, cache.Close())
	reopened, err := New(options)
	assert.NilError(t, err)
	if err == nil {
		_, statErr := os.Stat(stalePath)
		assert.Assert(t, errors.Is(statErr, os.ErrNotExist))
		assert.NilError(t, reopened.Close())
	}
}

func TestCacheReservationsBoundBytesAndEntries(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 10, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	first, err := cache.Reserve(context.Background(), 7)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	second, err := cache.Reserve(context.Background(), 3)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.DeepEqual(t, cache.Stats(), Stats{ReservedBytes: 10, ReservedEntries: 2})

	fullContext, fullCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer fullCancel()
	_, err = cache.Reserve(fullContext, 1)
	assert.Assert(t, errors.Is(err, context.DeadlineExceeded))
	assert.NilError(t, second.Commit(2))
	assert.DeepEqual(t, cache.Stats(), Stats{UsedBytes: 2, UsedEntries: 1, ReservedBytes: 7, ReservedEntries: 1})
	assert.NilError(t, first.Release())
	assert.DeepEqual(t, cache.Stats(), Stats{UsedBytes: 2, UsedEntries: 1})
	assert.NilError(t, second.Release())
	assert.DeepEqual(t, cache.Stats(), Stats{})

	tooLarge, err := cache.Reserve(context.Background(), 11)
	assert.Assert(t, tooLarge == nil)
	assert.Assert(t, errors.Is(err, ErrCapacityUnavailable))

	committed, err := cache.Reserve(context.Background(), 4)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, committed.Commit(5) != nil)
	assert.NilError(t, committed.Commit(4))
	assert.NilError(t, committed.Release())
	assert.NilError(t, committed.Release())
	assert.NilError(t, cache.Close())
}

func TestReservationPublishKeepsCommittedAccounting(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 20, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	reservation, err := cache.Reserve(context.Background(), 10)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	staging, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, reservation.Release())
		return
	}
	stagingPath := staging.Name()
	_, err = staging.Write(bytes.Repeat([]byte("z"), 5))
	assert.NilError(t, err)
	assert.NilError(t, staging.Close())
	_, err = reservation.Publish(key, stagingPath, key.CatalogVersion)
	assert.NilError(t, err)
	assert.DeepEqual(t, cache.Stats(), Stats{UsedBytes: 5, UsedEntries: 1})
	assert.NilError(t, reservation.Release())
	assert.DeepEqual(t, cache.Stats(), Stats{UsedBytes: 5, UsedEntries: 1})
	assert.NilError(t, cache.Remove(key))
	assert.NilError(t, cache.Close())
}

func TestCacheReservationWaitsForReleaseOrCancellation(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 10, MaxEntries: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	reservation, err := cache.Reserve(context.Background(), 10)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = cache.Reserve(ctx, 1)
	assert.Assert(t, errors.Is(err, context.DeadlineExceeded))
	assert.NilError(t, reservation.Release())
	assert.NilError(t, cache.Close())
}

func TestCachePublishesPinsEvictsAndPersists(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 10, MaxEntries: 3})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	oldKey := testKey()
	oldKey.CatalogVersion = "old-release"
	currentKey := testKey()
	currentKey.CatalogVersion = "new-release"
	newKey := testKey()
	newKey.CatalogVersion = "new-release"
	newKey.Cell = "8928308280ffffe"
	oldEntry := publishTestEntry(t, cache, oldKey, 4, "new-release")
	_ = publishTestEntry(t, cache, currentKey, 4, "new-release")
	thirdEntry := publishTestEntry(t, cache, newKey, 4, "new-release")
	assert.Assert(t, !fileExists(oldEntry.Path))
	assert.Assert(t, fileExists(thirdEntry.Path))
	assert.NilError(t, cache.Touch(currentKey))

	reader, err := cache.Open(newKey)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, errors.Is(cache.Remove(newKey), ErrEntryPinned))
	data, err := io.ReadAll(reader)
	assert.NilError(t, err)
	assert.Equal(t, len(data), 4)
	assert.NilError(t, reader.Close())
	assert.NilError(t, cache.Touch(newKey))
	assert.NilError(t, cache.FlushRecency())
	assert.NilError(t, cache.Close())

	reopened, err := New(Options{Root: root, MaxBytes: 10, MaxEntries: 3})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, reopened.Stats().UsedEntries, int64(2))
	assert.Equal(t, reopened.Stats().UsedBytes, int64(8))
	assert.NilError(t, reopened.Remove(newKey))
	assert.NilError(t, reopened.Close())
}

func TestCacheReopenDoesNotServeOldGenerationForNewKey(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 10, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	oldKey := testKey()
	oldKey.CatalogVersion = "old-release"
	publishTestEntry(t, cache, oldKey, 4, oldKey.CatalogVersion)
	assert.NilError(t, cache.Close())

	reopened, err := New(Options{Root: root, MaxBytes: 10, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	newKey := oldKey
	newKey.CatalogVersion = "new-release"
	_, err = reopened.Open(newKey)
	assert.Assert(t, errors.Is(err, ErrEntryNotFound))
	reader, err := reopened.Open(oldKey)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, reopened.Close())
		return
	}
	assert.NilError(t, reader.Close())
	assert.NilError(t, reopened.Close())
}

func TestCachePinnedCapacityAndCorruptStartupCleanup(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 8, MaxEntries: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	entry := publishTestEntry(t, cache, key, 8, key.CatalogVersion)
	reader, err := cache.Open(key)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	otherKey := key
	otherKey.Cell = "8928308280ffffe"
	staging, err := cache.CreateStaging(otherKey)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	stagingPath := staging.Name()
	assert.NilError(t, staging.Close())
	assert.NilError(t, os.WriteFile(stagingPath, bytes.Repeat([]byte("y"), 1), 0o600))
	_, err = cache.Publish(otherKey, stagingPath, key.CatalogVersion)
	assert.Assert(t, errors.Is(err, ErrCapacityUnavailable))
	assert.NilError(t, cache.RemoveStaging(stagingPath))
	assert.NilError(t, reader.Close())
	assert.NilError(t, cache.Close())

	assert.NilError(t, os.WriteFile(entry.Path, []byte("corrupt"), 0o600))
	reopened, err := New(Options{Root: root, MaxBytes: 8, MaxEntries: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.DeepEqual(t, reopened.Stats(), Stats{})
	assert.Assert(t, errors.Is(reopened.Remove(key), ErrEntryNotFound))
	assert.NilError(t, reopened.Close())
}

func publishTestEntry(t *testing.T, cache *Cache, key Key, size int, currentCatalogVersion string) Entry {
	t.Helper()
	staging, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		return Entry{}
	}
	stagingPath := staging.Name()
	_, err = staging.Write(bytes.Repeat([]byte("x"), size))
	assert.NilError(t, err)
	assert.NilError(t, staging.Sync())
	assert.NilError(t, staging.Close())
	entry, err := cache.Publish(key, stagingPath, currentCatalogVersion)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.RemoveStaging(stagingPath))
	}
	return entry
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestSchedulerCoalescesSameKeyAndCleansReservation(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	scheduler, err := NewScheduler(cache, SchedulerOptions{Workers: 1, QueueCapacity: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() {
		assert.NilError(t, scheduler.Close())
		assert.NilError(t, cache.Close())
	}()

	key := testKey()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	build := func(ctx context.Context, reservation *Reservation) error {
		calls.Add(1)
		assert.Assert(t, reservation != nil)
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- scheduler.Do(context.Background(), key, 10, build) }()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- scheduler.Do(context.Background(), key, 10, func(context.Context, *Reservation) error { return nil })
	}()
	close(release)
	assert.NilError(t, <-firstDone)
	assert.NilError(t, <-secondDone)
	assert.Equal(t, calls.Load(), int32(1))
	assert.DeepEqual(t, cache.Stats(), Stats{})
}

func TestSchedulerCallerDetachmentCancelsUnobservedBuild(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	scheduler, err := NewScheduler(cache, SchedulerOptions{Workers: 1, QueueCapacity: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() {
		assert.NilError(t, scheduler.Close())
		assert.NilError(t, cache.Close())
	}()

	callerContext, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	canceled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Do(callerContext, testKey(), 10, func(ctx context.Context, reservation *Reservation) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			return ctx.Err()
		})
	}()
	<-started
	cancel()
	assert.Assert(t, errors.Is(<-done, context.Canceled))
	<-canceled
	assert.DeepEqual(t, cache.Stats(), Stats{})
}

func TestSchedulerRejectsFullQueueAndClosedState(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	scheduler, err := NewScheduler(cache, SchedulerOptions{Workers: 1, QueueCapacity: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- scheduler.Do(context.Background(), key, 10, func(ctx context.Context, reservation *Reservation) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	secondContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- scheduler.Do(secondContext, Key{CatalogVersion: "release-2", ProjectionID: "projection", Cell: "cell-2", SizePolicyID: "size"}, 10, func(context.Context, *Reservation) error { return nil })
	}()
	deadline := time.Now().Add(time.Second)
	for len(scheduler.queue) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	err = scheduler.Do(context.Background(), Key{CatalogVersion: "release-3", ProjectionID: "projection", Cell: "cell-3", SizePolicyID: "size"}, 10, func(context.Context, *Reservation) error { return nil })
	assert.Assert(t, errors.Is(err, ErrQueueFull))
	cancel()
	assert.Assert(t, errors.Is(<-secondDone, context.Canceled))
	close(release)
	assert.NilError(t, <-firstDone)
	assert.NilError(t, scheduler.Close())
	assert.NilError(t, cache.Close())
}

func testKey() Key {
	return Key{
		CatalogVersion: "release+sha256:catalog",
		ProjectionID:   "sha256:projection",
		Cell:           "8928308280fffff",
		SizePolicyID:   "sha256:size-policy",
	}
}
