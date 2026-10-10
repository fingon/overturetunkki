//nolint:goconst // Repeated literals keep independent test cases readable.
package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	oldEntry := cache.publishTestEntry(t, oldKey, 4, "new-release")
	_ = cache.publishTestEntry(t, currentKey, 4, "new-release")
	thirdEntry := cache.publishTestEntry(t, newKey, 4, "new-release")
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
	cache.publishTestEntry(t, oldKey, 4, oldKey.CatalogVersion)
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

func TestCacheRemovesOrphanFilesAtStartup(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 20, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	tilePath, err := cache.Path(testKey())
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.Close())
		return
	}
	sidecarKey := testKey()
	sidecarKey.Cell = "8928308280ffffe"
	sidecarPath, err := cache.Path(sidecarKey)
	assert.NilError(t, err)
	assert.NilError(t, cache.Close())
	assert.NilError(t, os.MkdirAll(filepath.Dir(tilePath), 0o750))
	assert.NilError(t, os.MkdirAll(filepath.Dir(sidecarPath), 0o750))
	assert.NilError(t, os.WriteFile(tilePath, []byte("orphan tile"), 0o600))
	assert.NilError(t, os.WriteFile(strings.TrimSuffix(sidecarPath, fileSuffix)+sidecarSuffix, []byte("{}"), 0o600))

	reopened, err := New(Options{Root: root, MaxBytes: 20, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, !fileExists(tilePath))
	assert.Assert(t, !fileExists(strings.TrimSuffix(sidecarPath, fileSuffix)+sidecarSuffix))
	assert.DeepEqual(t, reopened.Stats(), Stats{})
	assert.NilError(t, reopened.Close())
}

func TestCacheRemovesPublishedTileWhenSidecarPublicationIsInterrupted(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 100, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	reservation, err := cache.Reserve(context.Background(), 30)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.Close())
		return
	}
	key := testKey()
	staging, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.Close())
		return
	}
	stagingPath := staging.Name()
	_, err = staging.Write([]byte("interrupted publication"))
	assert.NilError(t, err)
	assert.NilError(t, staging.Sync())
	assert.NilError(t, staging.Close())
	finalPath, err := cache.Path(key)
	assert.NilError(t, err)
	sidecarPath := strings.TrimSuffix(finalPath, fileSuffix) + sidecarSuffix
	assert.NilError(t, os.MkdirAll(filepath.Dir(sidecarPath), 0o750))
	assert.NilError(t, os.Mkdir(sidecarPath, 0o750))
	_, err = reservation.Publish(key, stagingPath, key.CatalogVersion)
	assert.ErrorContains(t, err, "publish sidecar")
	assert.Assert(t, !fileExists(finalPath))
	assert.Assert(t, !fileExists(stagingPath))
	assert.NilError(t, reservation.Release())
	assert.DeepEqual(t, cache.Stats(), Stats{})
	assert.NilError(t, cache.Close())
}

func TestCacheReportsPublishStorageFailure(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 20, MaxEntries: 2})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	staging, err := cache.CreateStaging(key)
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.Close())
		return
	}
	stagingPath := staging.Name()
	_, err = staging.Write([]byte("storage failure"))
	assert.NilError(t, err)
	assert.NilError(t, staging.Sync())
	assert.NilError(t, staging.Close())
	finalPath, err := cache.Path(key)
	assert.NilError(t, err)
	assert.NilError(t, os.WriteFile(filepath.Dir(finalPath), []byte("not a directory"), 0o600))
	_, err = cache.Publish(key, stagingPath, key.CatalogVersion)
	assert.ErrorContains(t, err, "create cache entry directory")
	assert.NilError(t, cache.RemoveStaging(stagingPath))
	assert.NilError(t, cache.Close())
}

func TestCachePinnedCapacityAndCorruptStartupCleanup(t *testing.T) {
	root := t.TempDir()
	cache, err := New(Options{Root: root, MaxBytes: 8, MaxEntries: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	entry := cache.publishTestEntry(t, key, 8, key.CatalogVersion)
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

func (cache *Cache) publishTestEntry(t *testing.T, key Key, size int, currentCatalogVersion string) Entry {
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
	waitForSchedulerWaiters(t, scheduler, key, 2)
	close(release)
	assert.NilError(t, <-firstDone)
	assert.NilError(t, <-secondDone)
	assert.Equal(t, calls.Load(), int32(1))
	assert.DeepEqual(t, cache.Stats(), Stats{})
}

func TestSchedulerRunsDifferentKeysInParallel(t *testing.T) {
	cache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	scheduler, err := NewScheduler(cache, SchedulerOptions{Workers: 2, QueueCapacity: 2})
	assert.NilError(t, err)
	if err != nil {
		assert.NilError(t, cache.Close())
		return
	}
	started := make(chan string, 2)
	release := make(chan struct{})
	build := func(cell string) JobFunc {
		return func(ctx context.Context, _ *Reservation) error {
			started <- cell
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	firstKey := testKey()
	secondKey := firstKey
	secondKey.Cell = "8928308280ffffe"
	results := make(chan error, 2)
	go func() { results <- scheduler.Do(context.Background(), firstKey, 10, build(firstKey.Cell)) }()
	go func() { results <- scheduler.Do(context.Background(), secondKey, 10, build(secondKey.Cell)) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("scheduler did not start both jobs")
		}
	}
	close(release)
	assert.NilError(t, <-results)
	assert.NilError(t, <-results)
	assert.DeepEqual(t, cache.Stats(), Stats{})
	assert.NilError(t, scheduler.Close())
	assert.NilError(t, cache.Close())
}

func TestSchedulerDetachedRunningBuildPublishes(t *testing.T) {
	for _, reattach := range []bool{false, true} {
		t.Run(fmt.Sprintf("reattach=%t", reattach), func(t *testing.T) {
			tileCache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
			assert.NilError(t, err)
			scheduler, err := NewScheduler(tileCache, SchedulerOptions{Workers: 1, QueueCapacity: 1})
			assert.NilError(t, err)
			defer func() { assert.NilError(t, scheduler.Close()); assert.NilError(t, tileCache.Close()) }()
			callerContext, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := make(chan struct{})
			release := make(chan struct{})
			published := make(chan struct{})
			var calls atomic.Int32
			key := testKey()
			build := func(ctx context.Context, reservation *Reservation) error {
				calls.Add(1)
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				staging, createErr := tileCache.CreateStaging(key)
				if createErr != nil {
					return createErr
				}
				if closeErr := staging.Close(); closeErr != nil {
					return closeErr
				}
				if writeErr := os.WriteFile(staging.Name(), []byte("tile"), 0o600); writeErr != nil {
					return writeErr
				}
				if _, publishErr := reservation.Publish(key, staging.Name(), key.CatalogVersion); publishErr != nil {
					return publishErr
				}
				close(published)
				return nil
			}
			first := make(chan error, 1)
			go func() { first <- scheduler.Do(callerContext, key, 10, build) }()
			<-started
			cancel()
			assert.Assert(t, errors.Is(<-first, context.Canceled))
			scheduler.mu.Lock()
			current := scheduler.jobs[mustKeyDigest(t, key)]
			assert.Assert(t, current != nil)
			assert.NilError(t, current.context.Err())
			scheduler.mu.Unlock()
			var second chan error
			if reattach {
				second = make(chan error, 1)
				go func() {
					second <- scheduler.Do(t.Context(), key, 10, func(context.Context, *Reservation) error { return errors.New("duplicate build") })
				}()
				waitForSchedulerWaiters(t, scheduler, key, 1)
			}
			close(release)
			select {
			case <-published:
			case <-time.After(time.Second):
				t.Fatal("detached build did not publish")
			}
			if reattach {
				assert.NilError(t, <-second)
			}
			reader, err := tileCache.Open(key)
			assert.NilError(t, err)
			data, err := io.ReadAll(reader)
			assert.NilError(t, err)
			assert.Equal(t, string(data), "tile")
			assert.NilError(t, reader.Close())
			assert.Equal(t, calls.Load(), int32(1))
			waitForCacheStats(t, tileCache, Stats{UsedBytes: 4, UsedEntries: 1})
		})
	}
}

func mustKeyDigest(t *testing.T, key Key) string {
	t.Helper()
	digest, err := key.digest()
	assert.NilError(t, err)
	return digest
}

func waitForSchedulerWaiters(t *testing.T, scheduler *Scheduler, key Key, expected int) {
	t.Helper()
	digest := mustKeyDigest(t, key)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		scheduler.mu.Lock()
		current := scheduler.jobs[digest]
		matches := current != nil && current.waiters == expected
		scheduler.mu.Unlock()
		if matches {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("callers did not attach to the shared job")
}

func TestSchedulerCanceledQueuedJobCanBeRetried(t *testing.T) {
	tileCache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	scheduler, err := NewScheduler(tileCache, SchedulerOptions{Workers: 1, QueueCapacity: 2})
	assert.NilError(t, err)
	defer func() { assert.NilError(t, scheduler.Close()); assert.NilError(t, tileCache.Close()) }()
	started := make(chan struct{})
	release := make(chan struct{})
	first := make(chan error, 1)
	key := testKey()
	go func() {
		first <- scheduler.Do(t.Context(), key, 10, func(context.Context, *Reservation) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	queuedKey := key
	queuedKey.Cell = "queued"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queued := make(chan error, 1)
	var calls atomic.Int32
	build := func(context.Context, *Reservation) error { calls.Add(1); return nil }
	go func() { queued <- scheduler.Do(ctx, queuedKey, 10, build) }()
	waitForSchedulerWaiters(t, scheduler, queuedKey, 1)
	cancel()
	assert.Assert(t, errors.Is(<-queued, context.Canceled))
	close(release)
	assert.NilError(t, <-first)
	assert.NilError(t, scheduler.Do(t.Context(), queuedKey, 10, build))
	assert.Equal(t, calls.Load(), int32(1))
	assert.DeepEqual(t, tileCache.Stats(), Stats{})
}

func TestSchedulerRejectsCanceledCallerAndCancelsOnClose(t *testing.T) {
	tileCache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	scheduler, err := NewScheduler(tileCache, SchedulerOptions{Workers: 1, QueueCapacity: 1})
	assert.NilError(t, err)
	defer func() { assert.NilError(t, scheduler.Close()); assert.NilError(t, tileCache.Close()) }()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var calls atomic.Int32
	err = scheduler.Do(ctx, testKey(), 10, func(context.Context, *Reservation) error { calls.Add(1); return nil })
	assert.Assert(t, errors.Is(err, context.Canceled))
	assert.Equal(t, calls.Load(), int32(0))
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Do(ctx, testKey(), 10, func(ctx context.Context, _ *Reservation) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-started
	cancel()
	assert.Assert(t, errors.Is(<-done, context.Canceled))
	assert.NilError(t, scheduler.Close())
	assert.DeepEqual(t, tileCache.Stats(), Stats{})
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
		firstDone <- scheduler.Do(context.Background(), key, 10, func(_ context.Context, _ *Reservation) error {
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

func waitForCacheStats(t *testing.T, cache *Cache, expected Stats) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cache.Stats() == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	assert.DeepEqual(t, cache.Stats(), expected)
}

func TestSchedulerCanceledCallerDoesNotCancelSharedBuild(t *testing.T) {
	tileCache, err := New(Options{Root: t.TempDir(), MaxBytes: 100, MaxEntries: 4})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	scheduler, err := NewScheduler(tileCache, SchedulerOptions{Workers: 1, QueueCapacity: 1})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() { assert.NilError(t, scheduler.Close()); assert.NilError(t, tileCache.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		first <- scheduler.Do(ctx, testKey(), 10, func(ctx context.Context, _ *Reservation) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	<-started
	go func() {
		second <- scheduler.Do(context.Background(), testKey(), 10, func(context.Context, *Reservation) error { return errors.New("shared build ran twice") })
	}()
	deadline := time.Now().Add(time.Second)
	for {
		scheduler.mu.Lock()
		waiters := 0
		for _, current := range scheduler.jobs {
			waiters = current.waiters
		}
		scheduler.mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second caller did not attach")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	assert.Assert(t, errors.Is(<-first, context.Canceled))
	release <- struct{}{}
	assert.NilError(t, <-second)
}
