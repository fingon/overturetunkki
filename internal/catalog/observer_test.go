package catalog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

type observerCheckStep struct {
	observation *Observation
	snapshot    Snapshot
	wait        <-chan struct{}
	err         error
}

type observerChecker struct {
	mu      sync.Mutex
	steps   []observerCheckStep
	calls   int
	started chan int
}

func (checker *observerChecker) Refresh(ctx context.Context) (Snapshot, error) {
	return checker.runStep(ctx, nil)
}

func (checker *observerChecker) RefreshObserved(ctx context.Context, onObserved func(Observation)) (Snapshot, error) {
	return checker.runStep(ctx, onObserved)
}

func (checker *observerChecker) runStep(ctx context.Context, onObserved func(Observation)) (Snapshot, error) {
	checker.mu.Lock()
	index := checker.calls
	checker.calls++
	step := checker.steps[len(checker.steps)-1]
	if index < len(checker.steps) {
		step = checker.steps[index]
	}
	checker.mu.Unlock()
	if checker.started != nil {
		checker.started <- index
	}
	if step.observation != nil && onObserved != nil {
		onObserved(*step.observation)
	}
	if step.wait != nil {
		select {
		case <-step.wait:
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}
	if step.err != nil {
		return Snapshot{}, step.err
	}
	return step.snapshot, nil
}

func (checker *observerChecker) callCount() int {
	checker.mu.Lock()
	defer checker.mu.Unlock()
	return checker.calls
}

func observerTestSnapshot(release string) Snapshot {
	return Snapshot{
		Release:        release,
		CatalogVersion: fmt.Sprintf("%s+sha256:test", release),
		CollectionID:   DefaultCollectionID,
		Manifest: []Asset{{
			PartitionID:   "00000",
			Href:          "https://example.invalid/part-00000.zstd.parquet",
			RowCount:      1,
			RowGroupCount: 1,
			SizeBytes:     1,
		}},
		Schema: Schema{
			Columns:           []Column{{Name: "id"}, {Name: "geometry"}},
			GeoParquetVersion: "1.1.0",
			PrimaryGeometry:   "geometry",
		},
	}
}

func observerTestObservation(snapshot Snapshot) Observation {
	return Observation{Release: snapshot.Release, CatalogVersion: snapshot.CatalogVersion}
}

func TestNewObserverValidatesOptions(t *testing.T) {
	cases := []struct {
		name    string
		checker Checker
		period  time.Duration
		message string
	}{
		{name: "nil checker", period: time.Second, message: "checker"},
		{name: "zero period", checker: &observerChecker{}, message: "positive"},
		{name: "negative period", checker: &observerChecker{}, period: -time.Second, message: "positive"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewObserver(test.checker, test.period)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestObserverStartsUnreadyUntilFreshCheck(t *testing.T) {
	startupErr := errors.New("stale startup state")
	checker := &observerChecker{steps: []observerCheckStep{{err: startupErr}}}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = observer.Current()
	assert.Assert(t, errors.Is(err, ErrCatalogNotReady))

	_, err = observer.Refresh(context.Background())
	assert.ErrorContains(t, err, startupErr.Error())
	state := observer.State()
	assert.Assert(t, !state.Ready)
	assert.Assert(t, errors.Is(state.Err, startupErr))
	_, err = observer.Current()
	assert.Assert(t, errors.Is(err, ErrCatalogUnavailable))
}

func TestObserverCoalescesOverlappingRefreshes(t *testing.T) {
	release := make(chan struct{})
	snapshot := observerTestSnapshot("old")
	checker := &observerChecker{
		steps: []observerCheckStep{{
			observation: func() *Observation {
				observation := observerTestObservation(snapshot)
				return &observation
			}(),
			snapshot: snapshot,
			wait:     release,
		}},
		started: make(chan int, 2),
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := observer.Refresh(context.Background())
		firstResult <- err
	}()
	select {
	case <-checker.started:
	case <-time.After(time.Second):
		t.Fatal("first check did not start")
	}
	secondResult := make(chan error, 1)
	callerContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, secondErr := observer.Refresh(callerContext)
	cancel()
	secondResult <- secondErr
	close(release)
	assert.NilError(t, receiveObserverError(t, firstResult))
	assert.ErrorContains(t, receiveObserverError(t, secondResult), "deadline exceeded")
	assert.Equal(t, checker.callCount(), 1)
}

func TestObserverCallerCancellationDetachesFromSharedCheck(t *testing.T) {
	release := make(chan struct{})
	snapshot := observerTestSnapshot("old")
	checker := &observerChecker{
		steps:   []observerCheckStep{{snapshot: snapshot, wait: release}},
		started: make(chan int, 1),
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := observer.Refresh(context.Background())
		firstResult <- err
	}()
	select {
	case <-checker.started:
	case <-time.After(time.Second):
		t.Fatal("check did not start")
	}
	callerContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = observer.Refresh(callerContext)
	assert.ErrorContains(t, err, "deadline exceeded")
	close(release)
	assert.NilError(t, receiveObserverError(t, firstResult))
	assert.Equal(t, checker.callCount(), 1)
}

func TestObserverFencesGenerationBeforeFailedReplacement(t *testing.T) {
	release := make(chan struct{})
	oldSnapshot := observerTestSnapshot("old")
	replacementErr := errors.New("incompatible replacement")
	checker := &observerChecker{
		steps: []observerCheckStep{
			{snapshot: oldSnapshot},
			{
				observation: &Observation{Release: "new"},
				wait:        release,
				err:         replacementErr,
			},
		},
		started: make(chan int, 2),
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, <-checker.started, 0)
	oldGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}

	checkResult := make(chan error, 1)
	go func() {
		_, err := observer.Refresh(context.Background())
		checkResult <- err
	}()
	select {
	case index := <-checker.started:
		assert.Equal(t, index, 1)
	case <-time.After(time.Second):
		t.Fatal("replacement check did not start")
	}
	select {
	case <-oldGeneration.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("old generation was not fenced")
	}
	_, err = observer.Current()
	assert.Assert(t, errors.Is(err, ErrCatalogNotReady))
	close(release)
	assert.ErrorContains(t, receiveObserverError(t, checkResult), "incompatible replacement")
	state := observer.State()
	assert.Assert(t, !state.Ready)
	assert.Assert(t, errors.Is(state.Err, replacementErr))
	_, err = observer.Current()
	assert.Assert(t, errors.Is(err, ErrCatalogUnavailable))
}

func TestObserverPublishesReplacementAndCancelsOldGeneration(t *testing.T) {
	release := make(chan struct{})
	oldSnapshot := observerTestSnapshot("old")
	newSnapshot := observerTestSnapshot("new")
	checker := &observerChecker{
		steps: []observerCheckStep{
			{snapshot: oldSnapshot},
			{observation: &Observation{Release: "new"}, snapshot: newSnapshot, wait: release},
		},
		started: make(chan int, 2),
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, <-checker.started, 0)
	oldGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}
	checkResult := make(chan error, 1)
	go func() {
		_, err := observer.Refresh(context.Background())
		checkResult <- err
	}()
	select {
	case index := <-checker.started:
		assert.Equal(t, index, 1)
	case <-time.After(time.Second):
		t.Fatal("replacement check did not start")
	}
	select {
	case <-oldGeneration.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("old generation was not canceled")
	}
	close(release)
	assert.NilError(t, receiveObserverError(t, checkResult))
	newGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, newGeneration.Number, oldGeneration.Number+1)
	assert.Equal(t, newGeneration.Snapshot.Release, "new")
	select {
	case <-newGeneration.Context.Done():
		t.Fatal("new generation was canceled")
	default:
	}
}

func TestObserverPublishesRollbackAsNewGeneration(t *testing.T) {
	oldSnapshot := observerTestSnapshot("old")
	newSnapshot := observerTestSnapshot("new")
	checker := &observerChecker{steps: []observerCheckStep{
		{snapshot: oldSnapshot},
		{snapshot: newSnapshot},
		{snapshot: oldSnapshot},
	}}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}

	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	oldGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}

	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	newGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, newGeneration.Number, oldGeneration.Number+1)
	assert.Equal(t, newGeneration.Snapshot.Release, newSnapshot.Release)
	select {
	case <-oldGeneration.Context.Done():
	default:
		t.Fatal("old generation was not canceled")
	}

	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	rollbackGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, rollbackGeneration.Number, newGeneration.Number+1)
	assert.Equal(t, rollbackGeneration.Snapshot.Release, oldSnapshot.Release)
	select {
	case <-newGeneration.Context.Done():
	default:
		t.Fatal("rolled-forward generation was not canceled")
	}
}

func TestObserverFailureFencesCurrentGeneration(t *testing.T) {
	oldSnapshot := observerTestSnapshot("old")
	checker := &observerChecker{
		steps: []observerCheckStep{{snapshot: oldSnapshot}, {err: errors.New("upstream unavailable")}},
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = observer.Refresh(context.Background())
	assert.NilError(t, err)
	oldGeneration, err := observer.Current()
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = observer.Refresh(context.Background())
	assert.ErrorContains(t, err, "upstream unavailable")
	select {
	case <-oldGeneration.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("failed check did not cancel current generation")
	}
	state := observer.State()
	assert.Assert(t, !state.Ready)
	assert.Assert(t, state.Err != nil)
	_, err = observer.Current()
	assert.Assert(t, errors.Is(err, ErrCatalogUnavailable))
}

func TestObserverRunRetriesStartupAndPollsWhileIdle(t *testing.T) {
	snapshot := observerTestSnapshot("old")
	checker := &observerChecker{
		steps: []observerCheckStep{
			{err: errors.New("startup unavailable")},
			{snapshot: snapshot},
			{snapshot: snapshot},
		},
		started: make(chan int, 100),
	}
	observer, err := NewObserver(checker, time.Millisecond)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- observer.Run(ctx) }()
	select {
	case index := <-checker.started:
		assert.Equal(t, index, 0)
	case <-time.After(time.Second):
		t.Fatal("startup check did not start")
	}
	select {
	case index := <-checker.started:
		assert.Assert(t, index >= 1)
	case <-time.After(time.Second):
		t.Fatal("idle check did not start after startup failure")
	}
	waitForObserverReady(t, observer)
	cancel()
	assert.NilError(t, receiveObserverError(t, runResult))
	assert.Assert(t, checker.callCount() >= 2)
}

func TestObserverRejectsConcurrentRun(t *testing.T) {
	release := make(chan struct{})
	checker := &observerChecker{
		steps:   []observerCheckStep{{snapshot: observerTestSnapshot("old"), wait: release}},
		started: make(chan int, 1),
	}
	observer, err := NewObserver(checker, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runResult := make(chan error, 1)
	go func() { runResult <- observer.Run(ctx) }()
	select {
	case <-checker.started:
	case <-time.After(time.Second):
		t.Fatal("run did not start")
	}
	assert.Assert(t, errors.Is(observer.Run(context.Background()), ErrObserverAlreadyRun))
	close(release)
	waitForObserverReady(t, observer)
	cancel()
	assert.NilError(t, receiveObserverError(t, runResult))
}

func receiveObserverError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("observer operation did not finish")
		return nil
	}
}

func waitForObserverReady(t *testing.T, observer *Observer) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if observer.Ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("observer did not become ready")
}
