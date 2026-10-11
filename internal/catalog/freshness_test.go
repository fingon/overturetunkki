//nolint:goconst // Independent scenarios use readable test literals.
package catalog

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

const testFreshness = 24 * time.Hour

type observerClock struct{ unixNS atomic.Int64 }

func (clock *observerClock) Now() time.Time                 { return time.Unix(0, clock.unixNS.Load()) }
func (clock *observerClock) Advance(duration time.Duration) { clock.unixNS.Add(int64(duration)) }

func newFreshnessObserver(t *testing.T, steps []observerCheckStep) (*Observer, *observerChecker, *observerClock) {
	t.Helper()
	clock := &observerClock{}
	clock.unixNS.Store(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC).UnixNano())
	checker := &observerChecker{steps: steps}
	observer, err := NewObserver(checker, ObserverOptions{PollInterval: testFreshness, StaleGrace: testFreshness, Now: clock.Now})
	assert.NilError(t, err)
	_, err = observer.Refresh(t.Context())
	assert.NilError(t, err)
	return observer, checker, clock
}

func renewObserver(t *testing.T, observer *Observer) {
	t.Helper()
	observer.mu.Lock()
	call := observer.inFlight
	observer.mu.Unlock()
	if call != nil {
		select {
		case <-call.done:
		case <-time.After(time.Second):
			t.Fatal("renewal did not finish")
		}
	}
}

func TestObserverSharesDailyFreshness(t *testing.T) {
	snapshot := observerTestSnapshot("old")
	observer, checker, clock := newFreshnessObserver(t, []observerCheckStep{{snapshot: snapshot}})
	clock.Advance(testFreshness - time.Nanosecond)
	var callers sync.WaitGroup
	for range 100 {
		callers.Go(func() {
			got, err := observer.Refresh(t.Context())
			assert.NilError(t, err)
			assert.Equal(t, got.CatalogVersion, snapshot.CatalogVersion)
			got.Manifest[0].Href = "caller mutation"
		})
	}
	callers.Wait()
	assert.Equal(t, checker.callCount(), 1)
	assert.Equal(t, observer.State().RefreshSuccesses, uint64(1))
	assert.Equal(t, observer.State().Snapshot.Manifest[0].Href, snapshot.Manifest[0].Href)
	clock.Advance(time.Nanosecond)
	_, err := observer.Refresh(t.Context())
	assert.NilError(t, err)
	renewObserver(t, observer)
	assert.Equal(t, checker.callCount(), 2)
	assert.Equal(t, observer.State().LastSuccess, clock.Now())
}

func TestObserverGraceFailuresAndRecovery(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		grace bool
	}{
		{name: "rate limited", err: &UpstreamError{StatusCode: http.StatusTooManyRequests}, grace: true},
		{name: "upstream outage", err: &UpstreamError{StatusCode: http.StatusServiceUnavailable}, grace: true},
		{name: "network", err: &url.Error{Op: "Get", URL: "https://example.invalid", Err: errors.New("connection lost")}, grace: true},
		{name: "timeout", err: fmt.Errorf("fetch timed out: %w", context.DeadlineExceeded), grace: true},
		{name: "malformed catalog", err: errors.New("invalid JSON")},
		{name: "unsupported schema", err: errors.New("unsupported schema")},
		{name: "missing catalog", err: &UpstreamError{StatusCode: http.StatusNotFound}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			snapshot := observerTestSnapshot("old")
			observer, checker, clock := newFreshnessObserver(t, []observerCheckStep{{snapshot: snapshot}, {err: test.err}, {snapshot: snapshot}})
			original, err := observer.Current()
			assert.NilError(t, err)
			clock.Advance(testFreshness)
			_, err = observer.Refresh(t.Context())
			assert.NilError(t, err)
			renewObserver(t, observer)
			assert.Equal(t, observer.Ready(), test.grace)
			assert.Equal(t, observer.State().RefreshFailures, uint64(1))
			assert.Equal(t, original.Context.Err() == nil, test.grace)
			for range 20 {
				_, err = observer.Refresh(t.Context())
				assert.Equal(t, err == nil, test.grace)
			}
			assert.Equal(t, checker.callCount(), 2)
			clock.Advance(2 * time.Minute)
			_, err = observer.Refresh(t.Context())
			assert.NilError(t, err)
			renewObserver(t, observer)
			assert.Assert(t, observer.Ready())
			assert.Equal(t, observer.State().RefreshSuccesses, uint64(2))
			assert.Equal(t, observer.state.consecutiveFailures, 0)
		})
	}
}

func TestObserverRetryAfterAndGraceExpiry(t *testing.T) {
	snapshot := observerTestSnapshot("old")
	clockTime := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	retryAt := clockTime.Add(3 * testFreshness)
	observer, checker, clock := newFreshnessObserver(t, []observerCheckStep{
		{snapshot: snapshot},
		{err: &UpstreamError{StatusCode: http.StatusTooManyRequests, RetryAt: retryAt}},
		{snapshot: snapshot},
	})
	generation, err := observer.Current()
	assert.NilError(t, err)
	clock.Advance(testFreshness)
	_, err = observer.Refresh(t.Context())
	assert.NilError(t, err)
	renewObserver(t, observer)
	assert.Equal(t, observer.state.nextCheck, retryAt)
	clock.Advance(testFreshness - time.Nanosecond)
	assert.Assert(t, observer.Ready())
	clock.Advance(time.Nanosecond)
	_, err = observer.Refresh(t.Context())
	assert.Assert(t, errors.Is(err, ErrCatalogUnavailable))
	assert.Assert(t, !observer.Ready())
	assert.Assert(t, generation.Context.Err() != nil)
	called := false
	err = observer.WithCurrent(func(Generation) { called = true })
	assert.Assert(t, errors.Is(err, ErrCatalogUnavailable))
	assert.Assert(t, !called)
	assert.Equal(t, checker.callCount(), 2)
	clock.Advance(testFreshness)
	_, err = observer.Refresh(t.Context())
	assert.NilError(t, err)
	assert.Assert(t, observer.Ready())
	assert.Equal(t, checker.callCount(), 3)
}

func TestObserverRenewalCoalescesDuringGrace(t *testing.T) {
	gate := make(chan struct{})
	snapshot := observerTestSnapshot("old")
	observer, checker, clock := newFreshnessObserver(t, []observerCheckStep{{snapshot: snapshot}, {snapshot: snapshot, wait: gate}})
	clock.Advance(testFreshness)
	_, err := observer.Refresh(t.Context())
	assert.NilError(t, err)
	for range 20 {
		_, err = observer.Refresh(t.Context())
		assert.NilError(t, err)
	}
	observer.mu.Lock()
	call := observer.inFlight
	observer.mu.Unlock()
	close(gate)
	select {
	case <-call.done:
	case <-time.After(time.Second):
		t.Fatal("renewal did not finish")
	}
	assert.Equal(t, checker.callCount(), 2)
}

func TestObserverFencesObservedChangesDuringGrace(t *testing.T) {
	cases := []struct {
		name        string
		observation Observation
	}{
		{name: "release", observation: Observation{Release: "new"}},
		{name: "manifest", observation: Observation{Release: "old", CatalogVersion: "old+changed"}},
		{name: "rollback", observation: Observation{Release: "older"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			gate := make(chan struct{})
			snapshot := observerTestSnapshot("old")
			observer, checker, clock := newFreshnessObserver(t, []observerCheckStep{
				{snapshot: snapshot}, {observation: &test.observation, wait: gate, err: &UpstreamError{StatusCode: http.StatusServiceUnavailable}},
			})
			original, err := observer.Current()
			assert.NilError(t, err)
			clock.Advance(testFreshness)
			_, err = observer.Refresh(t.Context())
			assert.NilError(t, err)
			select {
			case <-original.Context.Done():
			case <-time.After(time.Second):
				t.Fatal("observed change did not fence generation")
			}
			assert.Assert(t, !observer.Ready())
			observer.mu.Lock()
			call := observer.inFlight
			observer.mu.Unlock()
			close(gate)
			select {
			case <-call.done:
			case <-time.After(time.Second):
				t.Fatal("replacement did not finish")
			}
			assert.Assert(t, !observer.Ready())
			assert.Equal(t, checker.callCount(), 2)
			assert.Equal(t, observer.State().Snapshot.Release, "")
		})
	}
}

func TestObserverStartupCooldown(t *testing.T) {
	clock := &observerClock{}
	clock.unixNS.Store(time.Now().UnixNano())
	checker := &observerChecker{steps: []observerCheckStep{{err: &UpstreamError{StatusCode: http.StatusTooManyRequests}}, {snapshot: observerTestSnapshot("old")}}}
	observer, err := NewObserver(checker, ObserverOptions{PollInterval: testFreshness, StaleGrace: testFreshness, Now: clock.Now})
	assert.NilError(t, err)
	for range 20 {
		_, err = observer.Refresh(t.Context())
		assert.Assert(t, err != nil)
	}
	assert.Equal(t, checker.callCount(), 1)
	clock.Advance(2 * time.Minute)
	_, err = observer.Refresh(t.Context())
	assert.NilError(t, err)
	assert.Assert(t, observer.Ready())
}

func TestObserverGraceConfiguration(t *testing.T) {
	cases := []struct {
		name            string
		interval, grace time.Duration
		valid           bool
	}{
		{name: "no grace", interval: testFreshness, valid: true},
		{name: "negative grace", interval: testFreshness, grace: -time.Second},
		{name: "overflow", interval: time.Duration(1<<63 - 1), grace: time.Second},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewObserver(&observerChecker{}, ObserverOptions{PollInterval: test.interval, StaleGrace: test.grace})
			assert.Equal(t, err == nil, test.valid)
		})
	}
}

func TestObserverRetryBackoff(t *testing.T) {
	observer := &Observer{retryBase: catalogRetryBase}
	for attempt := 1; attempt <= 20; attempt++ {
		observer.state.consecutiveFailures = attempt
		delay := observer.retryDelayLocked()
		floor := min(catalogRetryBase*time.Duration(1<<min(attempt-1, 6)), catalogRetryMax)
		assert.Assert(t, delay >= floor && delay <= catalogRetryMax)
		if floor < catalogRetryMax {
			assert.Assert(t, delay > floor)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name, value string
		want        time.Time
	}{
		{name: "seconds", value: "120", want: now.Add(2 * time.Minute)},
		{name: "zero", value: "0", want: now},
		{name: "date", value: now.Add(time.Hour).Format(http.TimeFormat), want: now.Add(time.Hour)},
		{name: "past date", value: now.Add(-time.Hour).Format(http.TimeFormat)},
		{name: "empty"},
		{name: "invalid", value: "tomorrow"},
		{name: "negative", value: "-1"},
		{name: "overflow", value: "9223372036854775807"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) { assert.Equal(t, parseRetryAfter(test.value, now), test.want) })
	}
}
