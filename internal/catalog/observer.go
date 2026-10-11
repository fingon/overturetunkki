package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrCatalogNotReady    = errors.New("catalog is not ready")
	ErrCatalogUnavailable = errors.New("catalog is unavailable")
	ErrObserverAlreadyRun = errors.New("catalog observer is already running")
)

type Observation struct {
	Release        string
	CatalogVersion string
}

type Checker interface {
	Refresh(context.Context) (Snapshot, error)
}

type ObservedChecker interface {
	Checker
	RefreshObserved(context.Context, func(Observation)) (Snapshot, error)
}

const (
	catalogRetryBase          = time.Minute
	catalogRetryMax           = time.Hour
	catalogRetryJitterDivisor = 5
)

type ObserverOptions struct {
	PollInterval time.Duration
	StaleGrace   time.Duration
	Now          func() time.Time
}

type Observer struct {
	checker      Checker
	pollInterval time.Duration
	staleGrace   time.Duration
	now          func() time.Time
	retryBase    time.Duration
	wake         chan struct{}

	mu       sync.Mutex
	runMu    sync.Mutex
	running  bool
	inFlight *observationCall
	state    observerState
}

type observerState struct {
	lastSuccess         time.Time
	nextCheck           time.Time
	failures            uint64
	successes           uint64
	consecutiveFailures int
	ready               bool
	generation          uint64
	snapshot            Snapshot
	identity            Observation
	pending             Observation
	lastError           error
	activeContext       context.Context
	cancelActive        context.CancelFunc
}

type observationCall struct {
	done     chan struct{}
	snapshot Snapshot
	err      error
}

type State struct {
	LastSuccess      time.Time
	RefreshSuccesses uint64
	RefreshFailures  uint64
	Ready            bool
	Generation       uint64
	Snapshot         Snapshot
	Err              error
}

type Generation struct {
	Number   uint64
	Snapshot Snapshot
	Context  context.Context
}

func NewObserver(checker Checker, options ObserverOptions) (*Observer, error) {
	if checker == nil {
		return nil, errors.New("catalog checker is nil")
	}
	if options.PollInterval <= 0 {
		return nil, errors.New("catalog poll interval must be positive")
	}
	if options.StaleGrace < 0 || options.StaleGrace > time.Duration(1<<63-1)-options.PollInterval {
		return nil, errors.New("catalog stale grace must be nonnegative and fit with poll interval")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Observer{
		checker: checker, pollInterval: options.PollInterval, staleGrace: options.StaleGrace,
		now: options.Now, retryBase: catalogRetryBase, wake: make(chan struct{}, 1),
	}, nil
}

func (o *Observer) Refresh(ctx context.Context) (Snapshot, error) {
	if o == nil {
		return Snapshot{}, errors.New("catalog observer is nil")
	}
	if ctx == nil {
		return Snapshot{}, errors.New("catalog observer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("catalog observer canceled before start: %w", err)
	}

	o.mu.Lock()
	now := o.now()
	o.expireLocked(now)
	if o.state.ready && now.Before(o.state.lastSuccess.Add(o.pollInterval)) {
		snapshot := cloneSnapshot(o.state.snapshot)
		o.mu.Unlock()
		return snapshot, nil
	}
	call := o.inFlight
	if call == nil && !now.Before(o.state.nextCheck) {
		call = &observationCall{done: make(chan struct{})}
		o.inFlight = call
		o.state.pending = Observation{}
		checkContext, cancel := detachedCheckContext(ctx)
		go o.runCheck(checkContext, call, cancel)
	}
	if o.state.ready {
		snapshot := cloneSnapshot(o.state.snapshot)
		o.mu.Unlock()
		return snapshot, nil
	}
	if call == nil {
		err := fmt.Errorf("%w: %w", ErrCatalogUnavailable, o.state.lastError)
		o.mu.Unlock()
		return Snapshot{}, err
	}
	o.mu.Unlock()

	select {
	case <-call.done:
		if call.err != nil {
			return Snapshot{}, call.err
		}
		return cloneSnapshot(call.snapshot), nil
	case <-ctx.Done():
		return Snapshot{}, fmt.Errorf("catalog observer caller canceled: %w", ctx.Err())
	}
}

func (o *Observer) Run(ctx context.Context) error {
	if o == nil {
		return errors.New("catalog observer is nil")
	}
	if ctx == nil {
		return errors.New("catalog observer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return observerStopError(err)
	}
	o.runMu.Lock()
	if o.running {
		o.runMu.Unlock()
		return ErrObserverAlreadyRun
	}
	o.running = true
	o.runMu.Unlock()
	defer func() {
		o.runMu.Lock()
		o.running = false
		o.runMu.Unlock()
	}()

	for {
		if _, err := o.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Debug("catalog remains unavailable", "error", err)
		}
		o.mu.Lock()
		o.expireLocked(o.now())
		next := o.state.nextCheck
		if o.inFlight != nil {
			next = o.now().Add(o.pollInterval)
		}
		if o.state.ready {
			expiry := o.state.lastSuccess.Add(o.pollInterval + o.staleGrace)
			if expiry.Before(next) {
				next = expiry
			}
		}
		call := o.inFlight
		o.mu.Unlock()
		delay := max(next.Sub(o.now()), time.Millisecond)
		timer := time.NewTimer(delay)
		var done <-chan struct{}
		if call != nil {
			done = call.done
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return observerStopError(ctx.Err())
		case <-done:
		case <-o.wake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (o *Observer) expireLocked(now time.Time) {
	if o.state.ready && !now.Before(o.state.lastSuccess.Add(o.pollInterval+o.staleGrace)) {
		o.fenceLocked()
		o.state.lastError = errors.New("catalog validation expired")
		slog.Warn("catalog validation grace expired", "last_success", o.state.lastSuccess)
	}
}

func (o *Observer) State() State {
	if o == nil {
		return State{Err: errors.New("catalog observer is nil")}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expireLocked(o.now())
	return State{
		LastSuccess: o.state.lastSuccess, RefreshSuccesses: o.state.successes, RefreshFailures: o.state.failures,
		Ready:      o.state.ready,
		Generation: o.state.generation,
		Snapshot:   cloneSnapshot(o.state.snapshot),
		Err:        o.state.lastError,
	}
}

func (o *Observer) Ready() bool {
	return o != nil && o.State().Ready
}

func (o *Observer) Current() (Generation, error) {
	if o == nil {
		return Generation{}, errors.New("catalog observer is nil")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expireLocked(o.now())
	if !o.state.ready {
		if o.state.lastError != nil {
			return Generation{}, fmt.Errorf("%w: %w", ErrCatalogUnavailable, o.state.lastError)
		}
		return Generation{}, ErrCatalogNotReady
	}
	return Generation{
		Number:   o.state.generation,
		Snapshot: cloneSnapshot(o.state.snapshot),
		Context:  o.state.activeContext,
	}, nil
}

// WithCurrent holds the publication lock through publish; the callback must not call observer methods.
func (o *Observer) WithCurrent(publish func(Generation)) error {
	if o == nil || publish == nil {
		return errors.New("catalog publication requires observer and callback")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expireLocked(o.now())
	if !o.state.ready {
		return fmt.Errorf("%w: catalog generation is not ready", ErrCatalogUnavailable)
	}
	publish(Generation{Number: o.state.generation, Snapshot: cloneSnapshot(o.state.snapshot), Context: o.state.activeContext})
	return nil
}

func (o *Observer) runCheck(ctx context.Context, call *observationCall, cancel context.CancelFunc) {
	defer cancel()
	var (
		snapshot Snapshot
		err      error
	)
	if checker, ok := o.checker.(ObservedChecker); ok {
		snapshot, err = checker.RefreshObserved(ctx, o.observe)
	} else {
		snapshot, err = o.checker.Refresh(ctx)
		if err == nil {
			o.observe(snapshotObservation(snapshot))
		}
	}
	o.finishCheck(call, snapshot, err)
}

func (o *Observer) observe(observation Observation) {
	if observation.Release == "" {
		o.mu.Lock()
		o.fenceLocked()
		o.state.lastError = errors.New("catalog checker reported an empty release")
		o.mu.Unlock()
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state.ready {
		if observationChanged(o.state.identity, observation) {
			o.fenceLocked()
			o.state.pending = observation
		}
		return
	}
	o.state.pending = observation
}

func (o *Observer) finishCheck(call *observationCall, snapshot Snapshot, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err == nil {
		observation := snapshotObservation(snapshot)
		if err = o.validateObservationLocked(observation); err == nil {
			o.acceptObservationLocked(snapshot, observation)
		}
	}
	now := o.now()
	if err != nil {
		o.state.failures++
		o.state.consecutiveFailures++
		delay := o.retryDelayLocked()
		o.state.nextCheck = now.Add(delay)
		var upstreamErr *UpstreamError
		if errors.As(err, &upstreamErr) && upstreamErr.RetryAt.After(o.state.nextCheck) {
			o.state.nextCheck = upstreamErr.RetryAt
		}
		o.expireLocked(now)
		if !isTransientError(err) {
			o.fenceLocked()
		}
		o.state.lastError = err
		slog.Warn("catalog validation failed", "error", err, "next_check", o.state.nextCheck,
			"serving_grace", o.state.ready, "catalog_version", o.state.snapshot.CatalogVersion)
	} else {
		o.state.successes++
		o.state.consecutiveFailures = 0
		o.state.lastSuccess = now
		o.state.nextCheck = now.Add(o.pollInterval)
		slog.Debug("catalog validation succeeded", "catalog_version", snapshot.CatalogVersion, "next_check", o.state.nextCheck)
	}

	call.snapshot = cloneSnapshot(snapshot)
	call.err = err
	if o.inFlight == call {
		o.inFlight = nil
	}
	close(call.done)
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Observer) retryDelayLocked() time.Duration {
	delay := o.retryBase
	for attempt := 1; attempt < o.state.consecutiveFailures && delay < catalogRetryMax; attempt++ {
		delay = min(delay*2, catalogRetryMax)
	}
	return min(delay+time.Duration(1+rand.Int64N(max(int64(delay/catalogRetryJitterDivisor), 1))), catalogRetryMax)
}

func (o *Observer) validateObservationLocked(observation Observation) error {
	switch {
	case observation.Release == "" || observation.CatalogVersion == "":
		return errors.New("catalog checker returned an incomplete snapshot")
	case o.state.pending.Release != "" && observationChanged(o.state.pending, observation):
		return errors.New("catalog checker observation does not match snapshot")
	default:
		return nil
	}
}

func (o *Observer) acceptObservationLocked(snapshot Snapshot, observation Observation) {
	if o.state.ready && observationChanged(o.state.identity, observation) {
		o.fenceLocked()
	}
	if !o.state.ready {
		if o.state.generation == 0 {
			o.state.generation = 1
		}
		generationContext, cancel := context.WithCancel(context.Background())
		o.state.activeContext = generationContext
		o.state.cancelActive = cancel
		o.state.ready = true
	}
	o.state.snapshot = cloneSnapshot(snapshot)
	o.state.identity = observation
	o.state.pending = Observation{}
	o.state.lastError = nil
}

func (o *Observer) fenceLocked() {
	if o.state.ready || o.state.cancelActive != nil || o.state.snapshot.Release != "" {
		o.state.generation++
	}
	if o.state.cancelActive != nil {
		o.state.cancelActive()
	}
	o.state.ready = false
	o.state.snapshot = Snapshot{}
	o.state.identity = Observation{}
	o.state.activeContext = nil
	o.state.cancelActive = nil
}

func detachedCheckContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(context.Background(), deadline)
	}
	return context.WithCancel(context.Background())
}

func observerStopError(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("catalog observer stopped: %w", err)
}

func snapshotObservation(snapshot Snapshot) Observation {
	return Observation{Release: snapshot.Release, CatalogVersion: snapshot.CatalogVersion}
}

func observationChanged(current, next Observation) bool {
	if current.Release != "" && next.Release != current.Release {
		return true
	}
	return current.CatalogVersion != "" && next.CatalogVersion != "" && current.CatalogVersion != next.CatalogVersion
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	clone := snapshot
	if snapshot.Manifest != nil {
		clone.Manifest = append([]Asset(nil), snapshot.Manifest...)
	}
	if snapshot.Schema.Columns != nil {
		clone.Schema.Columns = append([]Column(nil), snapshot.Schema.Columns...)
	}
	return clone
}
