package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

type Observer struct {
	checker      Checker
	pollInterval time.Duration

	mu       sync.Mutex
	runMu    sync.Mutex
	running  bool
	inFlight *observationCall
	state    observerState
}

type observerState struct {
	ready         bool
	generation    uint64
	snapshot      Snapshot
	identity      Observation
	pending       Observation
	lastError     error
	activeContext context.Context
	cancelActive  context.CancelFunc
}

type observationCall struct {
	done     chan struct{}
	snapshot Snapshot
	err      error
}

type State struct {
	Ready      bool
	Generation uint64
	Snapshot   Snapshot
	Err        error
}

type Generation struct {
	Number   uint64
	Snapshot Snapshot
	Context  context.Context
}

func NewObserver(checker Checker, pollInterval time.Duration) (*Observer, error) {
	if checker == nil {
		return nil, fmt.Errorf("catalog checker is nil")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("catalog poll interval must be positive")
	}
	return &Observer{checker: checker, pollInterval: pollInterval}, nil
}

func (o *Observer) Refresh(ctx context.Context) (Snapshot, error) {
	if o == nil {
		return Snapshot{}, fmt.Errorf("catalog observer is nil")
	}
	if ctx == nil {
		return Snapshot{}, fmt.Errorf("catalog observer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("catalog observer canceled before start: %w", err)
	}

	o.mu.Lock()
	call := o.inFlight
	if call == nil {
		call = &observationCall{done: make(chan struct{})}
		o.inFlight = call
		o.state.pending = Observation{}
		checkContext, cancel := detachedCheckContext(ctx)
		o.mu.Unlock()
		go o.runCheck(call, checkContext, cancel)
	} else {
		o.mu.Unlock()
	}

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
		return fmt.Errorf("catalog observer is nil")
	}
	if ctx == nil {
		return fmt.Errorf("catalog observer context is nil")
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

	if _, err := o.Refresh(ctx); err != nil && ctx.Err() == nil {
		slog.Error("initial catalog check failed", "error", err)
	}
	if err := ctx.Err(); err != nil {
		return observerStopError(err)
	}
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return observerStopError(ctx.Err())
		case <-ticker.C:
			if _, err := o.Refresh(ctx); err != nil && ctx.Err() == nil {
				slog.Error("idle catalog check failed", "error", err)
			}
		}
	}
}

func (o *Observer) State() State {
	if o == nil {
		return State{Err: fmt.Errorf("catalog observer is nil")}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return State{
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
		return Generation{}, fmt.Errorf("catalog observer is nil")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.state.ready {
		if o.state.lastError != nil {
			return Generation{}, fmt.Errorf("%w: %v", ErrCatalogUnavailable, o.state.lastError)
		}
		return Generation{}, ErrCatalogNotReady
	}
	return Generation{
		Number:   o.state.generation,
		Snapshot: cloneSnapshot(o.state.snapshot),
		Context:  o.state.activeContext,
	}, nil
}

func (o *Observer) runCheck(call *observationCall, ctx context.Context, cancel context.CancelFunc) {
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
		o.state.lastError = fmt.Errorf("catalog checker reported an empty release")
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
		if observation.Release == "" || observation.CatalogVersion == "" {
			err = fmt.Errorf("catalog checker returned an incomplete snapshot")
		}
		if err == nil {
			if o.state.pending.Release != "" && observationChanged(o.state.pending, observation) {
				err = fmt.Errorf("catalog checker observation does not match snapshot")
			}
		}
		if err == nil {
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
				o.state.snapshot = cloneSnapshot(snapshot)
				o.state.identity = observation
				o.state.ready = true
			} else {
				o.state.snapshot = cloneSnapshot(snapshot)
				o.state.identity = observation
			}
			o.state.pending = Observation{}
			o.state.lastError = nil
		}
	}
	if err != nil {
		o.fenceLocked()
		o.state.lastError = err
	}
	call.snapshot = cloneSnapshot(snapshot)
	call.err = err
	if o.inFlight == call {
		o.inFlight = nil
	}
	close(call.done)
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
