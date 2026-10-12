package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type JobFunc func(context.Context, *Reservation) error

type SchedulerOptions struct {
	Workers       int
	QueueCapacity int
	Timeout       time.Duration
}

type Scheduler struct {
	cache   *Cache
	queue   chan *job
	wait    sync.WaitGroup
	timeout time.Duration

	mu     sync.Mutex
	closed bool
	jobs   map[string]*job
}

type job struct {
	transient bool
	key       string
	tileKey   Key
	maxBytes  int64
	build     JobFunc
	context   context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	waiters   int
	started   bool
	err       error
}

func NewScheduler(cache *Cache, options SchedulerOptions) (*Scheduler, error) {
	if cache == nil {
		return nil, errors.New("create cache scheduler: cache is nil")
	}
	if options.Workers <= 0 {
		return nil, fmt.Errorf("create cache scheduler: workers must be positive, got %d", options.Workers)
	}
	if options.QueueCapacity <= 0 {
		return nil, fmt.Errorf("create cache scheduler: queue capacity must be positive, got %d", options.QueueCapacity)
	}
	if options.Timeout < 0 {
		return nil, errors.New("scheduler timeout must not be negative")
	}
	scheduler := &Scheduler{
		timeout: options.Timeout,
		cache:   cache,
		queue:   make(chan *job, options.QueueCapacity),
		jobs:    make(map[string]*job),
	}
	for range options.Workers {
		scheduler.wait.Add(1)
		go scheduler.worker()
	}
	return scheduler, nil
}

func (scheduler *Scheduler) Do(ctx context.Context, key Key, maxBytes int64, build JobFunc) error {
	return scheduler.do(ctx, key, build, jobOptions{maxBytes: maxBytes})
}

func (scheduler *Scheduler) DoTransient(ctx context.Context, key Key, build func(context.Context) error) error {
	if build == nil {
		return errors.New("run transient job: build function is nil")
	}
	return scheduler.do(ctx, key, func(ctx context.Context, _ *Reservation) error {
		return build(ctx)
	}, jobOptions{transient: true})
}

type jobOptions struct {
	maxBytes  int64
	transient bool
}

func (scheduler *Scheduler) do(ctx context.Context, key Key, build JobFunc, options jobOptions) error {
	if scheduler == nil {
		return errors.New("run cache job: scheduler is nil")
	}
	if ctx == nil {
		return errors.New("run cache job: context is nil")
	}
	if build == nil {
		return errors.New("run cache job: build function is nil")
	}
	keyDigest, err := key.digest()
	if err != nil {
		return fmt.Errorf("run cache job: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("run cache job: %w", err)
	}
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return ErrSchedulerClosed
	}
	current := scheduler.jobs[keyDigest]
	if current == nil {
		jobContext, cancel := scheduler.jobContext()
		current = &job{
			transient: options.transient,
			key:       keyDigest,
			tileKey:   key,
			maxBytes:  options.maxBytes,
			build:     build,
			context:   jobContext,
			cancel:    cancel,
			done:      make(chan struct{}),
			waiters:   1,
		}
		select {
		case scheduler.queue <- current:
			scheduler.jobs[keyDigest] = current
		default:
			cancel()
			scheduler.mu.Unlock()
			return ErrQueueFull
		}
	} else {
		current.waiters++
	}
	scheduler.mu.Unlock()
	return scheduler.waitForCaller(ctx, current)
}

func (scheduler *Scheduler) jobContext() (context.Context, context.CancelFunc) {
	if scheduler.timeout > 0 {
		return context.WithTimeout(context.Background(), scheduler.timeout)
	}
	return context.WithCancel(context.Background())
}

func (scheduler *Scheduler) Close() error {
	if scheduler == nil {
		return errors.New("close cache scheduler: scheduler is nil")
	}
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return nil
	}
	scheduler.closed = true
	for _, current := range scheduler.jobs {
		current.cancel()
	}
	close(scheduler.queue)
	scheduler.mu.Unlock()
	scheduler.wait.Wait()
	return nil
}

func (scheduler *Scheduler) waitForCaller(ctx context.Context, current *job) error {
	select {
	case <-current.done:
		return current.err
	case <-ctx.Done():
		if current.transient {
			current.cancel()
			<-current.done
			return errors.Join(fmt.Errorf("transient job canceled: %w", ctx.Err()), current.err)
		}
		scheduler.detach(current)
		return fmt.Errorf("cache job caller detached: %w", ctx.Err())
	}
}

func (scheduler *Scheduler) detach(current *job) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if current.waiters > 0 {
		current.waiters--
	}
	if current.waiters == 0 && !current.started {
		current.cancel()
		if scheduler.jobs[current.key] == current {
			delete(scheduler.jobs, current.key)
		}
	}
}

func (scheduler *Scheduler) worker() {
	defer scheduler.wait.Done()
	for current := range scheduler.queue {
		scheduler.mu.Lock()
		if scheduler.jobs[current.key] != current {
			scheduler.mu.Unlock()
			current.err = context.Canceled
			close(current.done)
			continue
		}
		current.started = true
		scheduler.mu.Unlock()

		var reservation *Reservation
		err := current.context.Err()
		if err == nil && !current.transient {
			reservation, err = scheduler.cache.ReserveTile(current.context, current.tileKey, current.maxBytes)
		}
		if err == nil {
			err = current.context.Err()
			if err == nil {
				err = current.build(current.context, reservation)
			}
		}
		if reservation != nil {
			if releaseErr := reservation.Release(); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release cache job reservation: %w", releaseErr))
			}
		}
		current.cancel()
		scheduler.mu.Lock()
		abandoned := current.waiters == 0
		current.err = err
		if scheduler.jobs[current.key] == current {
			delete(scheduler.jobs, current.key)
		}
		close(current.done)
		scheduler.mu.Unlock()
		if abandoned && err != nil {
			slog.Error("detached cache job failed", "tile", current.tileKey.Tile, "catalog_version", current.tileKey.CatalogVersion, "projection_id", current.tileKey.ProjectionID, "size_policy_id", current.tileKey.SizePolicyID, "error", err)
		}
	}
}
