package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type JobFunc func(context.Context, *Reservation) error

type SchedulerOptions struct {
	Workers       int
	QueueCapacity int
}

type Scheduler struct {
	cache *Cache
	queue chan *job
	wait  sync.WaitGroup

	mu     sync.Mutex
	closed bool
	jobs   map[string]*job
}

type job struct {
	key      string
	maxBytes int64
	build    JobFunc
	context  context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	waiters  int
	started  bool
	err      error
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
	scheduler := &Scheduler{
		cache: cache,
		queue: make(chan *job, options.QueueCapacity),
		jobs:  make(map[string]*job),
	}
	for range options.Workers {
		scheduler.wait.Add(1)
		go scheduler.worker()
	}
	return scheduler, nil
}

func (scheduler *Scheduler) Do(ctx context.Context, key Key, maxBytes int64, build JobFunc) error {
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
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return ErrSchedulerClosed
	}
	current := scheduler.jobs[keyDigest]
	if current == nil {
		jobContext, cancel := context.WithCancel(context.Background())
		current = &job{
			key:      keyDigest,
			maxBytes: maxBytes,
			build:    build,
			context:  jobContext,
			cancel:   cancel,
			done:     make(chan struct{}),
			waiters:  1,
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
	if current.waiters == 0 {
		current.cancel()
		if !current.started && scheduler.jobs[current.key] == current {
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

		reservation, err := scheduler.cache.Reserve(current.context, current.maxBytes)
		if err == nil && current.context.Err() == nil {
			err = current.build(current.context, reservation)
		}
		if reservation != nil {
			releaseErr := reservation.Release()
			if err == nil && releaseErr != nil {
				err = releaseErr
			}
		}
		scheduler.mu.Lock()
		current.err = err
		if scheduler.jobs[current.key] == current {
			delete(scheduler.jobs, current.key)
		}
		close(current.done)
		scheduler.mu.Unlock()
	}
}
