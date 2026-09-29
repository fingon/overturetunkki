package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrScratchClosed             = errors.New("scratch pool is closed")
	ErrScratchCapacity           = errors.New("scratch capacity is unavailable")
	ErrInvalidScratchReservation = errors.New("scratch reservation is invalid")
)

type ScratchPool struct {
	maxBytes int64

	mu              sync.Mutex
	closed          bool
	reservedBytes   int64
	capacityChanged chan struct{}
}

type ScratchReservation struct {
	pool     *ScratchPool
	bytes    int64
	released bool
}

type ScratchStats struct {
	MaxBytes      int64
	ReservedBytes int64
}

func NewScratchPool(maxBytes int64) (*ScratchPool, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("scratch max bytes must be positive, got %d", maxBytes)
	}
	return &ScratchPool{maxBytes: maxBytes, capacityChanged: make(chan struct{})}, nil
}

func (pool *ScratchPool) Reserve(ctx context.Context, bytes int64) (*ScratchReservation, error) {
	if pool == nil {
		return nil, errors.New("reserve scratch: pool is nil")
	}
	if ctx == nil {
		return nil, errors.New("reserve scratch: context is nil")
	}
	if bytes <= 0 {
		return nil, fmt.Errorf("reserve scratch: bytes must be positive, got %d", bytes)
	}
	if bytes > pool.maxBytes {
		return nil, fmt.Errorf("%w: request is %d bytes, pool limit is %d bytes", ErrScratchCapacity, bytes, pool.maxBytes)
	}
	for {
		pool.mu.Lock()
		if pool.closed {
			pool.mu.Unlock()
			return nil, ErrScratchClosed
		}
		if pool.reservedBytes+bytes <= pool.maxBytes {
			pool.reservedBytes += bytes
			pool.mu.Unlock()
			return &ScratchReservation{pool: pool, bytes: bytes}, nil
		}
		changed := pool.capacityChanged
		pool.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("reserve scratch: %w", ctx.Err())
		case <-changed:
		}
	}
}

func (reservation *ScratchReservation) Release() error {
	if reservation == nil || reservation.pool == nil {
		return fmt.Errorf("release scratch: %w", ErrInvalidScratchReservation)
	}
	pool := reservation.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if reservation.released {
		return nil
	}
	pool.reservedBytes -= reservation.bytes
	reservation.released = true
	close(pool.capacityChanged)
	pool.capacityChanged = make(chan struct{})
	return nil
}

func (pool *ScratchPool) Stats() ScratchStats {
	if pool == nil {
		return ScratchStats{}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return ScratchStats{MaxBytes: pool.maxBytes, ReservedBytes: pool.reservedBytes}
}

func (pool *ScratchPool) Close() error {
	if pool == nil {
		return errors.New("close scratch: pool is nil")
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		return nil
	}
	if pool.reservedBytes != 0 {
		return fmt.Errorf("close scratch: %d bytes remain reserved", pool.reservedBytes)
	}
	pool.closed = true
	close(pool.capacityChanged)
	return nil
}
