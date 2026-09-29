package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestScratchPoolBoundsAndCancellation(t *testing.T) {
	pool, err := NewScratchPool(10)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	first, err := pool.Reserve(context.Background(), 7)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.DeepEqual(t, pool.Stats(), ScratchStats{MaxBytes: 10, ReservedBytes: 7})
	tooLarge, err := pool.Reserve(context.Background(), 11)
	assert.Assert(t, tooLarge == nil)
	assert.Assert(t, errors.Is(err, ErrScratchCapacity))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = pool.Reserve(ctx, 4)
	assert.Assert(t, errors.Is(err, context.DeadlineExceeded))
	assert.NilError(t, first.Release())
	assert.NilError(t, first.Release())

	second, err := pool.Reserve(context.Background(), 10)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, pool.Close() != nil)
	assert.NilError(t, second.Release())
	assert.NilError(t, pool.Close())
	_, err = pool.Reserve(context.Background(), 1)
	assert.Assert(t, errors.Is(err, ErrScratchClosed))
}

func TestScratchPoolRejectsInvalidOptions(t *testing.T) {
	for _, maxBytes := range []int64{0, -1} {
		_, err := NewScratchPool(maxBytes)
		assert.ErrorContains(t, err, "positive")
	}
	var reservation *ScratchReservation
	assert.Assert(t, reservation.Release() != nil)
}
