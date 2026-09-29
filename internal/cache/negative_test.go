package cache

import (
	"errors"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestNegativeCacheStoresOnlyProvenRejectionsAndEvictsLRU(t *testing.T) {
	negative, err := NewNegativeCache(2, time.Minute)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	firstKey := testKey()
	secondKey := firstKey
	secondKey.Cell = "8928308280ffffe"
	thirdKey := firstKey
	thirdKey.Cell = "8928308280ffffd"
	first, err := NewSizeRejection(firstKey, RejectionLimitRows, 11, 10)
	assert.NilError(t, err)
	second, err := NewSizeRejection(secondKey, RejectionLimitBytes, 101, 100)
	assert.NilError(t, err)
	third, err := NewSizeRejection(thirdKey, RejectionLimitRows, 11, 10)
	assert.NilError(t, err)
	assert.NilError(t, negative.Put(first))
	assert.NilError(t, negative.Put(second))
	_, ok, err := negative.Get(firstKey)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.NilError(t, negative.Put(third))
	_, ok, err = negative.Get(secondKey)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
	got, ok, err := negative.Get(firstKey)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Equal(t, got.Limit(), RejectionLimitRows)
	assert.Equal(t, got.Actual(), int64(11))
	assert.Equal(t, got.Threshold(), int64(10))

	unproven := SizeRejection{key: thirdKey, limit: RejectionLimitRows, actual: 2, threshold: 1}
	assert.Assert(t, negative.Put(unproven) != nil)
	assert.NilError(t, negative.DeleteCatalogVersion(thirdKey.CatalogVersion))
	_, ok, err = negative.Get(thirdKey)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
	assert.NilError(t, negative.Close())
	_, _, err = negative.Get(firstKey)
	assert.Assert(t, errors.Is(err, ErrNegativeCacheClosed))
}

func TestNegativeCacheExpiresAndValidatesRejections(t *testing.T) {
	negative, err := NewNegativeCache(1, 20*time.Millisecond)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	key := testKey()
	cases := []struct {
		name      string
		limit     RejectionLimit
		actual    int64
		threshold int64
	}{
		{name: "unknown limit", limit: "other", actual: 2, threshold: 1},
		{name: "zero actual", limit: RejectionLimitRows, actual: 0, threshold: 1},
		{name: "not over threshold", limit: RejectionLimitRows, actual: 1, threshold: 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewSizeRejection(key, test.limit, test.actual, test.threshold)
			assert.Assert(t, err != nil)
		})
	}
	rejection, err := NewSizeRejection(key, RejectionLimitBytes, 2, 1)
	assert.NilError(t, err)
	_, err = NewSizeRejection(key, RejectionLimitRows, 1, 0)
	assert.NilError(t, err)
	assert.NilError(t, negative.Put(rejection))
	time.Sleep(30 * time.Millisecond)
	_, ok, err := negative.Get(key)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}
