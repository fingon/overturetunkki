package main

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestRunStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NilError(t, run(ctx, []string{"--cache-dir=" + t.TempDir()}))
}
