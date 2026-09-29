package main

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestRunUsesConfiguredMode(t *testing.T) {
	cases := []string{"supervisor", "worker"}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assert.NilError(t, run(ctx, []string{
				"--mode=" + mode,
				"--cache-dir=" + t.TempDir(),
			}))
		})
	}
}
