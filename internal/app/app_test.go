package app

import (
	"context"
	"testing"
	"time"

	"github.com/mstenber/overturetunkki/internal/config"
	"gotest.tools/v3/assert"
)

func TestRunModesStopOnCancellation(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cfg.CacheDir = t.TempDir()
	cases := []config.Mode{config.ModeSupervisor, config.ModeWorker}
	for _, mode := range cases {
		t.Run(string(mode), func(t *testing.T) {
			modeConfig := cfg
			modeConfig.Mode = mode
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assert.NilError(t, Run(ctx, modeConfig))
		})
	}
}

func TestRunDeadlineIsReturned(t *testing.T) {
	cfg, err := config.Parse(nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cfg.CacheDir = t.TempDir()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	assert.ErrorContains(t, Run(ctx, cfg), "deadline exceeded")
}
