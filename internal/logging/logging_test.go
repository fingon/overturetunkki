package logging

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"gotest.tools/v3/assert"
)

func TestNewLogLevel(t *testing.T) {
	cases := []struct {
		name        string
		verbose     bool
		wantDebug   bool
		wantInfo    bool
	}{
		{name: "default", verbose: false, wantDebug: false, wantInfo: true},
		{name: "verbose", verbose: true, wantDebug: true, wantInfo: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			logger := New(test.verbose, &bytes.Buffer{})
			assert.Equal(t, logger.Enabled(context.Background(), slog.LevelDebug), test.wantDebug)
			assert.Equal(t, logger.Enabled(context.Background(), slog.LevelInfo), test.wantInfo)
		})
	}
}

func TestConfigureSetsDefaultLogger(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	Configure(true)
	assert.Assert(t, slog.Default().Enabled(context.Background(), slog.LevelDebug))
}
