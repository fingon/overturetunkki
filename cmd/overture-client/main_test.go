package main

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestRunValidatesClientConfiguration(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "invalid URL", args: []string{"--server-url=ftp://example.com", "catalog"}},
		{name: "invalid timeout", args: []string{"--timeout=0s", "catalog"}},
		{name: "command required", args: []string{"--server-url=http://localhost:8080"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			assert.Assert(t, run(context.Background(), test.args) != nil)
		})
	}
}

func TestRunUsesCommandDeadline(t *testing.T) {
	err := run(context.Background(), []string{"--timeout=1ns", "catalog"})
	assert.ErrorContains(t, err, "deadline exceeded")
}

func TestRunUsesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"--timeout=1h", "catalog"})
	assert.ErrorContains(t, err, "canceled")

	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	err = run(ctx, []string{"--timeout=1h", "catalog"})
	assert.ErrorContains(t, err, "deadline exceeded")
}
