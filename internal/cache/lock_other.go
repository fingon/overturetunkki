//go:build !linux && !darwin

package cache

import (
	"fmt"
	"os"
)

func acquireCacheLock(file *os.File) error {
	if file == nil {
		return fmt.Errorf("cache lock file is nil")
	}
	return fmt.Errorf("exclusive cache locks are unsupported on this platform")
}

func releaseCacheLock(file *os.File) error {
	if file == nil {
		return fmt.Errorf("cache lock file is nil")
	}
	return nil
}
