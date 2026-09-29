//go:build linux || darwin

package cache

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func acquireCacheLock(file *os.File) error {
	if file == nil {
		return errors.New("cache lock file is nil")
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ErrCacheInUse
		}
		return err
	}
	return nil
}

func releaseCacheLock(file *os.File) error {
	if file == nil {
		return errors.New("cache lock file is nil")
	}
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
