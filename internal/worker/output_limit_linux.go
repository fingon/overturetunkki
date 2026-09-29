//go:build linux

package worker

import "golang.org/x/sys/unix"

var setFileSizeLimit = func(maxBytes int64) error {
	limit := &unix.Rlimit{
		Cur: uint64(maxBytes),
		Max: uint64(maxBytes),
	}
	return unix.Setrlimit(unix.RLIMIT_FSIZE, limit)
}
