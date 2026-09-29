//go:build !linux

package worker

import "fmt"

func setFileSizeLimit(maxBytes int64) error {
	return fmt.Errorf("RLIMIT_FSIZE is unsupported on this platform (requested %d bytes)", maxBytes)
}
