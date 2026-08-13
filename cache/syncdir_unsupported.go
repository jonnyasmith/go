//go:build !android && !darwin && !dragonfly && !freebsd && !illumos && !ios && !linux && !netbsd && !openbsd && !windows

package cache

import (
	"fmt"
	"runtime"
)

func syncDirectory(string) error {
	return fmt.Errorf("directory synchronization is unsupported on %s", runtime.GOOS)
}
