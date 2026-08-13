//go:build !android && !darwin && !dragonfly && !freebsd && !illumos && !ios && !linux && !netbsd && !openbsd && !windows

package cache

import (
	"fmt"
	"os"
	"runtime"
)

func acquireDirectoryLock(*os.File) error {
	return fmt.Errorf("file locking is unsupported on %s", runtime.GOOS)
}

func releaseDirectoryLock(*os.File) error {
	return nil
}
