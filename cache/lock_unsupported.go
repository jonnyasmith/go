//go:build !aix && !android && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package cache

import (
	"fmt"
	"os"
	"runtime"
)

func lockFile(*os.File) error {
	return fmt.Errorf("file locking is unsupported on %s", runtime.GOOS)
}

func unlockFile(*os.File) error {
	return nil
}
