//go:build android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd

package cache

import (
	"fmt"
	"os"
)

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cache: open directory %q for sync: %w", dir, err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("cache: sync directory %q: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("cache: close directory %q after sync: %w", dir, closeErr)
	}
	return nil
}
