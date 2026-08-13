//go:build android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd

package cache

import (
	"errors"
	"os"
	"syscall"
)

func acquireDirectoryLock(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errDirectoryLockHeld
	}
	return err
}

func releaseDirectoryLock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
