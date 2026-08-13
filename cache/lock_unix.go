//go:build android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd

package cache

import (
	"errors"
	"os"
	"syscall"
)

func acquireDirectoryLock(file *os.File) error {
	err := retryEINTR(func() error {
		return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	})
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return errDirectoryLockHeld
	}
	return err
}

func releaseDirectoryLock(file *os.File) error {
	return retryEINTR(func() error {
		return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	})
}

func retryEINTR(operation func() error) error {
	for {
		err := operation()
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
