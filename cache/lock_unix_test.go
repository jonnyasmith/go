//go:build android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd

package cache

import (
	"errors"
	"syscall"
	"testing"
)

func TestRetryEINTR(t *testing.T) {
	terminal := errors.New("terminal")
	calls := 0
	err := retryEINTR(func() error {
		calls++
		if calls < 3 {
			return syscall.EINTR
		}
		return terminal
	})
	if !errors.Is(err, terminal) || calls != 3 {
		t.Fatalf("retry result = %v after %d calls; want terminal after 3", err, calls)
	}
}
