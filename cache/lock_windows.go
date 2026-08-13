package cache

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	lockFileFailImmediately = 0x00000001
	lockFileExclusiveLock   = 0x00000002
)

var (
	kernel32DLL      = syscall.NewLazyDLL("kernel32.dll")
	lockFileExProc   = kernel32DLL.NewProc("LockFileEx")
	unlockFileExProc = kernel32DLL.NewProc("UnlockFileEx")
)

func lockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	result, _, err := lockFileExProc.Call(
		file.Fd(),
		lockFileFailImmediately|lockFileExclusiveLock,
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		return err
	}
	return nil
}

func unlockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	result, _, err := unlockFileExProc.Call(
		file.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		return err
	}
	return nil
}
