package coremanifest

import (
	"errors"
	"os"
	"syscall"
)

const (
	fOFDGetLk = 36
	fOFDSetLk = 37
)

func wholeFile(t int16) *syscall.Flock_t {
	return &syscall.Flock_t{Type: t, Whence: 0, Start: 0, Len: 0}
}

// Open file description locks: owned by the open file, so separate opens conflict even within one process.
func lockFile(f *os.File) error {
	err := syscall.FcntlFlock(f.Fd(), fOFDSetLk, wholeFile(syscall.F_WRLCK))
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EACCES) {
		return ErrLocked
	}
	return err
}

func probeFile(f *os.File) (bool, error) {
	lk := wholeFile(syscall.F_WRLCK)
	if err := syscall.FcntlFlock(f.Fd(), fOFDGetLk, lk); err != nil {
		return false, err
	}
	return lk.Type != syscall.F_UNLCK, nil
}
