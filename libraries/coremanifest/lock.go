package coremanifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const LockName = "writer.lock"

var ErrLocked = errors.New("another writer holds the lock")

// WriterLock is held for the life of the writing process; the kernel releases it when the process dies.
type WriterLock struct {
	path string
	file *os.File
}

func LockPath(root string) string { return filepath.Join(root, LockName) }

func AcquireWriterLock(root string) (*WriterLock, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	path := LockPath(root)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		if errors.Is(err, ErrLocked) {
			return nil, fmt.Errorf("%s: %w", path, ErrLocked)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return &WriterLock{path: path, file: f}, nil
}

func (l *WriterLock) Path() string { return l.path }

func (l *WriterLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// WriterLockHeld reports whether some process holds root's writer lock, without taking it.
func WriterLockHeld(root string) (bool, error) {
	f, err := os.Open(LockPath(root))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	return probeFile(f)
}
