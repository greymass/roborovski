package internal

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type WAL struct {
	path string
	file *os.File
	mu   sync.Mutex
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	return &WAL{
		path: path,
		file: f,
	}, nil
}

func (w *WAL) Append(prefix5 [5]byte, blockNum uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := &WALEntry{
		Prefix5:  prefix5,
		BlockNum: blockNum,
	}
	_, err := w.file.Write(entry.Encode())
	return err
}

func (w *WAL) AppendBatch(entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	buf := make([]byte, len(entries)*WALEntrySize)
	for i, entry := range entries {
		copy(buf[i*WALEntrySize:], entry.Encode())
	}
	_, err := w.file.Write(buf)
	return err
}

func (w *WAL) ReadAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seek WAL: %w", err)
	}

	info, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat WAL: %w", err)
	}

	if info.Size() == 0 {
		return nil, nil
	}

	if info.Size()%WALEntrySize != 0 {
		return nil, fmt.Errorf("WAL size %d not aligned to entry size %d", info.Size(), WALEntrySize)
	}

	entryCount := int(info.Size()) / WALEntrySize
	entries := make([]WALEntry, 0, entryCount)

	buf := make([]byte, WALEntrySize)
	for {
		n, err := w.file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read WAL: %w", err)
		}
		if n != WALEntrySize {
			return nil, fmt.Errorf("short read: %d bytes", n)
		}

		entry, err := DecodeWALEntry(buf)
		if err != nil {
			return nil, fmt.Errorf("decode WAL entry: %w", err)
		}
		entries = append(entries, *entry)
	}

	return entries, nil
}

func (w *WAL) EntryCount() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()) / WALEntrySize, nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
