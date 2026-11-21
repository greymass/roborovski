package abicache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/greymass/roborovski/libraries/coremanifest"
)

const (
	dataHeaderSize = 16
	indexEntrySize = 20
)

// SyncBounds makes both files durable and returns their committed extent.
func (w *Writer) SyncBounds() (dataEnd uint64, indexLen uint32, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.dataFile.Sync(); err != nil {
		return 0, 0, fmt.Errorf("failed to sync data file: %w", err)
	}
	if err := w.indexFile.Sync(); err != nil {
		return 0, 0, fmt.Errorf("failed to sync index file: %w", err)
	}
	ds, err := w.dataFile.Stat()
	if err != nil {
		return 0, 0, err
	}
	is, err := w.indexFile.Stat()
	if err != nil {
		return 0, 0, err
	}
	if is.Size()%indexEntrySize != 0 {
		return 0, 0, fmt.Errorf("abis.index holds %d bytes, not a whole number of entries", is.Size())
	}
	return uint64(ds.Size()), uint32(is.Size() / indexEntrySize), nil
}

func fileSize(path string) (int64, error) {
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

// TruncateTo cuts the cache to its committed extent and refuses when the files hold less than that.
func TruncateTo(basePath string, dataEnd uint64, indexLen uint32) (cut bool, err error) {
	dataPath := filepath.Join(basePath, "abis.dat")
	indexPath := filepath.Join(basePath, "abis.index")
	ds, err := fileSize(dataPath)
	if err != nil {
		return false, err
	}
	is, err := fileSize(indexPath)
	if err != nil {
		return false, err
	}
	wantIndex := int64(indexLen) * indexEntrySize
	if ds < int64(dataEnd) || is < wantIndex {
		return false, fmt.Errorf("ABI cache holds %d data and %d index bytes, manifest commits %d and %d", ds, is, dataEnd, wantIndex)
	}
	if ds > int64(dataEnd) {
		if err := truncateFile(dataPath, int64(dataEnd)); err != nil {
			return false, err
		}
		cut = true
	}
	if is > wantIndex {
		if err := truncateFile(indexPath, wantIndex); err != nil {
			return false, err
		}
		cut = true
	}
	return cut, nil
}

// CommittedBounds finds the extent of the cache that covers blocks up to head, for a cache adopted without a manifest.
func CommittedBounds(basePath string, head uint32) (dataEnd uint64, indexLen uint32, err error) {
	index, err := os.ReadFile(filepath.Join(basePath, "abis.index"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	n := len(index) / indexEntrySize
	keep := 0
	for keep < n && binary.LittleEndian.Uint32(index[keep*indexEntrySize:]) <= head {
		keep++
	}
	if keep == 0 {
		return 0, 0, nil
	}
	data, err := os.Open(filepath.Join(basePath, "abis.dat"))
	if err != nil {
		return 0, 0, err
	}
	defer data.Close()
	last := index[(keep-1)*indexEntrySize:]
	offset := binary.LittleEndian.Uint64(last[12:20])
	var hdr [dataHeaderSize]byte
	if _, err := data.ReadAt(hdr[:], int64(offset)); err != nil {
		return 0, 0, fmt.Errorf("read ABI record at %d: %w", offset, err)
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != binary.LittleEndian.Uint32(last[0:4]) ||
		binary.LittleEndian.Uint64(hdr[4:12]) != binary.LittleEndian.Uint64(last[4:12]) {
		return 0, 0, fmt.Errorf("ABI record at %d does not match its index entry", offset)
	}
	end := offset + dataHeaderSize + uint64(binary.LittleEndian.Uint32(hdr[12:16]))
	st, err := data.Stat()
	if err != nil {
		return 0, 0, err
	}
	if uint64(st.Size()) < end {
		return 0, 0, fmt.Errorf("ABI record at %d runs past the end of abis.dat: %w", offset, io.ErrUnexpectedEOF)
	}
	return end, uint32(keep), nil
}

// ManifestLimit reports the ABI entry count committed by the published view under coreRoot, re-reading it only when it changes.
// A store with no manifest has no limit; a missing or corrupt published view keeps the last good count, or zero before one.
func ManifestLimit(coreRoot string) func() (uint32, bool) {
	var (
		mu    sync.Mutex
		seen  bool
		ino   uint64
		mtime int64
		count uint32
	)
	return func() (uint32, bool) {
		mu.Lock()
		defer mu.Unlock()
		st, err := os.Stat(coremanifest.PublishedPath(coreRoot))
		if err != nil {
			if seen || coremanifest.HasManifest(coreRoot) {
				return count, true
			}
			return 0, false
		}
		if seen && inode(st) == ino && st.ModTime().UnixNano() == mtime {
			return count, true
		}
		m, err := coremanifest.ReadPublished(coreRoot)
		if err != nil {
			return count, true
		}
		seen, ino, mtime, count = true, inode(st), st.ModTime().UnixNano(), m.ABIIndexLen
		return count, true
	}
}
