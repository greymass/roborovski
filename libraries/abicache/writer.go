package abicache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Writer struct {
	dataFile  *os.File
	indexFile *os.File
	mu        sync.Mutex
}

func NewWriter(basePath string) (*Writer, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	dataPath := filepath.Join(basePath, "abis.dat")
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	indexPath := filepath.Join(basePath, "abis.index")
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	return &Writer{
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

func (w *Writer) Write(blockNum uint32, contract uint64, abiJSON []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dataOffset, err := w.dataFile.Seek(0, 2)
	if err != nil {
		return fmt.Errorf("failed to get data offset: %w", err)
	}

	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], blockNum)
	binary.LittleEndian.PutUint64(header[4:12], contract)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(abiJSON)))

	if _, err := w.dataFile.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := w.dataFile.Write(abiJSON); err != nil {
		return fmt.Errorf("failed to write ABI data: %w", err)
	}

	indexEntry := make([]byte, 20)
	binary.LittleEndian.PutUint32(indexEntry[0:4], blockNum)
	binary.LittleEndian.PutUint64(indexEntry[4:12], contract)
	binary.LittleEndian.PutUint64(indexEntry[12:20], uint64(dataOffset))

	if _, err := w.indexFile.Write(indexEntry); err != nil {
		return fmt.Errorf("failed to write index entry: %w", err)
	}

	return nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}
	if err := w.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error

	if err := w.dataFile.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("sync data: %w", err))
	}
	if err := w.indexFile.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("sync index: %w", err))
	}
	if err := w.dataFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close data: %w", err))
	}
	if err := w.indexFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close index: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
