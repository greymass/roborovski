package internal

import (
	"sync"
)

type WALIndex struct {
	mu      sync.RWMutex
	entries map[[5]byte][]uint32
}

func NewWALIndex() *WALIndex {
	return &WALIndex{
		entries: make(map[[5]byte][]uint32),
	}
}

func (idx *WALIndex) Add(prefix5 [5]byte, blockNum uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[prefix5] = append(idx.entries[prefix5], blockNum)
}

func (idx *WALIndex) AddBatch(entries []WALEntry) {
	if len(entries) == 0 {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, entry := range entries {
		idx.entries[entry.Prefix5] = append(idx.entries[entry.Prefix5], entry.BlockNum)
	}
}

func (idx *WALIndex) Get(prefix5 [5]byte) []uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	blockNums, ok := idx.entries[prefix5]
	if !ok {
		return nil
	}

	result := make([]uint32, len(blockNums))
	copy(result, blockNums)
	return result
}

func (idx *WALIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries = make(map[[5]byte][]uint32)
}

func (idx *WALIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	count := 0
	for _, blockNums := range idx.entries {
		count += len(blockNums)
	}
	return count
}

func (idx *WALIndex) UniqueKeys() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.entries)
}

func (idx *WALIndex) LoadFromWAL(wal *WAL) error {
	entries, err := wal.ReadAll()
	if err != nil {
		return err
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries = make(map[[5]byte][]uint32)
	for _, entry := range entries {
		idx.entries[entry.Prefix5] = append(idx.entries[entry.Prefix5], entry.BlockNum)
	}
	return nil
}

func (idx *WALIndex) GetAllEntries() []WALEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var entries []WALEntry
	for prefix5, blockNums := range idx.entries {
		for _, blockNum := range blockNums {
			entries = append(entries, WALEntry{
				Prefix5:  prefix5,
				BlockNum: blockNum,
			})
		}
	}
	return entries
}
