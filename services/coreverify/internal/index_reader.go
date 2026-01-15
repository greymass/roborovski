package internal

import (
	"encoding/binary"
	"errors"
	"os"
)

var (
	ErrInvalidIndex = errors.New("invalid index file")
	ErrNotFound     = errors.New("not found")
)

// BlockIndexEntry represents a block's location in the log file
type BlockIndexEntry struct {
	BlockNum uint32
	Offset   uint64
	Size     uint32
	GlobMin  uint64
	GlobMax  uint64
}

// BlockIndex provides O(1) lookup of blocks by block number
type BlockIndex struct {
	entries  []BlockIndexEntry
	minBlock uint32
	maxBlock uint32
}

// LoadBlockIndex loads an index from a file
func LoadBlockIndex(path string) (*BlockIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read count
	countBytes := make([]byte, 4)
	if _, err := file.Read(countBytes); err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(countBytes)

	if count == 0 || count > 10000000 { // Sanity check: max 10M blocks per index
		return nil, ErrInvalidIndex
	}

	// Read entries
	idx := &BlockIndex{
		entries: make([]BlockIndexEntry, count),
	}

	entryBytes := make([]byte, 32)
	for i := uint32(0); i < count; i++ {
		if _, err := file.Read(entryBytes); err != nil {
			return nil, err
		}

		entry := BlockIndexEntry{
			BlockNum: binary.LittleEndian.Uint32(entryBytes[0:4]),
			Offset:   binary.LittleEndian.Uint64(entryBytes[4:12]),
			Size:     binary.LittleEndian.Uint32(entryBytes[12:16]),
			GlobMin:  binary.LittleEndian.Uint64(entryBytes[16:24]),
			GlobMax:  binary.LittleEndian.Uint64(entryBytes[24:32]),
		}
		idx.entries[i] = entry

		// Track min/max
		if i == 0 {
			idx.minBlock = entry.BlockNum
			idx.maxBlock = entry.BlockNum
		} else {
			if entry.BlockNum < idx.minBlock {
				idx.minBlock = entry.BlockNum
			}
			if entry.BlockNum > idx.maxBlock {
				idx.maxBlock = entry.BlockNum
			}
		}
	}

	return idx, nil
}

// Count returns the number of entries
func (idx *BlockIndex) Count() int {
	return len(idx.entries)
}

// GetAllEntries returns all entries (for validation)
func (idx *BlockIndex) GetAllEntries() []BlockIndexEntry {
	return idx.entries
}

// GetTotalGlobs returns the total number of globs across all blocks
func (idx *BlockIndex) GetTotalGlobs() int {
	total := 0
	for _, entry := range idx.entries {
		if entry.GlobMax >= entry.GlobMin {
			total += int(entry.GlobMax - entry.GlobMin + 1)
		}
	}
	return total
}

// SliceInfo contains metadata about a single slice
type SliceInfo struct {
	SliceNum       uint32 `json:"slice_num"`
	StartBlock     uint32 `json:"start_block"`
	EndBlock       uint32 `json:"end_block"`
	MaxBlock       uint32 `json:"max_block"`
	BlocksPerSlice uint32 `json:"blocks_per_slice"`
	Finalized      bool   `json:"finalized"`
	GlobMin        uint64 `json:"glob_min"`
	GlobMax        uint64 `json:"glob_max"`
}
