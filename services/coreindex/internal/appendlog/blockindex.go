package appendlog

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"
)

var (
	ErrInvalidIndex = errors.New("invalid index file")
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

// NewBlockIndex creates an empty block index
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0, 10000),
	}
}

// Add adds a block index entry
func (idx *BlockIndex) Add(entry BlockIndexEntry) {
	idx.entries = append(idx.entries, entry)
	if len(idx.entries) == 1 {
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

// Get retrieves a block entry by block number
func (idx *BlockIndex) Get(blockNum uint32) (*BlockIndexEntry, error) {
	if len(idx.entries) == 0 {
		return nil, ErrNotFound
	}

	// Try direct access first (common case: sequential blocks)
	if blockNum >= idx.minBlock && blockNum <= idx.maxBlock {
		expectedIdx := int(blockNum - idx.minBlock)
		if expectedIdx < len(idx.entries) && idx.entries[expectedIdx].BlockNum == blockNum {
			return &idx.entries[expectedIdx], nil
		}
	}

	// Fall back to binary search (handles gaps)
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].BlockNum >= blockNum
	})

	if i < len(idx.entries) && idx.entries[i].BlockNum == blockNum {
		return &idx.entries[i], nil
	}

	return nil, ErrNotFound
}

// WriteTo writes the index to a file
func (idx *BlockIndex) WriteTo(path string) error {
	return idx.WriteToWithSync(path, true)
}

// WriteToWithSync writes the index to a file with optional fsync
func (idx *BlockIndex) WriteToWithSync(path string, doSync bool) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Allocate buffer for all data: 4 bytes count + 32 bytes per entry
	bufSize := 4 + len(idx.entries)*32
	buf := make([]byte, bufSize)

	// Write count
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(idx.entries)))

	// Write entries (32 bytes each)
	offset := 4
	for _, entry := range idx.entries {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.BlockNum)
		binary.LittleEndian.PutUint64(buf[offset+4:offset+12], entry.Offset)
		binary.LittleEndian.PutUint32(buf[offset+12:offset+16], entry.Size)
		binary.LittleEndian.PutUint64(buf[offset+16:offset+24], entry.GlobMin)
		binary.LittleEndian.PutUint64(buf[offset+24:offset+32], entry.GlobMax)
		offset += 32
	}

	// Single write
	if _, err := file.Write(buf); err != nil {
		return err
	}

	if doSync {
		return file.Sync()
	}
	return nil
}

// LoadBlockIndex loads an index from a file
func LoadBlockIndex(path string) (*BlockIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 4 {
		return nil, ErrInvalidIndex
	}

	count := binary.LittleEndian.Uint32(data[0:4])

	if count == 0 || count > 10000000 {
		return nil, ErrInvalidIndex
	}

	expectedSize := 4 + int(count)*32
	if len(data) < expectedSize {
		return nil, ErrInvalidIndex
	}

	idx := &BlockIndex{
		entries: make([]BlockIndexEntry, count),
	}

	offset := 4
	for i := uint32(0); i < count; i++ {
		idx.entries[i].BlockNum = binary.LittleEndian.Uint32(data[offset : offset+4])
		idx.entries[i].Offset = binary.LittleEndian.Uint64(data[offset+4 : offset+12])
		idx.entries[i].Size = binary.LittleEndian.Uint32(data[offset+12 : offset+16])
		idx.entries[i].GlobMin = binary.LittleEndian.Uint64(data[offset+16 : offset+24])
		idx.entries[i].GlobMax = binary.LittleEndian.Uint64(data[offset+24 : offset+32])
		offset += 32

		// Track min/max
		if i == 0 {
			idx.minBlock = idx.entries[i].BlockNum
			idx.maxBlock = idx.entries[i].BlockNum
		} else {
			if idx.entries[i].BlockNum < idx.minBlock {
				idx.minBlock = idx.entries[i].BlockNum
			}
			if idx.entries[i].BlockNum > idx.maxBlock {
				idx.maxBlock = idx.entries[i].BlockNum
			}
		}
	}

	return idx, nil
}

// Count returns the number of entries
func (idx *BlockIndex) Count() int {
	return len(idx.entries)
}

// Range returns the block number range
func (idx *BlockIndex) Range() (min, max uint32) {
	return idx.minBlock, idx.maxBlock
}

// GetAllEntries returns all entries (for glob index rebuilding)
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

// TruncateAfter removes all entries with BlockNum > maxBlock
func (idx *BlockIndex) TruncateAfter(maxBlock uint32) int {
	if len(idx.entries) == 0 {
		return 0
	}

	// Find truncation point
	truncateAt := len(idx.entries)
	for i, entry := range idx.entries {
		if entry.BlockNum > maxBlock {
			truncateAt = i
			break
		}
	}

	removed := len(idx.entries) - truncateAt
	if removed > 0 {
		idx.entries = idx.entries[:truncateAt]
		// Update maxBlock
		if len(idx.entries) > 0 {
			idx.maxBlock = idx.entries[len(idx.entries)-1].BlockNum
		} else {
			idx.minBlock = 0
			idx.maxBlock = 0
		}
	}

	return removed
}
