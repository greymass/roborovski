package corereader

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

const (
	BlockIndexCacheMagic   = "BIDX"
	BlockIndexCacheVersion = 1
	BlockIndexCacheHeader  = 32
	BlockIndexEntrySize    = 32
)

type BlockIndexCacheEntry struct {
	BlockNum   uint32
	FileOffset uint64
	Size       uint32
	GlobMin    uint64
	GlobMax    uint64
}

type SliceBlockIndex struct {
	SliceNum   uint32
	StartBlock uint32
	Finalized  bool
	Entries    []BlockIndexCacheEntry
}

func (s *SliceBlockIndex) FindBlock(glob uint64) (uint32, bool) {
	if len(s.Entries) == 0 {
		return 0, false
	}

	idx := sort.Search(len(s.Entries), func(i int) bool {
		return s.Entries[i].GlobMax >= glob
	})

	if idx < len(s.Entries) {
		entry := &s.Entries[idx]
		if glob >= entry.GlobMin && glob <= entry.GlobMax {
			return entry.BlockNum, true
		}
	}

	return 0, false
}

func (s *SliceBlockIndex) GetGlobRange() (min, max uint64) {
	if len(s.Entries) == 0 {
		return 0, 0
	}
	return s.Entries[0].GlobMin, s.Entries[len(s.Entries)-1].GlobMax
}

type BlockIndexCache struct {
	mu     sync.RWMutex
	slices map[uint32]*SliceBlockIndex

	maxFinalizedSlice uint32
	dirty             atomic.Bool

	cacheFilePath string
	mmapData      []byte
	mmapFile      *os.File

	stats BlockIndexCacheStats
}

type BlockIndexCacheStats struct {
	TotalSlices     int
	TotalEntries    int64
	MemoryBytes     int64
	LoadTime        time.Duration
	FinalizedSlices int
	ActiveSlices    int
}

func NewBlockIndexCache(cacheFilePath string) *BlockIndexCache {
	return &BlockIndexCache{
		slices:        make(map[uint32]*SliceBlockIndex),
		cacheFilePath: cacheFilePath,
	}
}

func (c *BlockIndexCache) AddSlice(sliceNum uint32, startBlock uint32, entries []BlockIndexCacheEntry, finalized bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.slices[sliceNum] = &SliceBlockIndex{
		SliceNum:   sliceNum,
		StartBlock: startBlock,
		Finalized:  finalized,
		Entries:    entries,
	}

	if finalized && sliceNum > c.maxFinalizedSlice {
		c.maxFinalizedSlice = sliceNum
		c.dirty.Store(true)
	}
}

func (c *BlockIndexCache) AddBlockEntry(sliceNum, blockNum uint32, globMin, globMax uint64, fileOffset uint64, size uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slice, exists := c.slices[sliceNum]
	if !exists {
		startBlock := blockNum
		slice = &SliceBlockIndex{
			SliceNum:   sliceNum,
			StartBlock: startBlock,
			Finalized:  false,
			Entries:    make([]BlockIndexCacheEntry, 0, 10000),
		}
		c.slices[sliceNum] = slice
	}

	slice.Entries = append(slice.Entries, BlockIndexCacheEntry{
		BlockNum:   blockNum,
		FileOffset: fileOffset,
		Size:       size,
		GlobMin:    globMin,
		GlobMax:    globMax,
	})
}

func (c *BlockIndexCache) FinalizeSlice(sliceNum uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slice, exists := c.slices[sliceNum]
	if !exists {
		logger.Printf("warning", "BlockIndexCache: tried to finalize non-existent slice %d", sliceNum)
		return
	}

	if slice.Finalized {
		return
	}

	slice.Finalized = true
	if sliceNum > c.maxFinalizedSlice {
		c.maxFinalizedSlice = sliceNum
	}
	c.dirty.Store(true)

	logger.Printf("debug", "BlockIndexCache: finalized slice %d with %d entries", sliceNum, len(slice.Entries))
}

func (c *BlockIndexCache) FindBlock(sliceNum uint32, glob uint64) (blockNum uint32, found bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	slice, exists := c.slices[sliceNum]
	if !exists {
		return 0, false
	}

	return slice.FindBlock(glob)
}

func (c *BlockIndexCache) FindSliceForGlob(glob uint64) (sliceNum uint32, found bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for sn, slice := range c.slices {
		if len(slice.Entries) == 0 {
			continue
		}
		minGlob, maxGlob := slice.GetGlobRange()
		if glob >= minGlob && glob <= maxGlob {
			return sn, true
		}
	}

	return 0, false
}

func (c *BlockIndexCache) GetSlice(sliceNum uint32) (*SliceBlockIndex, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	slice, exists := c.slices[sliceNum]
	return slice, exists
}

func (c *BlockIndexCache) GetBlockEntry(sliceNum, blockNum uint32) (*BlockIndexCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	slice, exists := c.slices[sliceNum]
	if !exists {
		return nil, false
	}

	for i := range slice.Entries {
		if slice.Entries[i].BlockNum == blockNum {
			return &slice.Entries[i], true
		}
	}

	return nil, false
}

func (c *BlockIndexCache) HasSlice(sliceNum uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.slices[sliceNum]
	return exists
}

func (c *BlockIndexCache) GetMaxFinalizedSlice() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxFinalizedSlice
}

func (c *BlockIndexCache) GetStats() BlockIndexCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := BlockIndexCacheStats{
		TotalSlices: len(c.slices),
	}

	for _, slice := range c.slices {
		stats.TotalEntries += int64(len(slice.Entries))
		stats.MemoryBytes += int64(len(slice.Entries) * BlockIndexEntrySize)
		if slice.Finalized {
			stats.FinalizedSlices++
		} else {
			stats.ActiveSlices++
		}
	}

	return stats
}

func (c *BlockIndexCache) IsDirty() bool {
	return c.dirty.Load()
}

func (c *BlockIndexCache) LoadFromFile() error {
	start := time.Now()

	file, err := os.Open(c.cacheFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Printf("config", "BlockIndexCache: no cache file found at %s", c.cacheFilePath)
			return nil
		}
		return fmt.Errorf("failed to open cache file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat cache file: %w", err)
	}

	if fileInfo.Size() < BlockIndexCacheHeader {
		file.Close()
		return fmt.Errorf("cache file too small: %d bytes", fileInfo.Size())
	}

	mmapData, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(fileInfo.Size()),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to mmap cache file: %w", err)
	}

	if string(mmapData[0:4]) != BlockIndexCacheMagic {
		syscall.Munmap(mmapData)
		file.Close()
		return fmt.Errorf("invalid cache file magic")
	}

	version := binary.LittleEndian.Uint32(mmapData[4:8])
	if version != BlockIndexCacheVersion {
		syscall.Munmap(mmapData)
		file.Close()
		return fmt.Errorf("unsupported cache version: %d", version)
	}

	numSlices := binary.LittleEndian.Uint32(mmapData[8:12])
	maxSlice := binary.LittleEndian.Uint32(mmapData[12:16])

	c.mu.Lock()
	defer c.mu.Unlock()

	c.mmapFile = file
	c.mmapData = mmapData
	c.maxFinalizedSlice = maxSlice

	offset := BlockIndexCacheHeader
	for i := uint32(0); i < numSlices; i++ {
		if offset+12 > len(mmapData) {
			return fmt.Errorf("truncated cache file at slice %d", i)
		}

		sliceNum := binary.LittleEndian.Uint32(mmapData[offset : offset+4])
		startBlock := binary.LittleEndian.Uint32(mmapData[offset+4 : offset+8])
		numBlocks := binary.LittleEndian.Uint32(mmapData[offset+8 : offset+12])
		offset += 12

		entriesSize := int(numBlocks) * BlockIndexEntrySize
		if offset+entriesSize > len(mmapData) {
			return fmt.Errorf("truncated cache file at slice %d entries", sliceNum)
		}

		entries := make([]BlockIndexCacheEntry, numBlocks)
		for j := uint32(0); j < numBlocks; j++ {
			entryOffset := offset + int(j)*BlockIndexEntrySize
			entries[j] = BlockIndexCacheEntry{
				BlockNum:   binary.LittleEndian.Uint32(mmapData[entryOffset : entryOffset+4]),
				FileOffset: binary.LittleEndian.Uint64(mmapData[entryOffset+4 : entryOffset+12]),
				Size:       binary.LittleEndian.Uint32(mmapData[entryOffset+12 : entryOffset+16]),
				GlobMin:    binary.LittleEndian.Uint64(mmapData[entryOffset+16 : entryOffset+24]),
				GlobMax:    binary.LittleEndian.Uint64(mmapData[entryOffset+24 : entryOffset+32]),
			}
		}
		offset += entriesSize

		c.slices[sliceNum] = &SliceBlockIndex{
			SliceNum:   sliceNum,
			StartBlock: startBlock,
			Finalized:  true,
			Entries:    entries,
		}
	}

	c.stats.LoadTime = time.Since(start)

	logger.Printf("config", "BlockIndexCache: loaded %d slices from %s in %v",
		numSlices, c.cacheFilePath, c.stats.LoadTime)

	return nil
}

func (c *BlockIndexCache) SaveToFile() error {
	if !c.dirty.Load() {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var finalizedSlices []*SliceBlockIndex
	for _, slice := range c.slices {
		if slice.Finalized {
			finalizedSlices = append(finalizedSlices, slice)
		}
	}

	sort.Slice(finalizedSlices, func(i, j int) bool {
		return finalizedSlices[i].SliceNum < finalizedSlices[j].SliceNum
	})

	totalSize := BlockIndexCacheHeader
	for _, slice := range finalizedSlices {
		totalSize += 12 + len(slice.Entries)*BlockIndexEntrySize
	}

	data := make([]byte, totalSize)

	copy(data[0:4], BlockIndexCacheMagic)
	binary.LittleEndian.PutUint32(data[4:8], BlockIndexCacheVersion)
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(finalizedSlices)))
	binary.LittleEndian.PutUint32(data[12:16], c.maxFinalizedSlice)

	offset := BlockIndexCacheHeader
	for _, slice := range finalizedSlices {
		binary.LittleEndian.PutUint32(data[offset:offset+4], slice.SliceNum)
		binary.LittleEndian.PutUint32(data[offset+4:offset+8], slice.StartBlock)
		binary.LittleEndian.PutUint32(data[offset+8:offset+12], uint32(len(slice.Entries)))
		offset += 12

		for _, entry := range slice.Entries {
			binary.LittleEndian.PutUint32(data[offset:offset+4], entry.BlockNum)
			binary.LittleEndian.PutUint64(data[offset+4:offset+12], entry.FileOffset)
			binary.LittleEndian.PutUint32(data[offset+12:offset+16], entry.Size)
			binary.LittleEndian.PutUint64(data[offset+16:offset+24], entry.GlobMin)
			binary.LittleEndian.PutUint64(data[offset+24:offset+32], entry.GlobMax)
			offset += BlockIndexEntrySize
		}
	}

	tmpPath := c.cacheFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	if err := os.Rename(tmpPath, c.cacheFilePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename cache file: %w", err)
	}

	c.dirty.Store(false)

	logger.Printf("config", "BlockIndexCache: saved %d slices (%s) to %s",
		len(finalizedSlices), logger.FormatBytes(int64(totalSize)), c.cacheFilePath)

	return nil
}

func (c *BlockIndexCache) Close() error {
	if err := c.SaveToFile(); err != nil {
		logger.Printf("warning", "BlockIndexCache: failed to save on close: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mmapData != nil {
		syscall.Munmap(c.mmapData)
		c.mmapData = nil
	}
	if c.mmapFile != nil {
		c.mmapFile.Close()
		c.mmapFile = nil
	}

	return nil
}

func (c *BlockIndexCache) LoadSliceFromBlocksIndex(slicePath string, sliceNum, startBlock uint32, finalized bool) error {
	blockIndexPath := slicePath + "/blocks.index"

	data, err := os.ReadFile(blockIndexPath)
	if err != nil {
		return fmt.Errorf("failed to read blocks.index: %w", err)
	}

	if len(data) < 4 {
		return fmt.Errorf("blocks.index too small")
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	entries := make([]BlockIndexCacheEntry, 0, count)

	offset := 4
	for i := uint32(0); i < count; i++ {
		if offset+BlockIndexEntrySize > len(data) {
			break
		}

		entries = append(entries, BlockIndexCacheEntry{
			BlockNum:   binary.LittleEndian.Uint32(data[offset : offset+4]),
			FileOffset: binary.LittleEndian.Uint64(data[offset+4 : offset+12]),
			Size:       binary.LittleEndian.Uint32(data[offset+12 : offset+16]),
			GlobMin:    binary.LittleEndian.Uint64(data[offset+16 : offset+24]),
			GlobMax:    binary.LittleEndian.Uint64(data[offset+24 : offset+32]),
		})
		offset += BlockIndexEntrySize
	}

	c.AddSlice(sliceNum, startBlock, entries, finalized)

	return nil
}
