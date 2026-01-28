package appendlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

const DefaultBlocksPerSlice = 10000

type Slice struct {
	// Metadata
	SliceNum       uint32 // Slice number (0, 1, 2, ...)
	StartBlock     uint32 // First block in slice (inclusive)
	EndBlock       uint32 // Last block in slice (inclusive, or 0 if active)
	MaxBlock       uint32 // Last block in slice range (for header)
	BlocksPerSlice uint32 // Blocks per slice (for header)
	Finalized      bool   // True if slice is complete and optimized

	// Components (lazily loaded)
	blockLog   *Writer
	blockIndex *BlockIndex
	reader     *Reader
	header     *DataLogHeader // Parsed header from data.log

	// State
	basePath string
	loaded   bool
	mu       sync.RWMutex
}

// SliceInfo is serialized metadata about a slice
type SliceInfo struct {
	SliceNum       uint32 `json:"slice_num"`
	StartBlock     uint32 `json:"start_block"`      // First block in slice range (inclusive)
	EndBlock       uint32 `json:"end_block"`        // Last block actually written (may be < MaxBlock if active)
	MaxBlock       uint32 `json:"max_block"`        // Last block in slice range (for directory naming)
	BlocksPerSlice uint32 `json:"blocks_per_slice"` // Slice size (for directory naming)
	Finalized      bool   `json:"finalized"`
	GlobMin        uint64 `json:"glob_min"`
	GlobMax        uint64 `json:"glob_max"`
}

// SliceManager manages multiple storage slices for tiered storage
type SliceManager struct {
	basePath       string
	blocksPerSlice uint32
	debug          bool

	// Slice metadata (lightweight, always loaded)
	sliceInfos []SliceInfo

	// Active slice (fully loaded)
	activeSlice *Slice

	// Cached slices for reads (lazily loaded, LRU eviction)
	cachedSlices    map[uint32]*Slice
	cacheOrder      []uint32 // LRU order (oldest first)
	maxCachedSlices int

	// Slice creation tracking
	slicesCreated      uint32 // Total slices created this session
	logSliceInterval   uint32 // Log every N slices created (0 = only debug mode)
	lastLoggedSliceNum uint32 // Last slice number we logged

	// Background hash computation
	hashQueue chan string   // Queue of slice paths needing hash computation
	hashDone  chan struct{} // Signal to stop hash worker

	// Aggregated rotation stats for periodic logging
	rotationStats    rotationStatsAccum
	lastStatsLog     time.Time
	statsLogInterval time.Duration // How often to log aggregated stats (default 10s)

	mu sync.RWMutex
}

// rotationStatsAccum accumulates rotation timing stats for periodic logging
type rotationStatsAccum struct {
	count      int
	firstSlice uint32
	lastSlice  uint32
	// RotateSlice timings
	totalFinalize time.Duration
	totalCache    time.Duration
	totalCreate   time.Duration
	totalSaveIdx  time.Duration
	totalRotate   time.Duration
	maxRotate     time.Duration
	// FinalizeWithOptions breakdown
	totalValidSort time.Duration
	totalFinalIdx  time.Duration
	totalSyncClose time.Duration
	totalFlag      time.Duration
	totalReopen    time.Duration
}

// NewSliceManager creates or opens a slice manager
func NewSliceManager(basePath string, blocksPerSlice uint32, maxCachedSlices int, debug bool, logSliceInterval uint32) (*SliceManager, error) {
	startTime := time.Now()
	logger.Printf("debug-startup", "    NewSliceManager starting...")

	if blocksPerSlice == 0 {
		blocksPerSlice = DefaultBlocksPerSlice
	}
	if maxCachedSlices == 0 {
		maxCachedSlices = 2 // Keep 2 slices in cache (current + previous)
	}

	// Create directory if needed
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	sm := &SliceManager{
		basePath:           basePath,
		blocksPerSlice:     blocksPerSlice,
		debug:              debug,
		sliceInfos:         make([]SliceInfo, 0),
		cachedSlices:       make(map[uint32]*Slice),
		cacheOrder:         make([]uint32, 0),
		maxCachedSlices:    maxCachedSlices,
		logSliceInterval:   logSliceInterval,
		slicesCreated:      0,
		lastLoggedSliceNum: 0,
		hashQueue:          make(chan string, 100), // Buffer for pending hash jobs
		hashDone:           make(chan struct{}),
		lastStatsLog:       time.Now(),
		statsLogInterval:   10 * time.Second,
	}

	// Start background hash worker
	go sm.hashWorker()

	// Load existing slice metadata
	t0 := time.Now()
	if err := sm.loadSliceInfos(); err != nil {
		// No slices yet - that's fine for fresh DB
		sm.sliceInfos = make([]SliceInfo, 0)
		logger.Printf("debug-startup", "    loadSliceInfos: no existing slices (%.2fs)", time.Since(t0).Seconds())
	} else {
		logger.Printf("debug-startup", "    loadSliceInfos: loaded %d slices (%.2fs)", len(sm.sliceInfos), time.Since(t0).Seconds())
	}

	logger.Printf("debug-startup", "    NewSliceManager completed in %.2fs", time.Since(startTime).Seconds())
	return sm, nil
}

// BlockToSliceNum returns the slice number for a given block
func (sm *SliceManager) BlockToSliceNum(blockNum uint32) uint32 {
	if blockNum == 0 {
		return 0
	}
	return (blockNum - 1) / sm.blocksPerSlice
}

// SliceNumToBlockRange returns the block range for a slice number
func (sm *SliceManager) SliceNumToBlockRange(sliceNum uint32) (start, end uint32) {
	start = sliceNum*sm.blocksPerSlice + 1
	end = (sliceNum + 1) * sm.blocksPerSlice
	return
}

func (sm *SliceManager) slicePath(sliceNum uint32) string {
	start, end := sm.SliceNumToBlockRange(sliceNum)
	return filepath.Join(sm.basePath, fmt.Sprintf("history_%010d-%010d", start, end))
}

// GetActiveSlice returns the active slice for writing, creating if needed
// blockNumHint: optional block number to be written (0 = use existing logic)
func (sm *SliceManager) GetActiveSlice() (*Slice, error) {
	return sm.getActiveSliceWithHint(0)
}

// GetActiveSliceFor returns the active slice for a specific block number
func (sm *SliceManager) GetActiveSliceFor(blockNum uint32) (*Slice, error) {
	return sm.getActiveSliceWithHint(blockNum)
}

func (sm *SliceManager) getActiveSliceWithHint(blockNumHint uint32) (*Slice, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeSlice != nil {
		return sm.activeSlice, nil
	}

	// Find or create active slice
	var activeSliceNum uint32
	if len(sm.sliceInfos) == 0 {
		// Fresh start - use block number hint if provided
		if blockNumHint > 0 {
			activeSliceNum = sm.BlockToSliceNum(blockNumHint)
		} else {
			activeSliceNum = 0
		}
	} else {
		// Find last non-finalized slice
		lastInfo := sm.sliceInfos[len(sm.sliceInfos)-1]
		if lastInfo.Finalized {
			activeSliceNum = lastInfo.SliceNum + 1
		} else {
			activeSliceNum = lastInfo.SliceNum
		}
	}

	slice, err := sm.loadOrCreateSlice(activeSliceNum, true)
	if err != nil {
		return nil, err
	}

	sm.activeSlice = slice
	return slice, nil
}

// GetSliceForBlock returns the slice containing a specific block
func (sm *SliceManager) GetSliceForBlock(blockNum uint32) (*Slice, error) {
	sliceNum := sm.BlockToSliceNum(blockNum)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if it's the active slice
	if sm.activeSlice != nil && sm.activeSlice.SliceNum == sliceNum {
		return sm.activeSlice, nil
	}

	// Check cache
	if slice, found := sm.cachedSlices[sliceNum]; found {
		sm.updateCacheOrder(sliceNum)
		return slice, nil
	}

	// Load slice (read-only)
	slice, err := sm.loadOrCreateSlice(sliceNum, false)
	if err != nil {
		return nil, err
	}

	// Add to cache
	sm.addToCache(sliceNum, slice)

	return slice, nil
}

// GetSliceForGlob returns the slice containing a specific global sequence
func (sm *SliceManager) GetSliceForGlob(glob uint64) (*Slice, error) {
	sm.mu.RLock()

	// Search slice infos for the right slice
	for i := len(sm.sliceInfos) - 1; i >= 0; i-- {
		info := sm.sliceInfos[i]
		if glob >= info.GlobMin && glob <= info.GlobMax {
			sm.mu.RUnlock()
			return sm.GetSliceByNum(info.SliceNum)
		}
	}
	sm.mu.RUnlock()

	return nil, fmt.Errorf("glob %d not found in any slice", glob)
}

// GetSliceByNum returns a specific slice by number
func (sm *SliceManager) GetSliceByNum(sliceNum uint32) (*Slice, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if it's the active slice
	if sm.activeSlice != nil && sm.activeSlice.SliceNum == sliceNum {
		return sm.activeSlice, nil
	}

	// Check cache
	if slice, found := sm.cachedSlices[sliceNum]; found {
		sm.updateCacheOrder(sliceNum)
		return slice, nil
	}

	// Load slice
	slice, err := sm.loadOrCreateSlice(sliceNum, false)
	if err != nil {
		return nil, err
	}

	// Add to cache
	sm.addToCache(sliceNum, slice)

	return slice, nil
}

// RotateSlice finalizes the current active slice and creates a new one
func (sm *SliceManager) RotateSlice() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeSlice == nil {
		return nil
	}

	var t0, t1, t2, t3, t4 time.Time
	t0 = time.Now()
	rotatedSliceNum := sm.activeSlice.SliceNum

	// Finalize current slice (skip hash - computed in background)
	slicePath := sm.activeSlice.basePath
	finalizeTiming, err := sm.activeSlice.FinalizeWithOptions(true)
	if err != nil {
		return fmt.Errorf("failed to finalize slice %d: %w", sm.activeSlice.SliceNum, err)
	}
	t1 = time.Now()

	// Queue hash computation in background (non-blocking)
	sm.QueueHashComputation(slicePath)

	// Update slice info
	sm.updateSliceInfo(sm.activeSlice)

	// Move to cache (it's now read-only)
	sm.addToCache(sm.activeSlice.SliceNum, sm.activeSlice)
	t2 = time.Now()

	// Create new active slice
	newSliceNum := sm.activeSlice.SliceNum + 1
	newSlice, err := sm.loadOrCreateSlice(newSliceNum, true)
	if err != nil {
		return fmt.Errorf("failed to create slice %d: %w", newSliceNum, err)
	}
	t3 = time.Now()

	sm.activeSlice = newSlice

	// Save empty index files immediately so readers can detect the new slice
	if err := newSlice.SaveIndices(); err != nil {
		return fmt.Errorf("failed to save initial indices for slice %d: %w", newSliceNum, err)
	}
	t4 = time.Now()

	// Accumulate stats for aggregated logging
	totalRotate := t4.Sub(t0)
	sm.rotationStats.count++
	if sm.rotationStats.count == 1 {
		sm.rotationStats.firstSlice = rotatedSliceNum
	}
	sm.rotationStats.lastSlice = rotatedSliceNum
	sm.rotationStats.totalFinalize += t1.Sub(t0)
	sm.rotationStats.totalCache += t2.Sub(t1)
	sm.rotationStats.totalCreate += t3.Sub(t2)
	sm.rotationStats.totalSaveIdx += t4.Sub(t3)
	sm.rotationStats.totalRotate += totalRotate
	if totalRotate > sm.rotationStats.maxRotate {
		sm.rotationStats.maxRotate = totalRotate
	}
	// FinalizeWithOptions breakdown
	sm.rotationStats.totalValidSort += finalizeTiming.ValidSort
	sm.rotationStats.totalFinalIdx += finalizeTiming.SaveIdx
	sm.rotationStats.totalSyncClose += finalizeTiming.SyncClose
	sm.rotationStats.totalFlag += finalizeTiming.Flag
	sm.rotationStats.totalReopen += finalizeTiming.Reopen

	// Log aggregated stats periodically
	sm.logRotationStatsIfNeeded()

	return nil
}

func (sm *SliceManager) logRotationStatsIfNeeded() {
	if sm.rotationStats.count == 0 {
		return
	}
	if time.Since(sm.lastStatsLog) < sm.statsLogInterval {
		return
	}

	s := sm.rotationStats
	n := float64(s.count)

	logger.Printf("debug-timing", "SliceRotate: slices %d-%d (%d rot), avg=%.2fms max=%.2fms | finalize=%.2fms cache=%.2fms create=%.2fms saveIdx=%.2fms",
		s.firstSlice, s.lastSlice, s.count,
		float64(s.totalRotate.Microseconds())/n/1000.0,
		float64(s.maxRotate.Microseconds())/1000.0,
		float64(s.totalFinalize.Microseconds())/n/1000.0,
		float64(s.totalCache.Microseconds())/n/1000.0,
		float64(s.totalCreate.Microseconds())/n/1000.0,
		float64(s.totalSaveIdx.Microseconds())/n/1000.0)

	logger.Printf("debug-timing", "  Finalize breakdown: validSort=%.2fms saveIdx=%.2fms syncClose=%.2fms flag=%.2fms reopen=%.2fms",
		float64(s.totalValidSort.Microseconds())/n/1000.0,
		float64(s.totalFinalIdx.Microseconds())/n/1000.0,
		float64(s.totalSyncClose.Microseconds())/n/1000.0,
		float64(s.totalFlag.Microseconds())/n/1000.0,
		float64(s.totalReopen.Microseconds())/n/1000.0)

	sm.rotationStats = rotationStatsAccum{}
	sm.lastStatsLog = time.Now()
}

// ShouldRotate returns true if the active slice has reached its capacity
func (sm *SliceManager) ShouldRotate(nextBlockNum uint32) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.activeSlice == nil {
		return false
	}

	// Check if next block would exceed slice boundary
	_, sliceEnd := sm.SliceNumToBlockRange(sm.activeSlice.SliceNum)
	return nextBlockNum > sliceEnd
}

// loadOrCreateSlice loads an existing slice or creates a new one
func (sm *SliceManager) loadOrCreateSlice(sliceNum uint32, forWriting bool) (*Slice, error) {
	slicePath := sm.slicePath(sliceNum)

	// Check if slice directory exists
	_, err := os.Stat(slicePath)
	sliceExists := err == nil

	if !sliceExists {
		if !forWriting {
			return nil, fmt.Errorf("slice %d does not exist", sliceNum)
		}
		// Create new slice directory
		if err := os.MkdirAll(slicePath, 0755); err != nil {
			return nil, err
		}

		// Track slice creation
		sm.slicesCreated++

		// Log based on interval (not debug mode - aggregated stats cover debug)
		if sm.logSliceInterval > 0 && (sm.slicesCreated%sm.logSliceInterval) == 0 {
			startBlock, endBlock := sm.SliceNumToBlockRange(sliceNum)
			logger.Printf("slice", "Created slice %d (blocks %d-%d) [%d slices created this session]",
				sliceNum, startBlock, endBlock, sm.slicesCreated)
		}
	}

	startBlock, maxBlock := sm.SliceNumToBlockRange(sliceNum)

	slice := &Slice{
		SliceNum:       sliceNum,
		StartBlock:     startBlock,
		MaxBlock:       maxBlock,
		BlocksPerSlice: sm.blocksPerSlice,
		basePath:       slicePath,
	}

	if err := slice.Load(forWriting); err != nil {
		return nil, err
	}

	// Add to slice infos if new
	if !sliceExists {
		startBlock, maxBlock := sm.SliceNumToBlockRange(sliceNum)
		info := SliceInfo{
			SliceNum:       sliceNum,
			StartBlock:     startBlock,
			MaxBlock:       maxBlock,
			BlocksPerSlice: sm.blocksPerSlice,
			Finalized:      false,
		}
		sm.sliceInfos = append(sm.sliceInfos, info)
		sm.saveSliceInfos()
	}

	return slice, nil
}

// updateSliceInfo updates the metadata for a slice, adding it if not present.
// This handles the case where a slice directory exists on disk (from a prior crash)
// but wasn't added to sliceInfos during loadOrCreateSlice.
func (sm *SliceManager) updateSliceInfo(slice *Slice) {
	found := false
	for i := range sm.sliceInfos {
		if sm.sliceInfos[i].SliceNum == slice.SliceNum {
			sm.sliceInfos[i].EndBlock = slice.EndBlock
			sm.sliceInfos[i].Finalized = slice.Finalized
			sm.sliceInfos[i].GlobMin = slice.GetGlobMin()
			sm.sliceInfos[i].GlobMax = slice.GetGlobMax()
			found = true
			break
		}
	}
	if !found {
		sm.sliceInfos = append(sm.sliceInfos, SliceInfo{
			SliceNum:       slice.SliceNum,
			StartBlock:     slice.StartBlock,
			EndBlock:       slice.EndBlock,
			MaxBlock:       slice.MaxBlock,
			BlocksPerSlice: slice.BlocksPerSlice,
			Finalized:      slice.Finalized,
			GlobMin:        slice.GetGlobMin(),
			GlobMax:        slice.GetGlobMax(),
		})
		sort.Slice(sm.sliceInfos, func(i, j int) bool {
			return sm.sliceInfos[i].SliceNum < sm.sliceInfos[j].SliceNum
		})
	}
	sm.saveSliceInfos()
}

// addToCache adds a slice to the LRU cache
func (sm *SliceManager) addToCache(sliceNum uint32, slice *Slice) {
	// Evict if cache is full
	for len(sm.cachedSlices) >= sm.maxCachedSlices && len(sm.cacheOrder) > 0 {
		oldest := sm.cacheOrder[0]
		sm.cacheOrder = sm.cacheOrder[1:]
		if oldSlice, found := sm.cachedSlices[oldest]; found {
			oldSlice.Close()
			delete(sm.cachedSlices, oldest)
		}
	}

	sm.cachedSlices[sliceNum] = slice
	sm.cacheOrder = append(sm.cacheOrder, sliceNum)
}

// updateCacheOrder moves a slice to end of LRU order (most recently used)
func (sm *SliceManager) updateCacheOrder(sliceNum uint32) {
	for i, num := range sm.cacheOrder {
		if num == sliceNum {
			sm.cacheOrder = append(sm.cacheOrder[:i], sm.cacheOrder[i+1:]...)
			sm.cacheOrder = append(sm.cacheOrder, sliceNum)
			break
		}
	}
}

// saveSliceInfos saves slice metadata to disk
func (sm *SliceManager) saveSliceInfos() error {
	path := filepath.Join(sm.basePath, "slices.json")

	// Use Marshal instead of MarshalIndent - 10x faster, profiling showed 20% CPU in appendIndent
	data, err := json.Marshal(sm.sliceInfos)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// loadSliceInfos loads slice metadata from disk
func (sm *SliceManager) loadSliceInfos() error {
	path := filepath.Join(sm.basePath, "slices.json")

	t0 := time.Now()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	readTime := time.Since(t0)

	t1 := time.Now()
	err = json.Unmarshal(data, &sm.sliceInfos)
	unmarshalTime := time.Since(t1)

	// Log detailed timing if it took more than 100ms
	if readTime+unmarshalTime > 100*time.Millisecond {
		logger.Printf("debug-startup", "      loadSliceInfos: read %.2fs (%d bytes), unmarshal %.2fs (%d entries)",
			readTime.Seconds(), len(data), unmarshalTime.Seconds(), len(sm.sliceInfos))
	}

	return err
}

// AddSliceInfo adds a slice info entry and persists to disk
func (sm *SliceManager) AddSliceInfo(info SliceInfo) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.sliceInfos = append(sm.sliceInfos, info)

	sort.Slice(sm.sliceInfos, func(i, j int) bool {
		return sm.sliceInfos[i].SliceNum < sm.sliceInfos[j].SliceNum
	})

	return sm.saveSliceInfos()
}

// GetSliceInfos returns all slice metadata (for status/debug)
func (sm *SliceManager) GetSliceInfos() []SliceInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	infos := make([]SliceInfo, len(sm.sliceInfos))
	copy(infos, sm.sliceInfos)
	return infos
}

// GetTotalBlocks returns total block count across all slices
func (sm *SliceManager) GetTotalBlocks() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.sliceInfos) == 0 {
		return 0
	}

	// Sum finalized slices + active slice
	var total uint32
	for _, info := range sm.sliceInfos {
		if info.Finalized {
			total += info.EndBlock - info.StartBlock + 1
		}
	}

	// Add blocks from active slice
	if sm.activeSlice != nil && sm.activeSlice.blockIndex != nil {
		total += uint32(sm.activeSlice.blockIndex.Count())
	}

	return total
}

// Sync syncs all data to disk
func (sm *SliceManager) Sync() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Sync active slice
	if sm.activeSlice != nil {
		if err := sm.activeSlice.Sync(); err != nil {
			return err
		}
		sm.updateSliceInfo(sm.activeSlice)
	}

	return sm.saveSliceInfos()
}

// SaveActiveSliceIndices saves the active slice's block index to disk with fsync.
// This ensures queries can find blocks that are about to be broadcast.
func (sm *SliceManager) SaveActiveSliceIndices() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeSlice == nil {
		return nil
	}

	return sm.activeSlice.SaveIndices()
}

// Close closes all slices
func (sm *SliceManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Stop background hash worker
	sm.StopHashWorker()

	// Close active slice
	if sm.activeSlice != nil {
		sm.updateSliceInfo(sm.activeSlice)
		if err := sm.activeSlice.Close(); err != nil {
			return err
		}
	}

	// Close cached slices
	for _, slice := range sm.cachedSlices {
		slice.Close()
	}

	return sm.saveSliceInfos()
}

// Slice methods

// Load loads slice components
func (s *Slice) Load(forWriting bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loaded {
		return nil
	}

	startTime := time.Now()

	// Load block index
	t0 := time.Now()
	blockIdxPath := filepath.Join(s.basePath, "blocks.index")
	if blockIdx, err := LoadBlockIndex(blockIdxPath); err == nil {
		s.blockIndex = blockIdx
	} else {
		s.blockIndex = NewBlockIndex()
	}
	blockIdxTime := time.Since(t0)

	// Open block data log
	var writerTime, readerTime time.Duration
	dataLogPath := filepath.Join(s.basePath, "data.log")
	if forWriting {
		t2 := time.Now()
		header := NewDataLogHeader(s.BlocksPerSlice, s.StartBlock, s.MaxBlock)
		blockLog, err := NewWriterWithHeader(dataLogPath, header)
		if err != nil {
			return fmt.Errorf("failed to open data log for writing: %w", err)
		}
		s.blockLog = blockLog
		s.header = header
		writerTime = time.Since(t2)
	}

	// Open reader (with header validation if file exists and has header)
	if _, err := os.Stat(dataLogPath); err == nil {
		t3 := time.Now()
		hasHdr, _ := HasHeader(dataLogPath)
		if hasHdr {
			reader, err := NewReaderWithHeader(dataLogPath)
			if err != nil {
				return fmt.Errorf("failed to open data reader with header: %w", err)
			}
			s.reader = reader
			s.header = reader.Header()
		} else {
			reader, err := NewReader(dataLogPath)
			if err != nil {
				return fmt.Errorf("failed to open data reader: %w", err)
			}
			s.reader = reader
		}
		readerTime = time.Since(t3)
	}

	s.loaded = true

	// Log timing if it took more than 100ms
	totalTime := time.Since(startTime)
	if totalTime > 100*time.Millisecond {
		logger.Printf("debug-startup", "      Slice.Load(%d) took %.2fs: blockIdx=%.2fs writer=%.2fs reader=%.2fs",
			s.SliceNum, totalTime.Seconds(), blockIdxTime.Seconds(),
			writerTime.Seconds(), readerTime.Seconds())
	}

	return nil
}

// AppendBlock appends a block and builds glob index incrementally
// This keeps the glob index current for crash recovery and live queries
// If liveMode is true, indexes are written to disk immediately after each block
func (s *Slice) AppendBlock(blockNum uint32, data []byte, globMin, globMax uint64, liveMode bool) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.blockLog == nil {
		return 0, fmt.Errorf("slice %d is not open for writing", s.SliceNum)
	}

	// Append to block log
	offset, err := s.blockLog.Append(data)
	if err != nil {
		return 0, err
	}

	// Update block index (stores globMin/globMax for historical access)
	s.blockIndex.Add(BlockIndexEntry{
		BlockNum: blockNum,
		Offset:   offset,
		Size:     uint32(len(data)),
		GlobMin:  globMin,
		GlobMax:  globMax,
	})

	// Update end block
	s.EndBlock = blockNum

	// In live mode, write indexes to disk immediately so readers can access new data
	// This ensures corereader can find globs in the active slice
	if liveMode {
		if err := s.saveIndices(); err != nil {
			return 0, fmt.Errorf("failed to save indices in live mode: %w", err)
		}
	}

	return offset, nil
}

// GetBlock retrieves a block from this slice
// Note: Decompression is handled by the caller (SliceStore.GetBlock)
// This returns the raw data (which may be compressed) from storage
func (s *Slice) GetBlock(blockNum uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.reader == nil {
		return nil, fmt.Errorf("slice %d reader not available", s.SliceNum)
	}

	entry, err := s.blockIndex.Get(blockNum)
	if err != nil {
		return nil, err
	}

	// Return raw data (compressed or uncompressed)
	// Caller is responsible for decompression if needed
	return s.reader.ReadAt(entry.Offset)
}

// GetBlockByGlob finds block number for a global sequence using binary search on blockIndex
func (s *Slice) GetBlockByGlob(glob uint64) (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.blockIndex.GetAllEntries()
	if len(entries) == 0 {
		return 0, ErrNotFound
	}

	// Binary search: find the block where GlobMax >= glob
	// Since globs are contiguous across blocks, this finds the right block
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].GlobMax >= glob
	})

	if i < len(entries) && glob >= entries[i].GlobMin && glob <= entries[i].GlobMax {
		return entries[i].BlockNum, nil
	}

	return 0, ErrNotFound
}

// ContainsBlock returns true if this slice contains the given block
func (s *Slice) ContainsBlock(blockNum uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := s.blockIndex.Get(blockNum)
	return err == nil
}

// ContainsGlob returns true if this slice contains the given global sequence
func (s *Slice) ContainsGlob(glob uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.blockIndex.GetAllEntries()
	if len(entries) == 0 {
		return false
	}

	// Quick range check using first and last block
	if glob < entries[0].GlobMin || glob > entries[len(entries)-1].GlobMax {
		return false
	}

	// Binary search for exact match
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].GlobMax >= glob
	})

	return i < len(entries) && glob >= entries[i].GlobMin && glob <= entries[i].GlobMax
}

// GetGlobMin returns the minimum global sequence in this slice
func (s *Slice) GetGlobMin() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.blockIndex.GetAllEntries()
	if len(entries) == 0 {
		return 0
	}

	minGlob := entries[0].GlobMin
	for _, e := range entries {
		if e.GlobMin < minGlob {
			minGlob = e.GlobMin
		}
	}
	return minGlob
}

// GetGlobMax returns the maximum global sequence in this slice
func (s *Slice) GetGlobMax() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.blockIndex.GetAllEntries()
	if len(entries) == 0 {
		return 0
	}

	maxGlob := entries[0].GlobMax
	for _, e := range entries {
		if e.GlobMax > maxGlob {
			maxGlob = e.GlobMax
		}
	}
	return maxGlob
}

// validateCompleteness checks that slice has all expected blocks for finalization
func (s *Slice) validateCompleteness() error {
	if s.header == nil {
		return nil
	}

	expectedCount := int(s.header.BlocksPerSlice)
	actualCount := s.blockIndex.Count()

	minBlock, maxBlock := s.blockIndex.Range()

	// Special case: slice 0 starting at block 2 (block 1 doesn't exist in trace files)
	if s.header.StartBlock == 1 && minBlock == 2 {
		expectedCount = expectedCount - 1
	}

	if actualCount != expectedCount {
		return fmt.Errorf("block count mismatch: expected %d, got %d",
			expectedCount, actualCount)
	}

	if minBlock != s.header.StartBlock && !(s.header.StartBlock == 1 && minBlock == 2) {
		return fmt.Errorf("start block mismatch: expected %d, got %d",
			s.header.StartBlock, minBlock)
	}
	if maxBlock != s.header.EndBlock {
		return fmt.Errorf("end block mismatch: expected %d, got %d",
			s.header.EndBlock, maxBlock)
	}

	entries := s.blockIndex.GetAllEntries()
	for i, entry := range entries {
		expectedBlockNum := minBlock + uint32(i)
		if entry.BlockNum != expectedBlockNum {
			return fmt.Errorf("gap detected: expected block %d at index %d, got %d",
				expectedBlockNum, i, entry.BlockNum)
		}
	}

	return nil
}

// writeHashFile computes SHA256 of data.log and writes sha256.txt
func (s *Slice) writeHashFile() error {
	dataLogPath := filepath.Join(s.basePath, "data.log")
	hashPath := filepath.Join(s.basePath, "sha256.txt")

	hash, err := ComputeFileHash(dataLogPath)
	if err != nil {
		return err
	}

	return os.WriteFile(hashPath, []byte(hash+"\n"), 0644)
}

// Finalize marks the slice as complete and optimizes indices
// If skipHash is true, the SHA256 hash computation is skipped (can be done in background)
func (s *Slice) Finalize() error {
	_, err := s.FinalizeWithOptions(false)
	return err
}

// FinalizeTiming holds timing breakdown from FinalizeWithOptions
type FinalizeTiming struct {
	ValidSort time.Duration
	SaveIdx   time.Duration
	SyncClose time.Duration
	Flag      time.Duration
	Reopen    time.Duration
	Total     time.Duration
}

// FinalizeWithOptions marks the slice as complete with configurable options
func (s *Slice) FinalizeWithOptions(skipHash bool) (FinalizeTiming, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var timing FinalizeTiming
	var t0, t1, t2, t3, t4, t5 time.Time
	t0 = time.Now()

	if err := s.validateCompleteness(); err != nil {
		return timing, fmt.Errorf("slice validation failed: %w", err)
	}

	t1 = time.Now()

	// Save indices without fsync - we'll fsync data.log which is more critical
	// Index files can be regenerated from data.log if corrupted
	if err := s.saveIndicesWithSync(false); err != nil {
		return timing, err
	}
	t2 = time.Now()

	if s.blockLog != nil {
		if err := s.blockLog.Sync(); err != nil {
			return timing, err
		}
		if err := s.blockLog.Close(); err != nil {
			return timing, err
		}
		s.blockLog = nil
	}
	t3 = time.Now()

	if s.header != nil {
		dataLogPath := filepath.Join(s.basePath, "data.log")
		if err := UpdateFinalizedFlag(dataLogPath, true); err != nil {
			return timing, fmt.Errorf("failed to update finalized flag: %w", err)
		}

		if !skipHash {
			if err := s.writeHashFile(); err != nil {
				return timing, fmt.Errorf("failed to write hash file: %w", err)
			}
		}
	}
	t4 = time.Now()

	if s.reader != nil {
		s.reader.Close()
		s.reader = nil
	}
	dataLogPath := filepath.Join(s.basePath, "data.log")
	reader, err := NewReader(dataLogPath)
	if err != nil && !os.IsNotExist(err) {
		return timing, fmt.Errorf("failed to reopen reader after finalize: %w", err)
	}
	s.reader = reader
	t5 = time.Now()

	timing = FinalizeTiming{
		ValidSort: t1.Sub(t0),
		SaveIdx:   t2.Sub(t1),
		SyncClose: t3.Sub(t2),
		Flag:      t4.Sub(t3),
		Reopen:    t5.Sub(t4),
		Total:     t5.Sub(t0),
	}

	s.Finalized = true
	return timing, nil
}

// FlushLog flushes the block log buffer to disk (fast, no fsync or index save)
// Use this during bulk writes when you want to minimize latency between batches
func (s *Slice) FlushLog() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.blockLog != nil {
		return s.blockLog.Flush()
	}
	return nil
}

// Sync syncs data to disk
func (s *Slice) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.blockLog != nil {
		if err := s.blockLog.Sync(); err != nil {
			return err
		}
	}

	if err := s.saveIndices(); err != nil {
		return err
	}

	// Reopen reader to pick up synced data
	// The reader caches file size at open time, so we need to refresh it
	if s.reader != nil {
		s.reader.Close()
		s.reader = nil
	}
	dataLogPath := filepath.Join(s.basePath, "data.log")
	reader, err := NewReader(dataLogPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to reopen reader after sync: %w", err)
	}
	s.reader = reader

	return nil
}

// saveIndices saves indices to disk (with fsync)
func (s *Slice) saveIndices() error {
	return s.saveIndicesWithSync(true)
}

// saveIndicesWithSync saves indices to disk with optional fsync
func (s *Slice) saveIndicesWithSync(doSync bool) error {
	t0 := time.Now()

	blockIdxPath := filepath.Join(s.basePath, "blocks.index")
	if err := s.blockIndex.WriteToWithSync(blockIdxPath, doSync); err != nil {
		return fmt.Errorf("failed to save block index: %w", err)
	}
	t1 := time.Now()

	totalTime := t1.Sub(t0)
	if totalTime > 50*time.Millisecond {
		logger.Printf("debug-timing", "saveIndices: blockIdx=%dms (%d entries, %d KB) total=%dms sync=%v",
			t1.Sub(t0).Milliseconds(), s.blockIndex.Count(), s.blockIndex.Count()*32/1024,
			totalTime.Milliseconds(), doSync)
	}

	return nil
}

// SaveIndices exports saveIndices for external use (e.g., slice recreation)
func (s *Slice) SaveIndices() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveIndices()
}

// Close closes slice components
func (s *Slice) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only save indices if not finalized (finalized slices already have indices saved)
	if !s.Finalized {
		if err := s.saveIndices(); err != nil {
			return err
		}
	}

	if s.blockLog != nil {
		if err := s.blockLog.Close(); err != nil {
			return err
		}
		s.blockLog = nil
	}

	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			return err
		}
		s.reader = nil
	}

	s.loaded = false
	return nil
}

// GetBlockIndex returns the block index for direct access
func (s *Slice) GetBlockIndex() *BlockIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blockIndex
}

// BuildGlobIndex builds the glob index from the block index metadata (in-memory only)
func (s *Slice) BuildGlobIndex() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.blockIndex.GetAllEntries()
	totalGlobs := 0

	for _, entry := range entries {
		totalGlobs += int(entry.GlobMax - entry.GlobMin + 1)
	}

	return totalGlobs, nil
}

// ErrEmptySlice indicates a slice has no blocks (not an error, just skip it)
var ErrEmptySlice = fmt.Errorf("slice has no blocks")

// BuildGlobIndexFromPath validates block index and returns glob count (deprecated - glob index no longer needed)
func BuildGlobIndexFromPath(slicePath string, sync bool) (int, error) {
	blockIdxPath := filepath.Join(slicePath, "blocks.index")

	if _, err := os.Stat(blockIdxPath); os.IsNotExist(err) {
		return 0, ErrEmptySlice
	}

	blockIndex, err := LoadBlockIndex(blockIdxPath)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrEmptySlice, err)
	}

	entries := blockIndex.GetAllEntries()
	if len(entries) == 0 {
		return 0, ErrEmptySlice
	}

	totalGlobs := 0
	for _, entry := range entries {
		totalGlobs += int(entry.GlobMax - entry.GlobMin + 1)
	}

	return totalGlobs, nil
}

// RebuildBlockIndexFromDataLog scans data.log and rebuilds blocks.index
func RebuildBlockIndexFromDataLog(slicePath string) (int, error) {
	dataLogPath := filepath.Join(slicePath, "data.log")
	blockIdxPath := filepath.Join(slicePath, "blocks.index")

	// Check if data.log exists
	if _, err := os.Stat(dataLogPath); os.IsNotExist(err) {
		return 0, fmt.Errorf("data.log not found")
	}

	// Check for header
	hasHeader, _ := HasHeader(dataLogPath)

	var reader *Reader
	var err error
	if hasHeader {
		reader, err = NewReaderWithHeader(dataLogPath)
	} else {
		reader, err = NewReader(dataLogPath)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to open data.log: %w", err)
	}
	defer reader.Close()

	// Create new block index
	newBlockIndex := NewBlockIndex()

	// Determine start offset (after header if present)
	startOffset := uint64(0)
	if hasHeader {
		startOffset = HeaderSize
	}

	// Scan data.log and parse each block's metadata
	blockCount := 0
	scanErr := reader.ScanFromOffset(startOffset, func(offset uint64, data []byte) error {
		// Decompress if needed
		decompressed, err := decompressData(data)
		if err != nil {
			return fmt.Errorf("failed to decompress block at offset %d: %w", offset, err)
		}

		// Parse block metadata
		meta := parseBlockMetadata(decompressed)
		if meta == nil {
			return fmt.Errorf("failed to parse block metadata at offset %d", offset)
		}

		// Add to block index
		newBlockIndex.Add(BlockIndexEntry{
			BlockNum: meta.BlockNum,
			Offset:   offset,
			Size:     uint32(len(data)), // Store compressed size
			GlobMin:  meta.MinGlobInBlock,
			GlobMax:  meta.MaxGlobInBlock,
		})

		blockCount++
		return nil
	})

	if scanErr != nil {
		return blockCount, fmt.Errorf("scan failed after %d blocks: %w", blockCount, scanErr)
	}

	if blockCount == 0 {
		return 0, fmt.Errorf("no blocks found in data.log")
	}

	// Write rebuilt block index
	if err := newBlockIndex.WriteTo(blockIdxPath); err != nil {
		return blockCount, fmt.Errorf("failed to write blocks.index: %w", err)
	}

	return blockCount, nil
}

// RepairSliceIndexes rebuilds blocks.index from data.log and validates glob coverage
func RepairSliceIndexes(slicePath string) (int, int, error) {
	sliceName := filepath.Base(slicePath)
	var startBlock, maxBlock uint32
	_, err := fmt.Sscanf(sliceName, "history_%010d-%010d", &startBlock, &maxBlock)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse slice name %q: %w", sliceName, err)
	}

	// First scan data.log to check if blocks are in correct range
	dataLogPath := filepath.Join(slicePath, "data.log")
	if _, err := os.Stat(dataLogPath); os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("data.log not found")
	}

	hasHeader, _ := HasHeader(dataLogPath)
	var reader *Reader
	if hasHeader {
		reader, err = NewReaderWithHeader(dataLogPath)
	} else {
		reader, err = NewReader(dataLogPath)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open data.log: %w", err)
	}
	defer reader.Close()

	startOffset := uint64(0)
	if hasHeader {
		startOffset = HeaderSize
	}

	// Scan to detect corruption
	totalBlocks := 0
	validBlocks := 0
	wrongBlocks := 0
	var wrongBlockNums []uint32

	scanErr := reader.ScanFromOffset(startOffset, func(offset uint64, data []byte) error {
		decompressed, err := decompressData(data)
		if err != nil {
			return fmt.Errorf("failed to decompress block at offset %d: %w", offset, err)
		}

		meta := parseBlockMetadata(decompressed)
		if meta == nil {
			return fmt.Errorf("failed to parse block metadata at offset %d", offset)
		}

		totalBlocks++

		// Check if block belongs in this slice
		if meta.BlockNum >= startBlock && meta.BlockNum <= maxBlock {
			validBlocks++
		} else {
			wrongBlocks++
			if len(wrongBlockNums) < 10 {
				wrongBlockNums = append(wrongBlockNums, meta.BlockNum)
			}
		}

		return nil
	})

	if scanErr != nil {
		return 0, 0, fmt.Errorf("scan failed: %w", scanErr)
	}

	// Log corruption detection results
	if wrongBlocks > 0 {
		logger.Warning("Slice %s corruption detected:", sliceName)
		logger.Warning("  Expected blocks: %d-%d", startBlock, maxBlock)
		logger.Warning("  Total blocks in data.log: %d", totalBlocks)
		logger.Warning("  Valid blocks: %d", validBlocks)
		logger.Warning("  Wrong blocks: %d (first 10: %v)", wrongBlocks, wrongBlockNums)

		if validBlocks == 0 {
			// Slice is completely corrupt - delete it
			logger.Warning("  No valid blocks found - deleting slice entirely")
			if err := os.RemoveAll(slicePath); err != nil {
				return 0, 0, fmt.Errorf("failed to delete corrupted slice: %w", err)
			}
			return 0, 0, fmt.Errorf("slice completely corrupted and deleted - resync from trace files")
		}

		// Partial corruption - rebuild with only valid blocks
		logger.Warning("  Rebuilding slice with only valid blocks...")
		return rebuildSliceFiltered(slicePath, startBlock, maxBlock)
	}

	// No corruption - just rebuild indexes normally
	blockCount, err := RebuildBlockIndexFromDataLog(slicePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to rebuild block index: %w", err)
	}

	globCount, err := BuildGlobIndexFromPath(slicePath, true)
	if err != nil {
		if errors.Is(err, ErrEmptySlice) {
			return blockCount, 0, nil
		}
		return blockCount, 0, fmt.Errorf("failed to rebuild glob index: %w", err)
	}

	return blockCount, globCount, nil
}

// rebuildSliceFiltered rebuilds a slice keeping only blocks in the correct range
func rebuildSliceFiltered(slicePath string, startBlock, maxBlock uint32) (int, int, error) {
	dataLogPath := filepath.Join(slicePath, "data.log")
	newDataLogPath := dataLogPath + ".new"

	// Open corrupt data.log for reading
	hasHeader, _ := HasHeader(dataLogPath)
	var reader *Reader
	var err error
	if hasHeader {
		reader, err = NewReaderWithHeader(dataLogPath)
	} else {
		reader, err = NewReader(dataLogPath)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open corrupt data.log: %w", err)
	}
	defer reader.Close()

	// Create new data.log with header
	newFile, err := os.Create(newDataLogPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create new data.log: %w", err)
	}
	defer newFile.Close()

	// Write header
	header := NewDataLogHeader(maxBlock-startBlock+1, startBlock, maxBlock)
	headerBytes := header.Bytes()
	if _, err := newFile.Write(headerBytes); err != nil {
		os.Remove(newDataLogPath)
		return 0, 0, fmt.Errorf("failed to write header: %w", err)
	}

	// Create new block index
	newBlockIndex := NewBlockIndex()
	validBlockCount := 0
	currentOffset := uint64(HeaderSize)

	startOffset := uint64(0)
	if hasHeader {
		startOffset = HeaderSize
	}

	// Copy only valid blocks
	scanErr := reader.ScanFromOffset(startOffset, func(offset uint64, data []byte) error {
		decompressed, err := decompressData(data)
		if err != nil {
			return fmt.Errorf("failed to decompress: %w", err)
		}

		meta := parseBlockMetadata(decompressed)
		if meta == nil {
			return fmt.Errorf("failed to parse metadata")
		}

		// Only copy blocks in correct range
		if meta.BlockNum >= startBlock && meta.BlockNum <= maxBlock {
			// Write block data
			if _, err := newFile.Write(data); err != nil {
				return fmt.Errorf("failed to write block: %w", err)
			}

			// Add to block index
			newBlockIndex.Add(BlockIndexEntry{
				BlockNum: meta.BlockNum,
				Offset:   currentOffset,
				Size:     uint32(len(data)),
				GlobMin:  meta.MinGlobInBlock,
				GlobMax:  meta.MaxGlobInBlock,
			})

			currentOffset += uint64(len(data))
			validBlockCount++
		}

		return nil
	})

	if scanErr != nil {
		os.Remove(newDataLogPath)
		return 0, 0, fmt.Errorf("rebuild scan failed: %w", scanErr)
	}

	// Sync new data.log
	if err := newFile.Sync(); err != nil {
		os.Remove(newDataLogPath)
		return 0, 0, fmt.Errorf("failed to sync new data.log: %w", err)
	}
	newFile.Close()
	reader.Close()

	// Delete corrupt data.log and replace with new one
	if err := os.Remove(dataLogPath); err != nil {
		os.Remove(newDataLogPath)
		return 0, 0, fmt.Errorf("failed to remove corrupt data.log: %w", err)
	}

	if err := os.Rename(newDataLogPath, dataLogPath); err != nil {
		return 0, 0, fmt.Errorf("failed to replace data.log: %w", err)
	}

	// Write block index
	blockIdxPath := filepath.Join(slicePath, "blocks.index")
	if err := newBlockIndex.WriteTo(blockIdxPath); err != nil {
		return 0, 0, fmt.Errorf("failed to write blocks.index: %w", err)
	}

	// Build glob index
	globCount, err := BuildGlobIndexFromPath(slicePath, true)
	if err != nil {
		if errors.Is(err, ErrEmptySlice) {
			return validBlockCount, 0, nil
		}
		return validBlockCount, 0, fmt.Errorf("failed to rebuild glob index: %w", err)
	}

	logger.Printf("validation", "✓ Slice rebuilt: %d valid blocks, %d glob entries", validBlockCount, globCount)

	return validBlockCount, globCount, nil
}

// SortedSliceInfos returns slice infos sorted by slice number
func (sm *SliceManager) SortedSliceInfos() []SliceInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	infos := make([]SliceInfo, len(sm.sliceInfos))
	copy(infos, sm.sliceInfos)

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].SliceNum < infos[j].SliceNum
	})

	return infos
}

// hashWorker runs in the background computing SHA256 hashes for finalized slices
func (sm *SliceManager) hashWorker() {
	for {
		select {
		case <-sm.hashDone:
			return
		case slicePath := <-sm.hashQueue:
			sm.computeAndWriteHash(slicePath)
		}
	}
}

// computeAndWriteHash computes SHA256 of data.log and writes sha256.txt
func (sm *SliceManager) computeAndWriteHash(slicePath string) {
	dataLogPath := filepath.Join(slicePath, "data.log")
	hashPath := filepath.Join(slicePath, "sha256.txt")

	hash, err := ComputeFileHash(dataLogPath)
	if err != nil {
		logger.Printf("warning", "Failed to compute hash for %s: %v", filepath.Base(slicePath), err)
		return
	}

	if err := os.WriteFile(hashPath, []byte(hash+"\n"), 0644); err != nil {
		logger.Printf("warning", "Failed to write hash file for %s: %v", filepath.Base(slicePath), err)
		return
	}
}

// QueueHashComputation queues a slice path for background hash computation
func (sm *SliceManager) QueueHashComputation(slicePath string) {
	select {
	case sm.hashQueue <- slicePath:
	default:
		// Queue full, skip (hash can be computed later on demand)
		logger.Printf("warning", "Hash queue full, skipping %s", filepath.Base(slicePath))
	}
}

// StopHashWorker stops the background hash worker
func (sm *SliceManager) StopHashWorker() {
	close(sm.hashDone)
}
