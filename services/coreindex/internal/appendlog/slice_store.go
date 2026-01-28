package appendlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

// SliceStore manages append-only log files with tiered slice-based storage
// This is the production implementation for large datasets (400M+ blocks)
// Memory usage stays bounded regardless of dataset size.
type SliceStore struct {
	basePath string

	// Slice manager handles all slice operations
	sliceManager *SliceManager

	// Write-through cache for recently written blocks
	// Key: blockNum, Value: compressed block blob
	blockCache     map[uint32][]byte
	blockCacheMu   sync.RWMutex
	blockCacheSize int

	// Cache statistics (atomic for lock-free reads)
	cacheHits   uint64
	cacheMisses uint64

	// Compression settings
	enableZstd bool
	zstdLevel  int

	// Aggregated timing stats for periodic logging
	batchStats         batchStatsAccum
	batchStatsLastLog  time.Time
	batchStatsInterval time.Duration

	// Metadata
	lib   uint32
	head  uint32
	debug bool
	mu    sync.RWMutex
}

// batchStatsAccum accumulates AppendBlockBatch timing for periodic logging
type batchStatsAccum struct {
	count         int
	totalBlocks   int
	totalTime     time.Duration
	totalLockWait time.Duration
	totalAppend   time.Duration
	totalRotation time.Duration
	rotationCount int
	maxTime       time.Duration
}

// SliceStoreOptions configures the slice store
type SliceStoreOptions struct {
	Debug            bool
	BlocksPerSlice   uint32 // Blocks per slice (default: 1M)
	MaxCachedSlices  int    // Max slices to keep in memory (default: 2)
	BlockCacheSize   int    // Block cache size (default: 500)
	EnableZstd       bool   // Enable zstd compression (default: false)
	ZstdLevel        int    // Zstd compression level (default: 3)
	LogSliceInterval uint32 // Log slice creation every N slices (0 = only in debug mode)
}

// NewSliceStore creates or opens a slice store
func NewSliceStore(basePath string, opts SliceStoreOptions) (*SliceStore, error) {
	startTotal := time.Now()
	logger.Printf("debug-startup", "NewSliceStore starting for %s...", basePath)

	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	if opts.BlocksPerSlice == 0 {
		opts.BlocksPerSlice = DefaultBlocksPerSlice
	}
	if opts.MaxCachedSlices == 0 {
		opts.MaxCachedSlices = 2
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 500
	}
	if opts.ZstdLevel == 0 {
		opts.ZstdLevel = 3
	}

	s := &SliceStore{
		basePath:           basePath,
		blockCache:         make(map[uint32][]byte),
		blockCacheSize:     opts.BlockCacheSize,
		enableZstd:         opts.EnableZstd,
		zstdLevel:          opts.ZstdLevel,
		debug:              opts.Debug,
		batchStatsLastLog:  time.Now(),
		batchStatsInterval: 10 * time.Second,
	}

	// Create slice manager
	t0 := time.Now()
	sliceManager, err := NewSliceManager(basePath, opts.BlocksPerSlice, opts.MaxCachedSlices, opts.Debug, opts.LogSliceInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create slice manager: %w", err)
	}
	s.sliceManager = sliceManager
	logger.Printf("debug-startup", "  SliceManager created in %.2fs", time.Since(t0).Seconds())

	t1 := time.Now()
	sliceInfos := sliceManager.GetSliceInfos()
	var sliceBasedLIB uint32
	if len(sliceInfos) > 0 {
		for i := len(sliceInfos) - 1; i >= 0; i-- {
			if sliceInfos[i].Finalized && sliceInfos[i].EndBlock > 0 {
				sliceBasedLIB = sliceInfos[i].EndBlock
				break
			}
		}
		// Fallback to last slice if no finalized slices found
		if sliceBasedLIB == 0 && len(sliceInfos) > 0 {
			sliceBasedLIB = sliceInfos[len(sliceInfos)-1].EndBlock
		}
	}
	logger.Printf("debug-startup", "  Calculated sliceBasedLIB=%d from %d slices in %.2fs", sliceBasedLIB, len(sliceInfos), time.Since(t1).Seconds())

	t2 := time.Now()
	stateErr := s.loadState()
	if stateErr != nil {
		// State file missing/corrupt - use slice metadata
		if sliceBasedLIB > 0 {
			s.lib = sliceBasedLIB
			s.head = sliceBasedLIB
			logger.Printf("store", "State file missing, calculated LIB=%d from slice metadata", s.lib)
		} else {
			s.lib = 0
			s.head = 0
		}
	} else {
		// State loaded successfully - but verify it's not stale/corrupted
		if opts.Debug {
			logger.Printf("debug", "Loaded state - LIB=%d, HEAD=%d", s.lib, s.head)
		}

		// If state LIB is significantly behind slice data, use slice data
		// This handles cases where state.json is corrupted/stale
		if sliceBasedLIB > s.lib+10000 { // More than 10K blocks behind = suspicious
			logger.Warning("State file LIB=%d is significantly behind slice data LIB=%d, using slice data", s.lib, sliceBasedLIB)
			s.lib = sliceBasedLIB
			s.head = sliceBasedLIB
		}
	}
	logger.Printf("debug-startup", "  Loaded state (LIB=%d) in %.2fs", s.lib, time.Since(t2).Seconds())

	// NOTE: Crash recovery is now handled by StartupValidation() called from main.go
	// This consolidates all validation into a single pass after store initialization.

	// Log slice configuration
	logger.Printf("store", "Initialized: %d blocks per slice, %d max cached slices",
		opts.BlocksPerSlice, opts.MaxCachedSlices)

	if len(sliceInfos) > 0 {
		// Count active vs finalized slices
		finalized := 0
		active := 0
		var firstBlock, lastBlock uint32

		for _, info := range sliceInfos {
			if info.Finalized {
				finalized++
			} else {
				active++
			}
			if firstBlock == 0 || info.StartBlock < firstBlock {
				firstBlock = info.StartBlock
			}
			if info.EndBlock > lastBlock {
				lastBlock = info.EndBlock
			}
		}

		logger.Printf("store", "Loaded %d slices (blocks %d-%d): %d finalized, %d active",
			len(sliceInfos), firstBlock, lastBlock, finalized, active)
	}

	logger.Printf("debug-startup", "NewSliceStore completed in %.2fs", time.Since(startTotal).Seconds())
	return s, nil
}

// AppendBlock writes a block and optionally syncs indexes to disk
// If liveMode is true, indexes are written immediately for real-time reader access
func (s *SliceStore) AppendBlock(blockNum uint32, data []byte, globMin, globMax uint64, liveMode bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Compress data if enabled
	dataToWrite := data
	if s.enableZstd {
		compressed, err := compressData(data, s.zstdLevel)
		if err != nil {
			return fmt.Errorf("compression failed: %w", err)
		}
		dataToWrite = compressed
	}

	// Check if we need to rotate to a new slice
	if s.sliceManager.ShouldRotate(blockNum) {
		if err := s.sliceManager.RotateSlice(); err != nil {
			return fmt.Errorf("failed to rotate slice: %w", err)
		}
	}

	// Get active slice (pass blockNum as hint for initial slice calculation)
	slice, err := s.sliceManager.GetActiveSliceFor(blockNum)
	if err != nil {
		return fmt.Errorf("failed to get active slice: %w", err)
	}

	// Append to slice (liveMode controls whether indexes are written to disk)
	_, err = slice.AppendBlock(blockNum, dataToWrite, globMin, globMax, liveMode)
	if err != nil {
		return fmt.Errorf("failed to append block: %w", err)
	}

	// Cache the block (write-through cache with compressed data)
	s.cacheBlock(blockNum, dataToWrite)

	// Update metadata
	s.lib = blockNum
	s.head = blockNum

	return nil
}

// BuildGlobIndex builds glob indices for all slices from block metadata
func (s *SliceStore) BuildGlobIndex() (int, error) {
	return s.BuildGlobIndexParallelWithRepair(8, false) // Default to 8 workers, no repair
}

// BuildGlobIndexParallel builds glob indices for all slices using parallel workers
// Each slice's glob index is built independently from its block index metadata
func (s *SliceStore) BuildGlobIndexParallel(numWorkers int) (int, error) {
	return s.BuildGlobIndexParallelWithRepair(numWorkers, false)
}

// BuildGlobIndexParallelWithRepair builds glob indices with optional repair of invalid slices
// If repair=true, slices with invalid block indexes will be rebuilt from data.log
func (s *SliceStore) BuildGlobIndexParallelWithRepair(numWorkers int, repair bool) (int, error) {
	s.mu.Lock()
	sliceInfos := s.sliceManager.SortedSliceInfos()
	s.mu.Unlock()

	if len(sliceInfos) == 0 {
		return 0, nil
	}

	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers > len(sliceInfos) {
		numWorkers = len(sliceInfos)
	}

	if repair {
		logger.Printf("validation", "Building glob index for %d slices using %d workers (with repair)...", len(sliceInfos), numWorkers)
	} else {
		logger.Printf("validation", "Building glob index for %d slices using %d workers...", len(sliceInfos), numWorkers)
	}
	startTime := time.Now()

	// Channel for slice info to process
	type sliceWork struct {
		info SliceInfo
		path string
	}
	sliceChan := make(chan sliceWork, len(sliceInfos))
	for _, info := range sliceInfos {
		// Construct slice path directly (avoids loading slice into cache)
		slicePath := filepath.Join(s.basePath, fmt.Sprintf("history_%010d-%010d", info.StartBlock, info.MaxBlock))
		sliceChan <- sliceWork{info: info, path: slicePath}
	}
	close(sliceChan)

	// Results channel
	type result struct {
		sliceNum   uint32
		count      int
		err        error
		skipped    bool // true if slice was empty/invalid and not repaired
		repaired   bool // true if slice was repaired
		blockCount int  // blocks found during repair
	}
	resultChan := make(chan result, len(sliceInfos))

	// Progress tracking
	var processed atomic.Int64
	totalSlices := int64(len(sliceInfos))

	// Start workers - each worker builds glob index directly from files (no slice loading)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range sliceChan {
				// Build glob index directly from files (no slice cache involvement)
				// Skip fsync for bulk rebuild - we'll sync once at the end
				count, err := BuildGlobIndexFromPath(work.path, false)

				// Check if this was a skippable error (empty/invalid slice)
				skipped := false
				repaired := false
				blockCount := 0
				if err != nil && errors.Is(err, ErrEmptySlice) {
					if repair {
						// Try to repair by rebuilding from data.log
						bc, gc, repairErr := RepairSliceIndexes(work.path)
						if repairErr == nil {
							repaired = true
							blockCount = bc
							count = gc
							err = nil
						} else {
							// Repair failed - still skip but log the repair error
							skipped = true
							err = nil
						}
					} else {
						skipped = true
						err = nil // Don't treat as fatal error
					}
				}

				resultChan <- result{
					sliceNum:   work.info.SliceNum,
					count:      count,
					err:        err,
					skipped:    skipped,
					repaired:   repaired,
					blockCount: blockCount,
				}
				processed.Add(1)
			}
		}()
	}

	// Progress reporter goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := processed.Load()
				elapsed := time.Since(startTime).Seconds()
				progress := float64(p) / float64(totalSlices) * 100
				rate := float64(p) / elapsed
				remaining := totalSlices - p
				eta := float64(remaining) / rate
				logger.Printf("validation", "  Glob index progress: %d/%d slices (%.1f%%), %.0f slices/sec, ETA: %.0fs",
					p, totalSlices, progress, rate, eta)
			case <-done:
				return
			}
		}
	}()

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	totalGlobs := 0
	var skippedSliceNums []uint32
	var repairedSlices []uint32
	totalBlocksRepaired := 0
	var firstErr error
	for r := range resultChan {
		if r.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to build glob index for slice %d: %w", r.sliceNum, r.err)
		}
		if r.skipped {
			skippedSliceNums = append(skippedSliceNums, r.sliceNum)
		}
		if r.repaired {
			repairedSlices = append(repairedSlices, r.sliceNum)
			totalBlocksRepaired += r.blockCount
		}
		totalGlobs += r.count
	}

	close(done) // Stop progress reporter

	if firstErr != nil {
		return totalGlobs, firstErr
	}

	elapsed := time.Since(startTime).Seconds()

	// Log repair results
	if len(repairedSlices) > 0 {
		if len(repairedSlices) <= 10 {
			logger.Printf("validation", "✓ Repaired %d slices from data.log: %v (%d blocks recovered)",
				len(repairedSlices), repairedSlices, totalBlocksRepaired)
		} else {
			logger.Printf("validation", "✓ Repaired %d slices from data.log (first 10: %v...) (%d blocks recovered)",
				len(repairedSlices), repairedSlices[:10], totalBlocksRepaired)
		}
	}

	// Log skipped slices (only if not in repair mode, or repair failed)
	if len(skippedSliceNums) > 0 {
		if len(skippedSliceNums) <= 10 {
			logger.Warning("Skipped %d slices with empty/invalid block index: %v", len(skippedSliceNums), skippedSliceNums)
		} else {
			logger.Warning("Skipped %d slices with empty/invalid block index (first 10: %v...)", len(skippedSliceNums), skippedSliceNums[:10])
		}
		if !repair {
			logger.Warning("Use --repair-all to repair all invalid slices, or repair them individually:")
			for _, sliceNum := range skippedSliceNums {
				logger.Warning("  coreindex --repair-slice %d", sliceNum)
			}
		} else {
			logger.Warning("These slices could not be repaired (data.log may be missing or corrupted)")
		}
	}

	// Final summary
	processedSlices := len(sliceInfos) - len(skippedSliceNums)
	if len(repairedSlices) > 0 || len(skippedSliceNums) > 0 {
		logger.Printf("validation", "✓ Built glob index: %d entries across %d slices in %.2fs (%.0f slices/sec, %d repaired, %d skipped)",
			totalGlobs, processedSlices, elapsed, float64(len(sliceInfos))/elapsed, len(repairedSlices), len(skippedSliceNums))
	} else {
		logger.Printf("validation", "✓ Built glob index: %d entries across %d slices in %.2fs (%.0f slices/sec)",
			totalGlobs, len(sliceInfos), elapsed, float64(len(sliceInfos))/elapsed)
	}

	return totalGlobs, nil
}

// StartupValidation performs smart validation based on shutdown state and flags
func (s *SliceStore) StartupValidation(forceFullValidation bool, workers int) error {
	startTime := time.Now()

	// Check if we need crash recovery
	state, stateErr := s.loadStateFile()
	cleanShutdown := stateErr == nil && state.CleanShutdown

	if cleanShutdown && !forceFullValidation {
		logger.Println("validation", "Clean shutdown detected, skipping crash recovery")
		logger.Println("validation", "Use --verify flag to force full validation")

		if err := s.updateValidationMetadata("skip", 0); err != nil {
			logger.Warning("Failed to update validation metadata: %v", err)
		}

		logger.Printf("validation", "✓ Startup validation complete in %.2fs", time.Since(startTime).Seconds())
		return nil
	}

	// Unclean shutdown or forced - run full recovery
	if stateErr != nil {
		logger.Printf("validation", "State file error (%v), running crash recovery...", stateErr)
	} else if !state.CleanShutdown {
		logger.Println("validation", "Unclean shutdown detected, running crash recovery...")
	} else {
		logger.Println("validation", "Full validation requested (--verify flag)")
	}

	// Phase 1: Crash recovery - validate active slice and adjust LIB
	if err := s.recoverFromCrash(); err != nil {
		return fmt.Errorf("crash recovery failed: %w", err)
	}

	// Phase 2: Rebuild glob indices if needed
	if forceFullValidation || !cleanShutdown {
		logger.Printf("validation", "Building glob index with %d workers...", workers)
		globCount, err := s.BuildGlobIndexParallel(workers)
		if err != nil {
			return err
		}

		if err := s.updateValidationMetadata("full", globCount); err != nil {
			logger.Warning("Failed to update validation metadata: %v", err)
		}
	}

	logger.Printf("validation", "✓ Startup validation complete in %.2fs", time.Since(startTime).Seconds())
	return nil
}

// updateValidationMetadata updates the validation timestamp and method in state
func (s *SliceStore) updateValidationMetadata(method string, rebuildCount int) error {
	statePath := filepath.Join(s.basePath, "state.json")

	// Load existing state
	state, err := s.loadStateFile()
	if err != nil {
		// Create new state if doesn't exist
		state = &StateFile{
			LIB:        s.lib,
			Head:       s.head,
			Sliced:     true,
			SliceCount: len(s.sliceManager.GetSliceInfos()),
		}
	}

	// Update validation metadata
	state.LastValidated = time.Now()
	state.ValidationMethod = method
	state.ValidationCount++
	state.CleanShutdown = true // Mark as clean after successful validation

	// Log validation info
	switch method {
	case "full":
		logger.Printf("validation", "✓ Validation metadata updated: full rebuild complete (%d glob entries)", rebuildCount)
	case "skip":
		logger.Println("validation", "✓ Validation metadata updated: skipped (clean shutdown)")
	default:
		logger.Printf("validation", "✓ Validation metadata updated: %s validation complete", method)
	}

	// Use Marshal instead of MarshalIndent - 10x faster (profiling showed 20% CPU in appendIndent)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(statePath, data, 0644)
}

// MarkCleanShutdown marks the state as cleanly shut down
func (s *SliceStore) MarkCleanShutdown() error {
	statePath := filepath.Join(s.basePath, "state.json")

	// Load existing state
	state, err := s.loadStateFile()
	if err != nil {
		return fmt.Errorf("failed to load state for clean shutdown marker: %w", err)
	}

	// Mark clean shutdown
	state.CleanShutdown = true

	// Use Marshal instead of MarshalIndent - 10x faster (profiling showed 20% CPU in appendIndent)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(statePath, data, 0644)
}

// AppendBlockBatch writes multiple blocks without glob index updates or sync
// This is optimized for initial sync - call Flush()/Sync() periodically and BuildGlobIndex() after sync
func (s *SliceStore) AppendBlockBatch(blocks []BlockEntry) error {
	if len(blocks) == 0 {
		return nil
	}

	batchStart := time.Now()
	s.mu.Lock()
	lockWaitTime := time.Since(batchStart)
	defer s.mu.Unlock()

	// Process blocks sequentially, handling rotations as needed
	var currentSlice *Slice
	var err error
	var totalRotationTime time.Duration
	var rotationCount int

	for i := range blocks {
		block := &blocks[i]

		// Get active slice BEFORE checking rotation (needed to queue completed slice)
		// Pass block number as hint for initial slice calculation
		if currentSlice == nil {
			currentSlice, err = s.sliceManager.GetActiveSliceFor(block.BlockNum)
			if err != nil {
				return fmt.Errorf("failed to get slice for block %d: %w", block.BlockNum, err)
			}
		}

		// Use pre-compressed data if available, otherwise compress if enabled
		dataToWrite := block.Data
		if s.enableZstd && !block.PreCompressed {
			compressed, err := compressData(block.Data, s.zstdLevel)
			if err != nil {
				return fmt.Errorf("compression failed for block %d: %w", block.BlockNum, err)
			}
			dataToWrite = compressed
		}

		// Check if we need to rotate
		if s.sliceManager.ShouldRotate(block.BlockNum) {
			rotStart := time.Now()

			// Flush current slice before rotation (no full sync needed)
			if err := currentSlice.FlushLog(); err != nil {
				return fmt.Errorf("failed to flush slice before rotation: %w", err)
			}

			if err := s.sliceManager.RotateSlice(); err != nil {
				return fmt.Errorf("failed to rotate slice at block %d: %w", block.BlockNum, err)
			}

			// Get new active slice after rotation
			currentSlice, err = s.sliceManager.GetActiveSlice()
			if err != nil {
				return fmt.Errorf("failed to get new slice after rotation at block %d: %w", block.BlockNum, err)
			}

			rotationTime := time.Since(rotStart)
			totalRotationTime += rotationTime
			rotationCount++
		}

		// Append block to current slice (skips glob index building)
		// Batch mode is always bulk sync, so liveMode=false
		_, err := currentSlice.AppendBlock(block.BlockNum, dataToWrite, block.GlobMin, block.GlobMax, false)
		if err != nil {
			return fmt.Errorf("failed to append block %d: %w", block.BlockNum, err)
		}

		// Skip caching during batch writes (not needed for write-only path)
	}

	// Update metadata to last block in batch (no sync - caller handles that)
	lastBlock := &blocks[len(blocks)-1]
	s.lib = lastBlock.BlockNum
	s.head = lastBlock.BlockNum

	// Accumulate timing stats for periodic logging
	totalBatchTime := time.Since(batchStart)
	appendTime := totalBatchTime - totalRotationTime - lockWaitTime
	s.batchStats.count++
	s.batchStats.totalBlocks += len(blocks)
	s.batchStats.totalTime += totalBatchTime
	s.batchStats.totalLockWait += lockWaitTime
	s.batchStats.totalAppend += appendTime
	s.batchStats.totalRotation += totalRotationTime
	s.batchStats.rotationCount += rotationCount
	if totalBatchTime > s.batchStats.maxTime {
		s.batchStats.maxTime = totalBatchTime
	}

	// Log aggregated stats periodically
	s.logBatchStatsIfNeeded()

	return nil
}

func (s *SliceStore) logBatchStatsIfNeeded() {
	if s.batchStats.count == 0 || !s.debug {
		return
	}
	if time.Since(s.batchStatsLastLog) < s.batchStatsInterval {
		return
	}

	st := s.batchStats
	n := float64(st.count)

	logger.Printf("debug-timing", "AppendBlockBatch: %d batches, %d blocks, avg=%.2fms max=%.2fms | lock=%.2fms append=%.2fms rotate=%dx%.2fms",
		st.count, st.totalBlocks,
		float64(st.totalTime.Microseconds())/n/1000.0,
		float64(st.maxTime.Microseconds())/1000.0,
		float64(st.totalLockWait.Microseconds())/n/1000.0,
		float64(st.totalAppend.Microseconds())/n/1000.0,
		st.rotationCount,
		float64(st.totalRotation.Microseconds())/1000.0)

	s.batchStats = batchStatsAccum{}
	s.batchStatsLastLog = time.Now()
}

// cacheBlock adds a block to the write-through cache, evicting old blocks if needed
func (s *SliceStore) cacheBlock(blockNum uint32, data []byte) {
	s.blockCacheMu.Lock()
	defer s.blockCacheMu.Unlock()

	// Make a copy of data to avoid holding reference to caller's slice
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	s.blockCache[blockNum] = dataCopy

	// Evict oldest blocks if cache is full
	if len(s.blockCache) > s.blockCacheSize {
		minBlock := blockNum
		for bn := range s.blockCache {
			if bn < minBlock {
				minBlock = bn
			}
		}
		delete(s.blockCache, minBlock)
	}
}

// GetBlock retrieves a block by block number
func (s *SliceStore) GetBlock(blockNum uint32) ([]byte, error) {
	// Fast range check for sequential blockchain data
	// Block 1 doesn't exist (trace files start at block 2)
	if blockNum < 2 || blockNum > s.GetHead() {
		return nil, ErrNotFound
	}

	// Check cache first (fast path)
	s.blockCacheMu.RLock()
	if cached, found := s.blockCache[blockNum]; found {
		s.blockCacheMu.RUnlock()
		atomic.AddUint64(&s.cacheHits, 1)
		// Decompress if needed (cache stores compressed data)
		decompressed, err := decompressData(cached)
		if err != nil {
			return nil, fmt.Errorf("decompression failed: %w", err)
		}
		// Return a copy to prevent caller from modifying cache
		result := make([]byte, len(decompressed))
		copy(result, decompressed)
		return result, nil
	}
	s.blockCacheMu.RUnlock()

	atomic.AddUint64(&s.cacheMisses, 1)

	// Slow path: read from correct slice
	s.mu.RLock()
	defer s.mu.RUnlock()

	slice, err := s.sliceManager.GetSliceForBlock(blockNum)
	if err != nil {
		return nil, ErrNotFound
	}

	data, err := slice.GetBlock(blockNum)
	if err != nil {
		return nil, err
	}

	// Decompress if needed (auto-detects via magic bytes)
	return decompressData(data)
}

// GetCacheStats returns cache hit and miss counts
func (s *SliceStore) GetCacheStats() (hits, misses uint64) {
	return atomic.LoadUint64(&s.cacheHits), atomic.LoadUint64(&s.cacheMisses)
}

// GetCacheSize returns current number of cached blocks
func (s *SliceStore) GetCacheSize() int {
	s.blockCacheMu.RLock()
	defer s.blockCacheMu.RUnlock()
	return len(s.blockCache)
}

// GetBlockByGlob retrieves a block by global sequence
func (s *SliceStore) GetBlockByGlob(glob uint64) (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	slice, err := s.sliceManager.GetSliceForGlob(glob)
	if err != nil {
		return 0, ErrNotFound
	}

	return slice.GetBlockByGlob(glob)
}

// GetLIB returns last irreversible block number
func (s *SliceStore) GetLIB() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lib
}

// GetHead returns head block number
func (s *SliceStore) GetHead() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.head
}

// GetBlocksPerSlice returns the configured blocks per slice
func (s *SliceStore) GetBlocksPerSlice() uint32 {
	return s.sliceManager.blocksPerSlice
}

// Flush flushes buffered writes and fsyncs before saving state
func (s *SliceStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Sync slice manager (includes active slice)
	if err := s.sliceManager.Sync(); err != nil {
		return err
	}

	return s.saveState()
}

// Sync fsyncs all data to disk and saves indices
func (s *SliceStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.sliceManager.Sync(); err != nil {
		return err
	}

	return s.saveState()
}

// SaveActiveSliceIndices saves the active slice's block index to disk with fsync.
// Call this before broadcasting blocks to ensure queries can find them.
func (s *SliceStore) SaveActiveSliceIndices() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sliceManager.SaveActiveSliceIndices()
}

// Close closes the store and saves indices
func (s *SliceStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Glob index already built incrementally, no need to rebuild on close

	// Save state
	if err := s.saveState(); err != nil {
		return err
	}

	// Mark clean shutdown
	if err := s.MarkCleanShutdown(); err != nil {
		logger.Warning("	Failed to mark clean shutdown: %v", err)
	}

	// Close slice manager
	return s.sliceManager.Close()
}

// StateFile represents the persisted state metadata
type StateFile struct {
	LIB              uint32    `json:"lib"`
	Head             uint32    `json:"head"`
	Sliced           bool      `json:"sliced"`
	SliceCount       int       `json:"slice_count"`
	CleanShutdown    bool      `json:"clean_shutdown"`
	LastValidated    time.Time `json:"last_validated"`
	ValidationCount  int       `json:"validation_count"`
	ValidationMethod string    `json:"validation_method"` // "full", "quick", or "skip"
}

// saveState saves LIB/HEAD metadata
func (s *SliceStore) saveState() error {
	statePath := filepath.Join(s.basePath, "state.json")

	state := StateFile{
		LIB:              s.lib,
		Head:             s.head,
		Sliced:           true, // Mark as slice-based storage
		SliceCount:       len(s.sliceManager.GetSliceInfos()),
		CleanShutdown:    false,       // Will be set to true on graceful Close()
		LastValidated:    time.Time{}, // Will be updated by validation methods
		ValidationCount:  0,
		ValidationMethod: "",
	}

	// Preserve existing validation metadata if loading from disk
	// NOTE: CleanShutdown is NOT preserved - any saveState() clears it since we're actively writing.
	// Only MarkCleanShutdown() should set it to true (on graceful close).
	if existingState, err := s.loadStateFile(); err == nil {
		state.LastValidated = existingState.LastValidated
		state.ValidationCount = existingState.ValidationCount
		state.ValidationMethod = existingState.ValidationMethod
	}

	// Use Marshal instead of MarshalIndent - 10x faster (profiling showed 20% CPU in appendIndent)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(statePath, data, 0644)
}

// loadStateFile loads state file with full metadata
func (s *SliceStore) loadStateFile() (*StateFile, error) {
	statePath := filepath.Join(s.basePath, "state.json")

	data, err := os.ReadFile(statePath)
	if err != nil {
		return nil, err
	}

	var state StateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// loadState loads LIB/HEAD metadata (backward compatible)
func (s *SliceStore) loadState() error {
	state, err := s.loadStateFile()
	if err != nil {
		return err
	}

	s.lib = state.LIB
	s.head = state.Head

	return nil
}

// GetSliceInfos returns metadata about all slices (for status/debug)
func (s *SliceStore) GetSliceInfos() []SliceInfo {
	return s.sliceManager.GetSliceInfos()
}

// GetBasePath returns the base path of the store
func (s *SliceStore) GetBasePath() string {
	return s.basePath
}

// GetTotalBlocks returns total block count across all slices
func (s *SliceStore) GetTotalBlocks() uint32 {
	return s.sliceManager.GetTotalBlocks()
}

// GetSliceByNum returns a slice by its number (for admin operations)
func (s *SliceStore) GetSliceByNum(sliceNum uint32) (*Slice, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sliceManager.GetSliceByNum(sliceNum)
}

// DeleteSlice removes a slice directory (for admin operations like recreate)
func (s *SliceStore) DeleteSlice(sliceNum uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	slicePath := s.sliceManager.slicePath(sliceNum)
	if err := os.RemoveAll(slicePath); err != nil {
		return fmt.Errorf("failed to delete slice directory: %w", err)
	}

	// Remove from slice manager's metadata
	newSliceInfos := make([]SliceInfo, 0, len(s.sliceManager.sliceInfos))
	for _, info := range s.sliceManager.sliceInfos {
		if info.SliceNum != sliceNum {
			newSliceInfos = append(newSliceInfos, info)
		}
	}
	s.sliceManager.sliceInfos = newSliceInfos

	// Clear from cache if present
	delete(s.sliceManager.cachedSlices, sliceNum)

	// If this was the active slice, clear it
	if s.sliceManager.activeSlice != nil && s.sliceManager.activeSlice.SliceNum == sliceNum {
		s.sliceManager.activeSlice = nil
	}

	return nil
}

// ReloadSliceInfos reloads slice metadata from disk (for admin operations)
func (s *SliceStore) ReloadSliceInfos() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sliceManager.loadSliceInfos()
}

// AddDownloadedSlice adds a downloaded slice to the store, updates slices.json and LIB
func (s *SliceStore) AddDownloadedSlice(info *SliceInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.sliceManager.AddSliceInfo(*info); err != nil {
		return err
	}

	if info.EndBlock > s.lib {
		s.lib = info.EndBlock
		s.head = info.EndBlock
	}

	return s.saveState()
}

// SetLIB sets the LIB manually (for admin operations like slice recreation)
func (s *SliceStore) SetLIB(lib uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lib = lib
	s.head = lib

	// Clear active slice so a new one will be created when writing resumes
	s.sliceManager.activeSlice = nil

	return s.saveState()
}

// RemoveSlice removes an invalid slice and logs the operation
// This is a wrapper around DeleteSlice with validation-specific logging
func (s *SliceStore) RemoveSlice(sliceNum uint32) error {
	logger.Warning("Removing invalid slice %d...", sliceNum)
	if err := s.DeleteSlice(sliceNum); err != nil {
		return fmt.Errorf("failed to remove slice %d: %w", sliceNum, err)
	}
	// Save slices.json after removal
	s.mu.Lock()
	err := s.sliceManager.saveSliceInfos()
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to save slice infos after removal: %w", err)
	}
	return nil
}

// AdjustLIBToSlice sets LIB to end of given slice, for use after removing invalid slices
// If sliceNum is provided, LIB is set to end of that slice
// If sliceNum == 0 or no valid slices exist, LIB is set to 0
func (s *SliceStore) AdjustLIBToSlice(sliceNum uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sliceNum == 0 {
		s.lib = 0
		s.head = 0
		s.sliceManager.activeSlice = nil
		logger.Printf("store", "LIB reset to 0 (no valid slices)")
		return s.saveState()
	}

	// Find the slice info to get its end block
	var targetSlice *SliceInfo
	for _, info := range s.sliceManager.sliceInfos {
		if info.SliceNum == sliceNum {
			targetSlice = &info
			break
		}
	}

	if targetSlice == nil {
		return fmt.Errorf("slice %d not found in slice infos", sliceNum)
	}

	// Set LIB to end of the target slice
	// For finalized slices, EndBlock is the last block
	// For active slices, EndBlock may be 0 or the last written block
	newLIB := targetSlice.EndBlock
	if newLIB == 0 {
		// Active slice with no blocks yet - use previous slice's end or 0
		if sliceNum > 0 {
			// Find previous slice
			for _, info := range s.sliceManager.sliceInfos {
				if info.SliceNum == sliceNum-1 {
					newLIB = info.EndBlock
					break
				}
			}
		}
	}

	s.lib = newLIB
	s.head = newLIB
	s.sliceManager.activeSlice = nil

	logger.Printf("store", "LIB adjusted to %d (end of slice %d)", newLIB, sliceNum)
	return s.saveState()
}

// ValidateAndRepairLastSlices validates the last N slices and repairs if needed
func (s *SliceStore) ValidateAndRepairLastSlices(numSlices int) error {
	sliceInfos := s.sliceManager.SortedSliceInfos()
	if len(sliceInfos) == 0 {
		logger.Println("validation", "No slices to validate")
		return nil
	}

	// Determine which slices to validate (last N)
	startIdx := len(sliceInfos) - numSlices
	if startIdx < 0 {
		startIdx = 0
	}
	slicesToValidate := sliceInfos[startIdx:]

	logger.Printf("validation", "Validating last %d slices...", len(slicesToValidate))
	startTime := time.Now()

	// Validate each slice
	var invalidSlices []uint32
	for _, info := range slicesToValidate {
		result := s.ValidateSlice(info.SliceNum)
		if !result.Valid {
			invalidSlices = append(invalidSlices, info.SliceNum)
		}
	}

	if len(invalidSlices) == 0 {
		logger.Printf("validation", "✓ Startup validation complete: %d slices checked in %.2fs",
			len(slicesToValidate), time.Since(startTime).Seconds())
		return nil
	}

	// Remove invalid slices (from highest to lowest to maintain consistency)
	logger.Warning("Found %d invalid slices, removing...", len(invalidSlices))
	for i := len(invalidSlices) - 1; i >= 0; i-- {
		sliceNum := invalidSlices[i]
		if err := s.RemoveSlice(sliceNum); err != nil {
			return fmt.Errorf("failed to remove invalid slice %d: %w", sliceNum, err)
		}
	}

	// Find the last valid slice to adjust LIB
	sliceInfos = s.sliceManager.SortedSliceInfos()
	if len(sliceInfos) == 0 {
		if err := s.AdjustLIBToSlice(0); err != nil {
			return fmt.Errorf("failed to reset LIB: %w", err)
		}
	} else {
		// Adjust LIB to end of last valid slice
		lastValid := sliceInfos[len(sliceInfos)-1]
		if err := s.AdjustLIBToSlice(lastValid.SliceNum); err != nil {
			return fmt.Errorf("failed to adjust LIB to slice %d: %w", lastValid.SliceNum, err)
		}
	}

	logger.Printf("validation", "✓ Validation and repair complete in %.2fs, LIB=%d",
		time.Since(startTime).Seconds(), s.lib)
	return nil
}

// recoverFromCrash validates slices and ensures consistency with saved state
// For slice-based storage, we:
// 1. Verify each slice's data matches its metadata
// 2. Validate actual block data (CRC) and adjust LIB to last valid block
// 3. Rebuild indices if needed
func (s *SliceStore) recoverFromCrash() error {
	startTime := time.Now()
	logger.Printf("debug-startup", "    recoverFromCrash starting...")

	sliceInfos := s.sliceManager.GetSliceInfos()
	if len(sliceInfos) == 0 {
		logger.Printf("debug-startup", "    recoverFromCrash: no slices to recover")
		return nil
	}

	// Find the slice containing our LIB
	libSliceNum := s.sliceManager.BlockToSliceNum(s.lib)
	logger.Printf("debug-startup", "    recoverFromCrash: LIB=%d in slice %d, total slices=%d", s.lib, libSliceNum, len(sliceInfos))

	// Track slices beyond LIB for summary logging
	var beyondLIBSlices []uint32

	for _, info := range sliceInfos {
		if info.SliceNum > libSliceNum {
			beyondLIBSlices = append(beyondLIBSlices, info.SliceNum)
			continue
		}

		if info.SliceNum == libSliceNum && !info.Finalized {
			t0 := time.Now()
			slice, err := s.sliceManager.GetSliceByNum(info.SliceNum)
			logger.Printf("debug-startup", "    GetSliceByNum(%d) took %.2fs", info.SliceNum, time.Since(t0).Seconds())
			if err != nil {
				// Slice referenced in state.json doesn't exist (likely deleted/corrupted)
				// Rebuild state from last available finalized slice
				logger.Warning("Slice %d referenced in state.json does not exist - rebuilding state from available slices", info.SliceNum)

				// Find last finalized slice with valid data
				var lastValidLIB uint32
				for i := len(sliceInfos) - 1; i >= 0; i-- {
					if sliceInfos[i].Finalized && sliceInfos[i].EndBlock > 0 {
						lastValidLIB = sliceInfos[i].EndBlock
						logger.Printf("store", "Using LIB=%d from last finalized slice %d", lastValidLIB, sliceInfos[i].SliceNum)
						break
					}
				}

				if lastValidLIB > 0 {
					s.lib = lastValidLIB
					s.head = lastValidLIB
					if err := s.saveState(); err != nil {
						return fmt.Errorf("failed to save rebuilt state: %w", err)
					}
					logger.Printf("store", "State rebuilt: LIB=%d, HEAD=%d", s.lib, s.head)
					return nil // Recovery complete
				}

				return fmt.Errorf("failed to load slice %d for recovery and no valid finalized slices found: %w", info.SliceNum, err)
			}

			blockIndex := slice.GetBlockIndex()
			if blockIndex == nil {
				continue
			}

			entries := blockIndex.GetAllEntries()
			if len(entries) == 0 {
				continue
			}

			_, maxBlock := blockIndex.Range()

			// Determine the target LIB (minimum of state LIB and index maxBlock)
			targetLIB := s.lib
			if maxBlock < targetLIB {
				logger.Warning("State claims LIB=%d but slice %d index only has blocks up to %d",
					s.lib, info.SliceNum, maxBlock)
				targetLIB = maxBlock
			}

			// Validate blocks from targetLIB backwards to find last valid block
			// This catches cases where index entries exist but data is corrupt/missing
			validatedLIB := uint32(0)
			if len(entries) > 0 {
				validatedLIB = entries[0].BlockNum - 1 // Start before first block in slice
			}

			logger.Printf("store", "Validating blocks in active slice %d (checking up to block %d)...",
				info.SliceNum, targetLIB)

			// Validate from the end backwards - stop at first valid block
			for i := len(entries) - 1; i >= 0; i-- {
				entry := entries[i]
				if entry.BlockNum > targetLIB {
					continue // Skip blocks beyond our target
				}

				// Try to read the block data (includes CRC validation)
				_, err := slice.GetBlock(entry.BlockNum)
				if err != nil {
					logger.Warning("Block %d failed validation: %v", entry.BlockNum, err)
					continue
				}

				// Found a valid block
				validatedLIB = entry.BlockNum
				break
			}

			if validatedLIB < s.lib {
				logger.Warning("Adjusting LIB from %d to %d based on validated data", s.lib, validatedLIB)
				s.lib = validatedLIB
				s.head = validatedLIB

				// Truncate block index to remove invalid entries
				removed := blockIndex.TruncateAfter(validatedLIB)
				if removed > 0 {
					logger.Printf("store", "Removed %d invalid entries from block index", removed)
				}

				// Save the corrected indices (glob index no longer needed - lookups use blockIndex)
				if err := slice.SaveIndices(); err != nil {
					return fmt.Errorf("failed to save corrected indices: %w", err)
				}

				if err := s.saveState(); err != nil {
					return fmt.Errorf("failed to save corrected state: %w", err)
				}
				logger.Printf("store", "LIB adjusted to %d based on validated block data", s.lib)
			}

			// Check for blocks beyond LIB that need index cleanup
			for _, entry := range blockIndex.GetAllEntries() {
				if entry.BlockNum > s.lib {
					logger.Warning("Found block %d in slice %d beyond LIB %d - note: glob index no longer used",
						entry.BlockNum, info.SliceNum, s.lib)
					break
				}
			}
		}
	}

	// Log summary of slices beyond LIB
	if len(beyondLIBSlices) > 0 {
		if len(beyondLIBSlices) <= 5 {
			logger.Warning("Found %d slices beyond LIB slice %d: %v (may need cleanup)",
				len(beyondLIBSlices), libSliceNum, beyondLIBSlices)
		} else {
			logger.Warning("Found %d slices beyond LIB slice %d (first 5: %v..., last: %d) - may need cleanup",
				len(beyondLIBSlices), libSliceNum, beyondLIBSlices[:5], beyondLIBSlices[len(beyondLIBSlices)-1])
		}
	}

	logger.Printf("store", "Crash recovery complete - LIB: %d", s.lib)
	logger.Printf("debug-startup", "    recoverFromCrash completed in %.2fs", time.Since(startTime).Seconds())
	return nil
}

// Background glob index building is no longer needed since indices are built
// incrementally during AppendBlock(). This code was removed as it became redundant.
// Validation and recovery still use BuildGlobIndex() methods when needed.

// SliceValidationResult contains the result of validating a single slice
type SliceValidationResult struct {
	Valid           bool
	SliceNum        uint32
	BlocksValidated int
	GlobsValidated  int
	Duration        time.Duration
	Error           error  // nil if valid
	ErrorBlock      uint32 // which block failed (if applicable)
}

// ValidateSlice performs validation of a slice's data integrity
// Steps:
// 1. Load blocks.index and verify block/glob continuity
// 2. Sequential scan of data.log verifying CRC for each entry (no decompression)
// 3. Cross-check: number of entries scanned matches blocks.index count
// 4. For finalized slices: verify entry count matches expected slice size
//
// This is optimized for speed: sequential I/O + CRC verification without decompression.
// A 4MB slice validates in ~0.1s vs ~40s with the old random-read approach.
func (s *SliceStore) ValidateSlice(sliceNum uint32) *SliceValidationResult {
	startTime := time.Now()
	result := &SliceValidationResult{
		SliceNum: sliceNum,
		Valid:    false,
	}

	// Get slice info
	var sliceInfo *SliceInfo
	for _, info := range s.sliceManager.GetSliceInfos() {
		if info.SliceNum == sliceNum {
			sliceInfo = &info
			break
		}
	}
	if sliceInfo == nil {
		result.Error = fmt.Errorf("slice %d not found in slice infos", sliceNum)
		result.Duration = time.Since(startTime)
		return result
	}

	logger.Printf("validation", "Validating slice %d (blocks %d-%d)...", sliceNum, sliceInfo.StartBlock, sliceInfo.MaxBlock)

	// Load slice to get block index
	slice, err := s.sliceManager.GetSliceByNum(sliceNum)
	if err != nil {
		result.Error = fmt.Errorf("failed to load slice %d: %w", sliceNum, err)
		result.Duration = time.Since(startTime)
		return result
	}

	// Get block index
	blockIndex := slice.GetBlockIndex()
	if blockIndex == nil {
		result.Error = fmt.Errorf("slice %d has no block index", sliceNum)
		result.Duration = time.Since(startTime)
		return result
	}

	entries := blockIndex.GetAllEntries()
	if len(entries) == 0 {
		// Empty slice is valid (active slice that hasn't received blocks yet)
		result.Valid = true
		result.Duration = time.Since(startTime)
		logger.Printf("validation", "✓ Slice %d validated: empty slice in %.2fs", sliceNum, result.Duration.Seconds())
		return result
	}

	// Step 1: Verify block/glob continuity from index entries (very fast, in-memory)
	var prevBlockNum uint32 = 0
	var prevGlobMax uint64 = 0
	var firstBlock = true

	for i, entry := range entries {
		if !firstBlock {
			// Check block continuity
			if entry.BlockNum != prevBlockNum+1 {
				result.Error = fmt.Errorf("block continuity gap: expected %d, got %d", prevBlockNum+1, entry.BlockNum)
				result.ErrorBlock = entry.BlockNum
				result.BlocksValidated = i
				result.Duration = time.Since(startTime)
				logger.Warning("✗ Slice %d invalid: %v", sliceNum, result.Error)
				return result
			}

			// Check glob continuity (GlobMin should be previous GlobMax + 1, unless empty block)
			if entry.GlobMin > 0 && prevGlobMax > 0 && entry.GlobMin != prevGlobMax+1 {
				if entry.GlobMax >= entry.GlobMin { // Non-empty block
					result.Error = fmt.Errorf("glob continuity gap at block %d: expected GlobMin %d, got %d",
						entry.BlockNum, prevGlobMax+1, entry.GlobMin)
					result.ErrorBlock = entry.BlockNum
					result.BlocksValidated = i
					result.Duration = time.Since(startTime)
					logger.Warning("✗ Slice %d invalid: %v", sliceNum, result.Error)
					return result
				}
			}
		}

		// Count globs
		if entry.GlobMax >= entry.GlobMin {
			result.GlobsValidated += int(entry.GlobMax - entry.GlobMin + 1)
		}

		prevBlockNum = entry.BlockNum
		prevGlobMax = entry.GlobMax
		firstBlock = false
	}

	// Step 2: Sequential CRC scan of data.log (fast sequential I/O, no decompression)
	slicePath := s.sliceManager.slicePath(sliceNum)
	dataLogPath := filepath.Join(slicePath, "data.log")

	hasHeader, _ := HasHeader(dataLogPath)
	var reader *Reader
	if hasHeader {
		reader, err = NewReaderWithHeader(dataLogPath)
	} else {
		reader, err = NewReader(dataLogPath)
	}
	if err != nil {
		result.Error = fmt.Errorf("failed to open data.log: %w", err)
		result.Duration = time.Since(startTime)
		return result
	}
	defer reader.Close()

	// Count entries during scan (CRC verified by ScanFromOffset/Scan internally)
	entriesScanned := 0
	var scanErr error

	scanCallback := func(offset uint64, data []byte) error {
		entriesScanned++
		return nil
	}

	if hasHeader {
		scanErr = reader.ScanFromOffset(HeaderSize, scanCallback)
	} else {
		scanErr = reader.Scan(scanCallback)
	}

	if scanErr != nil {
		result.Error = fmt.Errorf("data.log CRC validation failed: %w", scanErr)
		result.Duration = time.Since(startTime)
		logger.Warning("✗ Slice %d invalid: %v", sliceNum, result.Error)
		return result
	}

	// Step 3: Cross-check entry counts
	if entriesScanned != len(entries) {
		result.Error = fmt.Errorf("entry count mismatch: blocks.index has %d entries, data.log has %d",
			len(entries), entriesScanned)
		result.Duration = time.Since(startTime)
		logger.Warning("✗ Slice %d invalid: %v", sliceNum, result.Error)
		return result
	}

	result.BlocksValidated = len(entries)

	// Step 4: For finalized slices, verify entry count matches expected slice size
	if sliceInfo.Finalized {
		expectedBlocks := int(s.sliceManager.blocksPerSlice)
		if result.BlocksValidated != expectedBlocks {
			result.Error = fmt.Errorf("finalized slice block count mismatch: expected %d, got %d",
				expectedBlocks, result.BlocksValidated)
			result.Duration = time.Since(startTime)
			logger.Warning("✗ Slice %d invalid: %v", sliceNum, result.Error)
			return result
		}
	}

	result.Valid = true
	result.Duration = time.Since(startTime)
	logger.Printf("validation", "✓ Slice %d validated: %d blocks, %d globs in %.2fs",
		sliceNum, result.BlocksValidated, result.GlobsValidated, result.Duration.Seconds())
	return result
}

// ValidateSlices validates multiple slices and returns results
// If parallel > 1, uses parallel validation (for --verify mode)
func (s *SliceStore) ValidateSlices(sliceNums []uint32, parallel int) []*SliceValidationResult {
	if parallel <= 1 {
		// Sequential validation
		results := make([]*SliceValidationResult, len(sliceNums))
		for i, sliceNum := range sliceNums {
			results[i] = s.ValidateSlice(sliceNum)
		}
		return results
	}

	// Parallel validation
	results := make([]*SliceValidationResult, len(sliceNums))
	var wg sync.WaitGroup
	workChan := make(chan int, len(sliceNums))

	// Start workers
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range workChan {
				results[i] = s.ValidateSlice(sliceNums[i])
			}
		}()
	}

	// Send work
	for i := range sliceNums {
		workChan <- i
	}
	close(workChan)

	wg.Wait()
	return results
}
