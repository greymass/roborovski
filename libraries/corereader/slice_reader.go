package corereader

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/compression"
	"github.com/greymass/roborovski/libraries/logger"
)

// Scan cache - eliminates duplicate filesystem scans when opening same path multiple times
var (
	scanCacheMu sync.RWMutex
	scanCache   = make(map[string]*cachedScan)
)

type cachedScan struct {
	sharedMetadata *SharedSliceMetadata
	scannedAt      time.Time
	mu             sync.RWMutex
}

// SharedSliceMetadata is thread-safe shared slice metadata for multiple readers
type SharedSliceMetadata struct {
	mu         sync.RWMutex
	slices     []SliceInfo
	sliceIndex map[uint32]int // sliceNum → index in slices array
}

func newSharedSliceMetadata(initialSlices []SliceInfo) *SharedSliceMetadata {
	// Pre-allocate capacity for 1000 additional slices (~10M blocks)
	capacity := len(initialSlices) + 1000
	slices := make([]SliceInfo, len(initialSlices), capacity)
	copy(slices, initialSlices)

	sliceIndex := make(map[uint32]int, len(initialSlices))
	for i, s := range slices {
		sliceIndex[s.SliceNum] = i
	}

	return &SharedSliceMetadata{
		slices:     slices,
		sliceIndex: sliceIndex,
	}
}

func (sm *SharedSliceMetadata) getSliceCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.slices)
}

func (sm *SharedSliceMetadata) getSlice(idx int) SliceInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.slices[idx]
}

func (sm *SharedSliceMetadata) getSlices() []SliceInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]SliceInfo, len(sm.slices))
	copy(result, sm.slices)
	return result
}

func (sm *SharedSliceMetadata) appendSlice(slice SliceInfo) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	idx := len(sm.slices)
	sm.slices = append(sm.slices, slice)
	sm.sliceIndex[slice.SliceNum] = idx
	return idx
}

func (sm *SharedSliceMetadata) updateSlice(idx int, slice SliceInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if idx >= 0 && idx < len(sm.slices) {
		sm.slices[idx] = slice
	}
}

func (sm *SharedSliceMetadata) addSlice(slice SliceInfo) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.sliceIndex[slice.SliceNum]; exists {
		return false
	}

	insertIdx := sort.Search(len(sm.slices), func(i int) bool {
		return sm.slices[i].SliceNum >= slice.SliceNum
	})

	sm.slices = append(sm.slices, SliceInfo{})
	copy(sm.slices[insertIdx+1:], sm.slices[insertIdx:])
	sm.slices[insertIdx] = slice

	for i, s := range sm.slices {
		sm.sliceIndex[s.SliceNum] = i
	}

	return true
}

func (sm *SharedSliceMetadata) markSliceFinalized(idx int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if idx >= 0 && idx < len(sm.slices) {
		sm.slices[idx].Finalized = true
	}
}

func (sm *SharedSliceMetadata) updateLastSliceGlobRange(globMin, globMax uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(sm.slices) > 0 {
		lastIdx := len(sm.slices) - 1
		sm.slices[lastIdx].GlobMin = globMin
		sm.slices[lastIdx].GlobMax = globMax
	}
}

// findSliceForGlob finds the slice containing the given global sequence using binary search.
// Returns the slice info and index, or nil/-1 if not found.
func (sm *SharedSliceMetadata) findSliceForGlob(glob uint64) (*SliceInfo, int) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.slices) == 0 {
		return nil, -1
	}

	// Binary search for the slice where MaxGlob >= glob
	idx := sort.Search(len(sm.slices), func(i int) bool {
		return sm.slices[i].GlobMax >= glob
	})

	if idx < len(sm.slices) {
		slice := &sm.slices[idx]
		if glob >= slice.GlobMin && glob <= slice.GlobMax {
			result := *slice // Copy to avoid holding lock
			return &result, idx
		}
	}

	return nil, -1
}

func (sm *SharedSliceMetadata) updateLastSliceEndBlock(endBlock uint32, globMin, globMax uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(sm.slices) > 0 {
		lastIdx := len(sm.slices) - 1
		sm.slices[lastIdx].EndBlock = endBlock
		sm.slices[lastIdx].GlobMin = globMin
		sm.slices[lastIdx].GlobMax = globMax
	}
}

func (sm *SharedSliceMetadata) getSliceByNum(sliceNum uint32) *SliceInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if idx, ok := sm.sliceIndex[sliceNum]; ok {
		slice := sm.slices[idx]
		return &slice
	}
	return nil
}

// getCachedOrScan returns shared metadata if cached, otherwise scans filesystem and creates new shared metadata
func getCachedOrScan(basePath string) (*SharedSliceMetadata, error) {
	scanCacheMu.RLock()
	cached, exists := scanCache[basePath]
	scanCacheMu.RUnlock()

	if exists {
		cached.mu.RLock()
		age := time.Since(cached.scannedAt)
		metadata := cached.sharedMetadata
		cached.mu.RUnlock()

		logger.Printf("config", "Using cached storage scan: %d slices (cached %v ago)",
			metadata.getSliceCount(), age.Round(time.Second))
		return metadata, nil
	}

	// First scan for this path - try loading from cache
	logger.Printf("config", "Scanning storage directory: %s", basePath)
	slices, err := LoadSlicesFromCache(basePath)
	if err != nil {
		logger.Printf("config", "Cache load failed (%v), scanning filesystem...", err)
		slices, err = scanSlicesFromFilesystem(basePath)
		if err != nil {
			return nil, err
		}
	} else {
		logger.Printf("config", "Loaded %d slices from cache", len(slices))
	}

	metadata := newSharedSliceMetadata(slices)

	// Cache for subsequent opens
	scanCacheMu.Lock()
	scanCache[basePath] = &cachedScan{
		sharedMetadata: metadata,
		scannedAt:      time.Now(),
	}
	scanCacheMu.Unlock()

	return metadata, nil
}

type SliceReaderOptions struct {
	CacheSizeMB           int64  // Decompressed slice buffer cache (0 = disabled)
	BlockCacheSizeMB      int64  // Decompressed block cache for queries (0 = disabled, default: 2048)
	UnindexedSliceCount   int    // Number of recent slices to pin in cache (default: 2)
	LRUCacheSlices        int    // Number of slices in LRU cache (default: 20)
	DisableGlobRangeIndex bool   // Skip loading glob range index (saves memory + startup time)
	DisableStatsReporter  bool   // Disable internal stats logging (service handles its own)
	BlockIndexCachePath   string // Path to blockindex cache file (empty = disabled)
}

func DefaultSliceReaderOptions() SliceReaderOptions {
	return SliceReaderOptions{
		CacheSizeMB:           0,
		BlockCacheSizeMB:      2048,
		UnindexedSliceCount:   2,
		LRUCacheSlices:        20,
		DisableGlobRangeIndex: false,
	}
}

func SyncReaderOptions() SliceReaderOptions {
	return SliceReaderOptions{
		CacheSizeMB:           0,
		BlockCacheSizeMB:      0,
		UnindexedSliceCount:   2,
		LRUCacheSlices:        2,
		DisableGlobRangeIndex: true,
		DisableStatsReporter:  true,
	}
}

func QueryReaderOptions() SliceReaderOptions {
	return SliceReaderOptions{
		CacheSizeMB:           0,
		BlockCacheSizeMB:      2048,
		UnindexedSliceCount:   2,
		LRUCacheSlices:        20,
		DisableGlobRangeIndex: false,
	}
}

type BlockCache struct {
	mu       sync.RWMutex
	cache    map[uint32]*lazyBlockBlob // blockNum → parsed block
	order    []uint32                  // LRU order (oldest first)
	maxBytes int64                     // Max total bytes (estimated)
	curBytes int64                     // Current bytes used (estimated)
	hits     atomic.Uint64             // Cache hit counter
	misses   atomic.Uint64             // Cache miss counter
}

func NewBlockCache(maxSizeMB int64) *BlockCache {
	if maxSizeMB <= 0 {
		return nil
	}
	return &BlockCache{
		cache:    make(map[uint32]*lazyBlockBlob),
		order:    make([]uint32, 0, 10000),
		maxBytes: maxSizeMB * 1024 * 1024,
	}
}

func (c *BlockCache) Get(blockNum uint32) *lazyBlockBlob {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	data, found := c.cache[blockNum]
	c.mu.RUnlock()
	if found {
		c.hits.Add(1)
		return data
	}
	c.misses.Add(1)
	return nil
}

func (c *BlockCache) Put(blockNum uint32, blob *lazyBlockBlob, estSize int64) {
	if c == nil || blob == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.cache[blockNum]; exists {
		return
	}

	for c.curBytes+estSize > c.maxBytes && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if oldBlob, exists := c.cache[oldest]; exists {
			c.curBytes -= estimateBlobSize(oldBlob)
			delete(c.cache, oldest)
		}
	}

	c.cache[blockNum] = blob
	c.order = append(c.order, blockNum)
	c.curBytes += estSize
}

func estimateBlobSize(blob *lazyBlockBlob) int64 {
	if blob == nil {
		return 0
	}
	size := int64(64) // Base struct overhead
	size += int64(len(blob.CatsOffset) * 4)
	size += int64(len(blob.GlobalSeqs) * 8)
	for _, b := range blob.CatsBytes {
		size += int64(len(b))
	}
	return size
}

func (c *BlockCache) Stats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.RLock()
	sizeBytes = c.curBytes
	numBlocks = len(c.cache)
	c.mu.RUnlock()
	return c.hits.Load(), c.misses.Load(), sizeBytes, numBlocks
}

type SliceInfo struct {
	SliceNum       uint32 `json:"slice_num"`
	StartBlock     uint32 `json:"start_block"`      // First block in slice range (inclusive)
	EndBlock       uint32 `json:"end_block"`        // Last block actually written
	MaxBlock       uint32 `json:"max_block"`        // Last block in slice range (for directory naming)
	BlocksPerSlice uint32 `json:"blocks_per_slice"` // Slice size (for directory naming)
	Finalized      bool   `json:"finalized"`
	GlobMin        uint64 `json:"glob_min"`
	GlobMax        uint64 `json:"glob_max"`
}

type sliceReader struct {
	sliceNum   uint32
	basePath   string
	blockIndex map[uint32]blockIndexEntry
	blockFile  *os.File
	mmapData   []byte
	header     *DataLogHeader
	loaded     bool
	mu         sync.RWMutex
	refCount   atomic.Int32
}

type SliceGlobRange struct {
	SliceNum uint32 // Slice number
	MinGlob  uint64 // First global sequence in slice
	MaxGlob  uint64 // Last global sequence in slice
}

type GlobRangeIndex struct {
	ranges              []SliceGlobRange // Sorted array of slice ranges (finalized slices only, excludes last 2)
	highestIndexedSlice uint32           // Highest slice number in index (last 2 slices excluded)
	totalSlices         int              // Number of slices indexed
	memoryUsedKB        int64            // Approximate memory usage in KB
	loadTime            time.Duration    // Time taken to load index
	mu                  sync.RWMutex     // Protect during lookups
	updateMu            sync.Mutex       // Protect during updates (adding new slices)
	lastUpdateCheck     time.Time        // Last time we checked for new finalized slices
	lastStatsSliceCount int              // For tracking growth in debug logging
	updateCooldown      time.Duration    // Minimum time between update checks (default: 5s)
}

type GlobRangeIndexStats struct {
	IndexedSlices           int       // Number of slices in index
	HighestIndexed          uint32    // Highest slice number indexed
	HighestGlob             uint64    // Highest globalSeq in indexed slices
	MemoryKB                int64     // Memory used in KB
	LastUpdate              time.Time // When index was last updated
	SlicesAddedSinceLastLog int       // Slices added since last stats call (for growth tracking)
}

type SliceReader struct {
	basePath       string
	sharedMetadata *SharedSliceMetadata // Shared metadata across all readers

	lastAccessedSliceIdx atomic.Int32   // Index of last accessed slice (-1 = none)
	sliceMapByBlock      map[uint32]int // Block number / 10000 → slice index

	pinnedSlices        map[uint32]*sliceReader
	pinnedSlicesMu      sync.RWMutex
	unindexedSliceCount int

	sliceReaders       map[uint32]*sliceReader
	sliceReadersMu     sync.RWMutex
	sliceReaderLoading map[uint32]chan struct{} // Tracks slices currently being loaded (channel closed when done)
	maxCached          int
	cacheOrder         []uint32 // LRU order

	bufferPool *SliceBufferPool

	blockCache *BlockCache

	bufferLoadMu sync.Mutex
	sliceLoading map[uint32]bool // Tracks which slices are currently being loaded (for buffer pool)

	globRangeIndex  *GlobRangeIndex
	blockIndexCache *BlockIndexCache

	sliceDiscoveryMu sync.Mutex
	sliceDiscovering map[uint32]chan struct{}

	cacheHits           atomic.Uint64
	cacheMisses         atomic.Uint64
	lastStatsReport     atomic.Int64  // Unix timestamp of last report
	statsReportInterval int64         // Seconds between reports (default 10)
	stopStatsTicker     chan struct{} // Signal to stop background stats reporter
	closeOnce           sync.Once     // Ensures Close() is idempotent
}

func NewSliceReader(basePath string) (*SliceReader, error) {
	return NewSliceReaderWithCache(basePath, 0) // 0 = cache disabled
}

func NewSliceReaderWithCache(basePath string, cacheSizeMB int64) (*SliceReader, error) {
	opts := DefaultSliceReaderOptions()
	opts.CacheSizeMB = cacheSizeMB
	return NewSliceReaderWithOptions(basePath, opts)
}

func NewSliceReaderWithOptions(basePath string, opts SliceReaderOptions) (*SliceReader, error) {
	if _, err := os.Stat(basePath); err != nil {
		return nil, fmt.Errorf("storage path not found: %w", err)
	}

	// Use cached scan if available (eliminates duplicate scans)
	sharedMetadata, err := getCachedOrScan(basePath)
	if err != nil || sharedMetadata.getSliceCount() == 0 {
		logger.Printf("warning", "Filesystem scan failed (%v), falling back to slices.json", err)
		slices, err := loadSlicesFromJSON(basePath)
		if err != nil {
			return nil, err
		}
		sharedMetadata = newSharedSliceMetadata(slices)
	}

	// Sort slices by slice number
	slices := sharedMetadata.getSlices()
	sort.Slice(slices, func(i, j int) bool {
		return slices[i].SliceNum < slices[j].SliceNum
	})
	sharedMetadata = newSharedSliceMetadata(slices)

	sliceCount := sharedMetadata.getSliceCount()
	logger.Printf("debug", "SliceReader: Loaded %d slices from %s", sliceCount, basePath)
	if sliceCount > 0 {
		firstSlice := sharedMetadata.getSlice(0)
		logger.Printf("debug", "  First slice: num=%d blocks=%d-%d globMin=%d globMax=%d",
			firstSlice.SliceNum, firstSlice.StartBlock, firstSlice.EndBlock,
			firstSlice.GlobMin, firstSlice.GlobMax)
		lastSlice := sharedMetadata.getSlice(sliceCount - 1)
		logger.Printf("debug", "  Last slice: num=%d blocks=%d-%d globMin=%d globMax=%d",
			lastSlice.SliceNum, lastSlice.StartBlock, lastSlice.EndBlock,
			lastSlice.GlobMin, lastSlice.GlobMax)
	}

	sliceMapByBlock := make(map[uint32]int)
	for i := 0; i < sliceCount; i++ {
		slice := sharedMetadata.getSlice(i)
		sliceKey := slice.StartBlock / slice.BlocksPerSlice
		sliceMapByBlock[sliceKey] = i
	}

	var pool *SliceBufferPool
	if opts.CacheSizeMB > 0 {
		pool = NewSliceBufferPool(opts.CacheSizeMB)
	}

	blockCache := NewBlockCache(opts.BlockCacheSizeMB)
	if blockCache != nil {
		logger.Printf("config", "Block cache enabled: %d MB", opts.BlockCacheSizeMB)
	}

	var blockIndexCache *BlockIndexCache
	if opts.BlockIndexCachePath != "" {
		blockIndexCache = NewBlockIndexCache(opts.BlockIndexCachePath)
	}

	sr := &SliceReader{
		basePath:            basePath,
		sharedMetadata:      sharedMetadata,
		sliceMapByBlock:     sliceMapByBlock,
		pinnedSlices:        make(map[uint32]*sliceReader),
		unindexedSliceCount: opts.UnindexedSliceCount,
		sliceReaders:        make(map[uint32]*sliceReader),
		sliceReaderLoading:  make(map[uint32]chan struct{}),
		maxCached:           opts.LRUCacheSlices,
		cacheOrder:          make([]uint32, 0),
		bufferPool:          pool,
		blockCache:          blockCache,
		blockIndexCache:     blockIndexCache,
		sliceLoading:        make(map[uint32]bool),
		sliceDiscovering:    make(map[uint32]chan struct{}),
		statsReportInterval: 10, // Report cache stats every 10 seconds
		stopStatsTicker:     make(chan struct{}),
	}
	sr.lastAccessedSliceIdx.Store(-1) // Initialize to invalid
	sr.lastStatsReport.Store(time.Now().Unix())

	if !opts.DisableStatsReporter {
		go sr.statsReporterLoop()
	}

	if !opts.DisableGlobRangeIndex {
		logger.Printf("config", "Building glob range index from cached metadata...")
		rangeIndex := buildGlobRangeIndexFromSlices(sharedMetadata)
		if rangeIndex != nil {
			sr.globRangeIndex = rangeIndex

			var maxGlob uint64
			if len(rangeIndex.ranges) > 0 {
				maxGlob = rangeIndex.ranges[len(rangeIndex.ranges)-1].MaxGlob
			}

			logger.Printf("config", "Glob range index built:")
			logger.Printf("config", "  Indexed slices: %d (excluding last 2 active slices)",
				rangeIndex.totalSlices)
			logger.Printf("config", "  Highest slice:  %d", rangeIndex.highestIndexedSlice)
			logger.Printf("config", "  Max globalSeq:  %d", maxGlob)
			logger.Printf("config", "  Memory usage:   %d KB", rangeIndex.memoryUsedKB)
			logger.Printf("config", "  Build time:     %v", rangeIndex.loadTime)
		}
	} else {
		logger.Printf("config", "Glob range index disabled (sync-optimized mode)")
	}

	if sr.blockIndexCache != nil {
		if err := sr.initBlockIndexCache(); err != nil {
			logger.Printf("warning", "BlockIndexCache initialization failed: %v", err)
		}
	}

	return sr, nil
}

func (sr *SliceReader) initBlockIndexCache() error {
	start := time.Now()

	if err := sr.blockIndexCache.LoadFromFile(); err != nil {
		logger.Printf("warning", "BlockIndexCache: failed to load from file: %v", err)
	}

	maxCachedSlice := sr.blockIndexCache.GetMaxFinalizedSlice()
	sliceCount := sr.sharedMetadata.getSliceCount()

	var loadedCount int
	for i := 0; i < sliceCount; i++ {
		slice := sr.sharedMetadata.getSlice(i)

		if sr.blockIndexCache.HasSlice(slice.SliceNum) {
			continue
		}

		slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
		if err := sr.blockIndexCache.LoadSliceFromBlocksIndex(slicePath, slice.SliceNum, slice.StartBlock, slice.Finalized); err != nil {
			logger.Printf("debug", "BlockIndexCache: failed to load slice %d: %v", slice.SliceNum, err)
			continue
		}
		loadedCount++
	}

	stats := sr.blockIndexCache.GetStats()
	logger.Printf("config", "BlockIndexCache initialized:")
	logger.Printf("config", "  From cache file: slices up to %d", maxCachedSlice)
	logger.Printf("config", "  Loaded from disk: %d slices", loadedCount)
	logger.Printf("config", "  Total slices: %d (%d finalized, %d active)",
		stats.TotalSlices, stats.FinalizedSlices, stats.ActiveSlices)
	logger.Printf("config", "  Total entries: %d (~%s)",
		stats.TotalEntries, logger.FormatBytes(stats.MemoryBytes))
	logger.Printf("config", "  Init time: %v", time.Since(start))

	return nil
}

const (
	sliceCacheMagic   = "RBSM"
	sliceCacheVersion = uint32(1)
	sliceCacheHeader  = 16
	sliceCacheEntry   = 40
)

func LoadSlicesFromCache(basePath string) ([]SliceInfo, error) {
	cachePath := filepath.Join(basePath, "slices.cache")
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, err
	}

	if len(data) < sliceCacheHeader {
		return nil, fmt.Errorf("cache file too small")
	}

	magic := string(data[0:4])
	if magic != sliceCacheMagic {
		return nil, fmt.Errorf("invalid magic: %s", magic)
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != sliceCacheVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	sliceCount := binary.LittleEndian.Uint32(data[8:12])
	expectedSize := sliceCacheHeader + int(sliceCount)*sliceCacheEntry
	if len(data) < expectedSize {
		return nil, fmt.Errorf("cache file truncated: expected %d bytes, got %d", expectedSize, len(data))
	}

	slices := make([]SliceInfo, sliceCount)
	offset := sliceCacheHeader

	for i := uint32(0); i < sliceCount; i++ {
		entry := data[offset : offset+sliceCacheEntry]
		slices[i] = SliceInfo{
			SliceNum:       binary.LittleEndian.Uint32(entry[0:4]),
			StartBlock:     binary.LittleEndian.Uint32(entry[4:8]),
			EndBlock:       binary.LittleEndian.Uint32(entry[8:12]),
			MaxBlock:       binary.LittleEndian.Uint32(entry[12:16]),
			BlocksPerSlice: binary.LittleEndian.Uint32(entry[16:20]),
			Finalized:      entry[20] != 0,
			GlobMin:        binary.LittleEndian.Uint64(entry[24:32]),
			GlobMax:        binary.LittleEndian.Uint64(entry[32:40]),
		}
		offset += sliceCacheEntry
	}

	return slices, nil
}

func SaveSlicesToCache(basePath string, slices []SliceInfo) error {
	cachePath := filepath.Join(basePath, "slices.cache")
	tmpPath := cachePath + ".tmp"

	bufSize := sliceCacheHeader + len(slices)*sliceCacheEntry
	buf := make([]byte, bufSize)

	copy(buf[0:4], []byte(sliceCacheMagic))
	binary.LittleEndian.PutUint32(buf[4:8], sliceCacheVersion)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(slices)))

	offset := sliceCacheHeader
	for _, slice := range slices {
		entry := buf[offset : offset+sliceCacheEntry]
		binary.LittleEndian.PutUint32(entry[0:4], slice.SliceNum)
		binary.LittleEndian.PutUint32(entry[4:8], slice.StartBlock)
		binary.LittleEndian.PutUint32(entry[8:12], slice.EndBlock)
		binary.LittleEndian.PutUint32(entry[12:16], slice.MaxBlock)
		binary.LittleEndian.PutUint32(entry[16:20], slice.BlocksPerSlice)
		if slice.Finalized {
			entry[20] = 1
		}
		binary.LittleEndian.PutUint64(entry[24:32], slice.GlobMin)
		binary.LittleEndian.PutUint64(entry[32:40], slice.GlobMax)
		offset += sliceCacheEntry
	}

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp cache file: %w", err)
	}

	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write cache: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync cache: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close cache: %w", err)
	}

	if err := os.Rename(tmpPath, cachePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename cache: %w", err)
	}

	return nil
}

func scanSlicesFromFilesystem(basePath string) ([]SliceInfo, error) {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var slices []SliceInfo
	const blocksPerSlice = 10000 // Standard slice size

	logger.Printf("config", "Scanning storage directory (%d entries)...", len(entries))

	scannedCount := 0
	lastLogTime := time.Now()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		var startBlock, maxBlock uint32
		n, err := fmt.Sscanf(entry.Name(), "history_%010d-%010d", &startBlock, &maxBlock)
		if err != nil || n != 2 {
			continue // Not a slice directory
		}

		slicePath := filepath.Join(basePath, entry.Name())

		blockIndexPath := filepath.Join(slicePath, "blocks.index")
		endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
		if err != nil {
			logger.Printf("warning", "Failed to read blocks.index for slice %s: %v (skipping)", entry.Name(), err)
			continue
		}

		sliceNum := startBlock / blocksPerSlice
		finalized := false
		dataLogPath := filepath.Join(slicePath, "data.log")
		if header, err := ReadDataLogHeader(dataLogPath); err == nil {
			finalized = header.IsFinalized()
		}

		slices = append(slices, SliceInfo{
			SliceNum:       sliceNum,
			StartBlock:     startBlock,
			EndBlock:       endBlock,
			MaxBlock:       maxBlock,
			BlocksPerSlice: blocksPerSlice,
			Finalized:      finalized,
			GlobMin:        globMin,
			GlobMax:        globMax,
		})

		scannedCount++
		now := time.Now()
		if scannedCount%1000 == 0 || now.Sub(lastLogTime) >= 5*time.Second {
			logger.Printf("config", "  Scanned %d slices...", scannedCount)
			lastLogTime = now
		}
	}

	if len(slices) == 0 {
		return nil, fmt.Errorf("no slice directories found")
	}

	logger.Printf("config", "Storage scan complete: %d slices found", len(slices))
	return slices, nil
}

func findLastBlockInIndex(indexPath string) (uint32, uint64, uint64, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return 0, 0, 0, err
	}

	if len(data) < 4 {
		return 0, 0, 0, fmt.Errorf("index file too small")
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	if count == 0 {
		return 0, 0, 0, fmt.Errorf("index is empty")
	}

	entrySize := 32
	expectedSize := 4 + int(count)*entrySize

	if len(data) < expectedSize {
		count = uint32((len(data) - 4) / entrySize)
		if count == 0 {
			return 0, 0, 0, fmt.Errorf("no complete entries in index")
		}
	}

	lastEntryOffset := 4 + (int(count)-1)*entrySize
	if lastEntryOffset+entrySize > len(data) {
		return 0, 0, 0, fmt.Errorf("index truncated")
	}

	entry := data[lastEntryOffset : lastEntryOffset+entrySize]
	endBlock := binary.LittleEndian.Uint32(entry[0:4])
	lastGlobMax := binary.LittleEndian.Uint64(entry[24:32])

	firstEntry := data[4 : 4+entrySize]
	firstGlobMin := binary.LittleEndian.Uint64(firstEntry[16:24])

	if lastGlobMax > 1<<40 || firstGlobMin > 1<<40 {
		logger.Printf("error", "Suspicious values in %s: count=%d, lastEntry bytes[16:32]=%x, firstEntry bytes[16:24]=%x",
			indexPath, count, entry[16:32], firstEntry[16:24])
		logger.Printf("error", "Parsed: endBlock=%d, firstGlobMin=%d, lastGlobMax=%d",
			endBlock, firstGlobMin, lastGlobMax)
	}

	return endBlock, firstGlobMin, lastGlobMax, nil
}

func loadSlicesFromJSON(basePath string) ([]SliceInfo, error) {
	slicesPath := filepath.Join(basePath, "slices.json")
	data, err := os.ReadFile(slicesPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read slices.json: %w", err)
	}

	var sliceInfos []SliceInfo
	if err := json.Unmarshal(data, &sliceInfos); err != nil {
		return nil, fmt.Errorf("failed to parse slices.json: %w", err)
	}

	return sliceInfos, nil
}

// buildGlobRangeIndexFromSlices creates glob range index from SliceInfo array
// without reading any files (all data already in memory).
func buildGlobRangeIndexFromSlices(sharedMetadata *SharedSliceMetadata) *GlobRangeIndex {
	sliceInfos := sharedMetadata.getSlices() // Get copy for processing
	startTime := time.Now()

	if len(sliceInfos) < 3 {
		return nil
	}

	// Exclude last 2 active slices (same logic as loadGlobRangeIndex)
	finalizedSlices := sliceInfos[:len(sliceInfos)-2]
	highestIndexedSlice := finalizedSlices[len(finalizedSlices)-1].SliceNum

	ranges := make([]SliceGlobRange, 0, len(finalizedSlices))

	for _, slice := range finalizedSlices {
		if slice.GlobMin == 0 && slice.GlobMax == 0 {
			continue
		}

		ranges = append(ranges, SliceGlobRange{
			SliceNum: slice.SliceNum,
			MinGlob:  slice.GlobMin,
			MaxGlob:  slice.GlobMax,
		})
	}

	memoryUsedKB := int64(len(ranges) * 20 / 1024)
	loadTime := time.Since(startTime)

	return &GlobRangeIndex{
		ranges:              ranges,
		highestIndexedSlice: highestIndexedSlice,
		totalSlices:         len(ranges),
		memoryUsedKB:        memoryUsedKB,
		loadTime:            loadTime,
		lastUpdateCheck:     time.Now(),
		lastStatsSliceCount: len(ranges),
		updateCooldown:      5 * time.Second,
	}
}

func loadGlobRangeIndex(basePath string, sliceInfos []SliceInfo) (*GlobRangeIndex, error) {
	startTime := time.Now()

	if len(sliceInfos) < 3 {
		return nil, fmt.Errorf("need at least 3 slices (have %d)", len(sliceInfos))
	}

	finalizedSlices := sliceInfos[:len(sliceInfos)-2]
	highestIndexedSlice := finalizedSlices[len(finalizedSlices)-1].SliceNum

	ranges := make([]SliceGlobRange, 0, len(finalizedSlices))

	loadedCount := 0
	skippedCount := 0
	lastLogTime := startTime

	for _, slice := range finalizedSlices {
		slicePath := filepath.Join(basePath,
			fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
		blockIndexPath := filepath.Join(slicePath, "blocks.index")

		minGlob, maxGlob, err := readGlobRangeFromBlockIndex(blockIndexPath)
		if err != nil {
			logger.Printf("warning", "Failed to read glob range for slice %d: %v (skipping)",
				slice.SliceNum, err)
			skippedCount++
			continue
		}

		ranges = append(ranges, SliceGlobRange{
			SliceNum: slice.SliceNum,
			MinGlob:  minGlob,
			MaxGlob:  maxGlob,
		})

		loadedCount++

		now := time.Now()
		if loadedCount%1000 == 0 || now.Sub(lastLogTime) >= 5*time.Second {
			pct := float64(loadedCount) / float64(len(finalizedSlices)) * 100.0
			elapsed := now.Sub(startTime)
			rate := float64(loadedCount) / elapsed.Seconds()
			remaining := time.Duration(float64(len(finalizedSlices)-loadedCount) / rate * float64(time.Second))
			logger.Printf("config", "  Loading glob range index: %d/%d slices (%.1f%%) | %.0f slices/sec | ETA: %v",
				loadedCount, len(finalizedSlices), pct, rate, remaining.Round(time.Second))
			lastLogTime = now
		}
	}

	memoryUsedKB := int64(len(ranges) * 20 / 1024)

	loadTime := time.Since(startTime)

	if skippedCount > 0 {
		logger.Printf("warning", "Skipped %d slices due to load errors", skippedCount)
	}

	return &GlobRangeIndex{
		ranges:              ranges,
		highestIndexedSlice: highestIndexedSlice,
		totalSlices:         loadedCount,
		memoryUsedKB:        memoryUsedKB,
		loadTime:            loadTime,
		lastUpdateCheck:     time.Now(),
		lastStatsSliceCount: loadedCount,
		updateCooldown:      5 * time.Second,
	}, nil
}

func readGlobRangeFromBlockIndex(blockIndexPath string) (minGlob, maxGlob uint64, err error) {
	file, err := os.Open(blockIndexPath)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	countBuf := make([]byte, 4)
	if _, err := file.Read(countBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to read count: %w", err)
	}
	count := binary.LittleEndian.Uint32(countBuf)

	if count == 0 {
		return 0, 0, fmt.Errorf("index is empty")
	}

	const entrySize = 32
	const globMinOffset = 16 // offset within entry to GlobMin
	const globMaxOffset = 24 // offset within entry to GlobMax

	firstEntryBuf := make([]byte, entrySize)
	if _, err := file.Read(firstEntryBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to read first entry: %w", err)
	}
	minGlob = binary.LittleEndian.Uint64(firstEntryBuf[globMinOffset:])

	if count == 1 {
		maxGlob = binary.LittleEndian.Uint64(firstEntryBuf[globMaxOffset:])
		return minGlob, maxGlob, nil
	}

	lastEntryOffset := 4 + int64(count-1)*entrySize
	if _, err := file.Seek(lastEntryOffset, 0); err != nil {
		return 0, 0, fmt.Errorf("failed to seek to last entry: %w", err)
	}

	lastEntryBuf := make([]byte, entrySize)
	if _, err := file.Read(lastEntryBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to read last entry: %w", err)
	}
	maxGlob = binary.LittleEndian.Uint64(lastEntryBuf[globMaxOffset:])

	return minGlob, maxGlob, nil
}

func (sr *SliceReader) Close() error {
	sr.closeOnce.Do(func() {
		close(sr.stopStatsTicker)

		if sr.blockIndexCache != nil {
			if err := sr.blockIndexCache.Close(); err != nil {
				logger.Printf("warning", "BlockIndexCache close error: %v", err)
			}
		}

		sr.sliceReadersMu.Lock()
		defer sr.sliceReadersMu.Unlock()

		for _, reader := range sr.sliceReaders {
			reader.close()
		}
		sr.sliceReaders = make(map[uint32]*sliceReader)
	})
	return nil
}

func (sr *SliceReader) GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	return sr.blockCache.Stats()
}

func (sr *SliceReader) GetBlockIndexCache() *BlockIndexCache {
	return sr.blockIndexCache
}

func (sr *SliceReader) RefreshSliceMetadata() error {
	sliceCount := sr.sharedMetadata.getSliceCount()
	if sliceCount == 0 {
		return fmt.Errorf("no slices loaded")
	}

	lastSlice := sr.sharedMetadata.getSlice(sliceCount - 1)

	slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", lastSlice.StartBlock, lastSlice.MaxBlock))
	blockIndexPath := filepath.Join(slicePath, "blocks.index")

	endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
	if err != nil {
		logger.Printf("warning", "Failed to refresh last slice metadata: %v", err)
		return err
	}

	logger.Printf("sync", "Slice metadata read from %s: endBlock=%d, globMin=%d, globMax=%d",
		blockIndexPath, endBlock, globMin, globMax)

	if globMin > globMax {
		logger.Printf("error", "Invalid slice metadata: globMin (%d) > globMax (%d) from %s",
			globMin, globMax, blockIndexPath)
	}
	if globMax > 1<<40 {
		logger.Printf("error", "Suspicious slice metadata: globMax=%d seems too large (path: %s)",
			globMax, blockIndexPath)
	}

	oldEndBlock := lastSlice.EndBlock
	oldGlobMin := lastSlice.GlobMin
	oldGlobMax := lastSlice.GlobMax
	if endBlock == oldEndBlock {
		return nil
	}

	sr.sharedMetadata.updateLastSliceEndBlock(endBlock, globMin, globMax)

	if sr.blockIndexCache != nil {
		if err := sr.blockIndexCache.LoadSliceFromBlocksIndex(slicePath, lastSlice.SliceNum, lastSlice.StartBlock, false); err != nil {
			logger.Printf("warning", "Failed to update blockIndexCache for slice %d: %v", lastSlice.SliceNum, err)
		}
	}

	logger.Printf("sync", "Slice %d metadata updated: EndBlock=%d→%d, GlobMin=%d→%d, GlobMax=%d→%d",
		lastSlice.SliceNum, oldEndBlock, endBlock, oldGlobMin, globMin, oldGlobMax, globMax)

	sliceKey := (lastSlice.StartBlock - 1) / lastSlice.BlocksPerSlice
	sr.sliceMapByBlock[sliceKey] = sliceCount - 1

	sr.lastAccessedSliceIdx.Store(-1)

	logger.Printf("sync", "Slice %d EndBlock updated: %d → %d (index will auto-reload)",
		lastSlice.SliceNum, oldEndBlock, endBlock)
	return nil
}

func (sr *SliceReader) GetSliceInfos() []SliceInfo {
	return sr.sharedMetadata.getSlices()
}

type SliceBlockStats struct {
	SliceNum        uint32  `json:"slice_num"`
	StartBlock      uint32  `json:"start_block"`
	EndBlock        uint32  `json:"end_block"`
	BlockCount      int     `json:"block_count"`
	TotalSizeBytes  int64   `json:"total_size_bytes"`
	TotalActions    int64   `json:"total_actions"`
	AvgBlockSize    float64 `json:"avg_block_size_bytes"`
	AvgActionsBlock float64 `json:"avg_actions_per_block"`
	MinBlockSize    uint32  `json:"min_block_size"`
	MaxBlockSize    uint32  `json:"max_block_size"`
	MinActions      int     `json:"min_actions_per_block"`
	MaxActions      int     `json:"max_actions_per_block"`
	MinActionsBlock uint32  `json:"min_actions_block"`
	MaxActionsBlock uint32  `json:"max_actions_block"`
	MinSizeBlock    uint32  `json:"min_size_block"`
	MaxSizeBlock    uint32  `json:"max_size_block"`
	Finalized       bool    `json:"finalized"`
}

func (sr *SliceReader) GetSliceBlockStats(sliceNum uint32) (*SliceBlockStats, error) {
	sliceInfo := sr.getSliceInfoByNum(sliceNum)
	if sliceInfo == nil {
		return nil, fmt.Errorf("slice %d not found", sliceNum)
	}

	reader, err := sr.getSliceReader(sliceNum)
	if err != nil {
		return nil, fmt.Errorf("failed to load slice reader: %w", err)
	}
	defer sr.releaseSliceReader(sliceNum)

	stats := &SliceBlockStats{
		SliceNum:   sliceNum,
		StartBlock: sliceInfo.StartBlock,
		EndBlock:   sliceInfo.EndBlock,
		Finalized:  sliceInfo.Finalized,
	}

	if len(reader.blockIndex) == 0 {
		return stats, nil
	}

	stats.BlockCount = len(reader.blockIndex)
	stats.MinBlockSize = ^uint32(0)
	stats.MinActions = int(^uint32(0))

	for blockNum, entry := range reader.blockIndex {
		actualBlockNum := stats.StartBlock + uint32(blockNum)
		stats.TotalSizeBytes += int64(entry.Size)
		actions := int(entry.GlobMax - entry.GlobMin + 1)
		stats.TotalActions += int64(actions)

		if entry.Size < stats.MinBlockSize {
			stats.MinBlockSize = entry.Size
			stats.MinSizeBlock = actualBlockNum
		}
		if entry.Size > stats.MaxBlockSize {
			stats.MaxBlockSize = entry.Size
			stats.MaxSizeBlock = actualBlockNum
		}
		if actions < stats.MinActions {
			stats.MinActions = actions
			stats.MinActionsBlock = actualBlockNum
		}
		if actions > stats.MaxActions {
			stats.MaxActions = actions
			stats.MaxActionsBlock = actualBlockNum
		}
	}

	if stats.BlockCount > 0 {
		stats.AvgBlockSize = float64(stats.TotalSizeBytes) / float64(stats.BlockCount)
		stats.AvgActionsBlock = float64(stats.TotalActions) / float64(stats.BlockCount)
	}

	if stats.MinBlockSize == ^uint32(0) {
		stats.MinBlockSize = 0
	}
	if stats.MinActions == int(^uint32(0)) {
		stats.MinActions = 0
	}

	return stats, nil
}

func (sr *SliceReader) FindFirstAvailableBlock(afterBlock uint32) (uint32, bool) {
	sliceCount := sr.sharedMetadata.getSliceCount()
	if sliceCount == 0 {
		return 0, false
	}
	for i := 0; i < sliceCount; i++ {
		slice := sr.sharedMetadata.getSlice(i)
		if slice.EndBlock < afterBlock {
			continue
		}
		if slice.StartBlock <= afterBlock {
			return afterBlock, true
		}
		return slice.StartBlock, true
	}
	// afterBlock is beyond all slices - return it as-is (we're caught up)
	return afterBlock, true
}

func (sr *SliceReader) GetSliceHeader(sliceNum uint32) (*DataLogHeader, error) {
	reader, err := sr.getSliceReader(sliceNum)
	if err != nil {
		return nil, err
	}
	defer sr.releaseSliceReader(sliceNum)
	return reader.header, nil
}

func (sr *SliceReader) IsSliceFinalized(sliceNum uint32) (bool, error) {
	header, err := sr.GetSliceHeader(sliceNum)
	if err != nil {
		return false, err
	}
	if header == nil {
		return false, nil
	}
	return header.IsFinalized(), nil
}

func (sr *SliceReader) findSliceForBlock(blockNum uint32) (*SliceInfo, error) {
	cachedIdx := int(sr.lastAccessedSliceIdx.Load())
	if cachedIdx >= 0 {
		slice := sr.sharedMetadata.getSlice(cachedIdx)
		if slice.EndBlock > 0 && blockNum >= slice.StartBlock && blockNum <= slice.EndBlock {
			sr.cacheHits.Add(1)
			return &slice, nil
		} else if blockNum > slice.EndBlock && blockNum <= slice.EndBlock+10 {
			sr.cacheMisses.Add(1)
		} else {
			sr.cacheMisses.Add(1)
		}
	} else {
		sr.cacheMisses.Add(1)
	}

	sliceCount := sr.sharedMetadata.getSliceCount()
	if sliceCount == 0 {
		return nil, fmt.Errorf("no slices available")
	}

	firstSlice := sr.sharedMetadata.getSlice(0)
	blocksPerSlice := firstSlice.BlocksPerSlice
	sliceKey := (blockNum - 1) / blocksPerSlice // -1 because blocks start at 1

	sliceIdx, found := sr.sliceMapByBlock[sliceKey]
	if !found {
		sliceStartBlock := sliceKey*blocksPerSlice + 1
		sliceMaxBlock := (sliceKey + 1) * blocksPerSlice

		slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", sliceStartBlock, sliceMaxBlock))
		if _, err := os.Stat(slicePath); err == nil {
			blockIndexPath := filepath.Join(slicePath, "blocks.index")
			endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
			if err != nil {
				return nil, fmt.Errorf("block %d in new slice %d but index not ready", blockNum, sliceKey)
			}

			if existingIdx, exists := sr.sliceMapByBlock[sliceKey]; exists {
				sr.lastAccessedSliceIdx.Store(int32(existingIdx))
				slice := sr.sharedMetadata.getSlice(existingIdx)
				return &slice, nil
			}

			newSlice := SliceInfo{
				SliceNum:       sliceKey,
				StartBlock:     sliceStartBlock,
				MaxBlock:       sliceMaxBlock,
				EndBlock:       endBlock,
				GlobMin:        globMin,
				GlobMax:        globMax,
				BlocksPerSlice: blocksPerSlice,
			}

			newIdx := sr.sharedMetadata.appendSlice(newSlice)
			sr.sliceMapByBlock[sliceKey] = newIdx
			sr.lastAccessedSliceIdx.Store(int32(newIdx))

			if sr.blockIndexCache != nil {
				if err := sr.blockIndexCache.LoadSliceFromBlocksIndex(slicePath, sliceKey, sliceStartBlock, false); err != nil {
					logger.Printf("debug", "BlockIndexCache: failed to load new slice %d: %v", sliceKey, err)
				}
			}

			logger.Printf("sync", "Detected new slice %d (blocks %d-%d, EndBlock=%d) - added to reader",
				sliceKey, sliceStartBlock, sliceMaxBlock, endBlock)

			return &newSlice, nil
		}

		// Search all slices for the block
		for i := 0; i < sliceCount; i++ {
			info := sr.sharedMetadata.getSlice(i)
			if info.EndBlock > 0 && blockNum >= info.StartBlock && blockNum <= info.EndBlock {
				sr.lastAccessedSliceIdx.Store(int32(i))
				return &info, nil
			}
		}
		return nil, fmt.Errorf("block %d not found in any slice", blockNum)
	}

	slice := sr.sharedMetadata.getSlice(sliceIdx)

	if slice.EndBlock == 0 || blockNum < slice.StartBlock || blockNum > slice.EndBlock {
		logger.Printf("debug", "Block %d needs metadata refresh for slice %d (EndBlock=%d, range %d-%d)",
			blockNum, slice.SliceNum, slice.EndBlock, slice.StartBlock, slice.EndBlock)

		slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
		blockIndexPath := filepath.Join(slicePath, "blocks.index")
		endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
		if err != nil {
			if slice.EndBlock == 0 {
				return nil, fmt.Errorf("block %d: slice %d has stale metadata and index not readable: %w", blockNum, slice.SliceNum, err)
			}
			return nil, fmt.Errorf("block %d outside slice %d range (%d-%d)", blockNum, slice.SliceNum, slice.StartBlock, slice.EndBlock)
		}

		if blockNum > endBlock {
			return nil, fmt.Errorf("block %d outside slice %d range (%d-%d)", blockNum, slice.SliceNum, slice.StartBlock, endBlock)
		}

		oldEndBlock := slice.EndBlock
		updatedSlice := slice
		updatedSlice.EndBlock = endBlock
		updatedSlice.GlobMin = globMin
		updatedSlice.GlobMax = globMax
		sr.sharedMetadata.updateSlice(sliceIdx, updatedSlice)
		logger.Printf("debug", "Updated slice %d EndBlock: %d → %d (index will auto-reload on-demand)",
			slice.SliceNum, oldEndBlock, endBlock)
		slice = updatedSlice
	}

	sr.lastAccessedSliceIdx.Store(int32(sliceIdx))
	return &slice, nil
}

func (sr *SliceReader) statsReporterLoop() {
	ticker := time.NewTicker(time.Duration(sr.statsReportInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.reportAndResetCacheStats()
		case <-sr.stopStatsTicker:
			return
		}
	}
}

func (sr *SliceReader) reportAndResetCacheStats() {
	hits := sr.cacheHits.Swap(0)     // Read and reset atomically
	misses := sr.cacheMisses.Swap(0) // Read and reset atomically
	total := hits + misses

	if total == 0 {
		return // No activity in this interval
	}

	hitRate := float64(hits) / float64(total) * 100.0

	logger.Printf("debug-stats", "SliceReader cache: %d hits, %d misses (%.1f%% hit rate) over %ds",
		hits, misses, hitRate, sr.statsReportInterval)

	if sr.blockCache != nil {
		bcHits, bcMisses, bcSize, bcBlocks := sr.blockCache.Stats()
		bcTotal := bcHits + bcMisses
		if bcTotal > 0 {
			bcHitRate := float64(bcHits) / float64(bcTotal) * 100.0
			logger.Printf("debug-stats", "BlockCache: %d hits, %d misses (%.1f%% hit rate), %d blocks, %.1f MB",
				bcHits, bcMisses, bcHitRate, bcBlocks, float64(bcSize)/(1024*1024))
		}
	}

}

func (sr *SliceReader) findSliceForGlob(glob uint64) (*SliceInfo, error) {
	sliceInfo, _, err := sr.findSliceForGlobTracked(glob)
	return sliceInfo, err
}

func (sr *SliceReader) findSliceForGlobTracked(glob uint64) (*SliceInfo, bool, error) {
	sliceInfo, _ := sr.sharedMetadata.findSliceForGlob(glob)
	if sliceInfo != nil {
		return sliceInfo, false, nil
	}

	sr.sliceDiscoveryMu.Lock()

	sliceInfo, _ = sr.sharedMetadata.findSliceForGlob(glob)
	if sliceInfo != nil {
		sr.sliceDiscoveryMu.Unlock()
		return sliceInfo, false, nil
	}

	if sr.sliceDiscovering == nil {
		sr.sliceDiscovering = make(map[uint32]chan struct{})
	}

	globKey := uint32(glob >> 32)
	if waitCh, discovering := sr.sliceDiscovering[globKey]; discovering {
		sr.sliceDiscoveryMu.Unlock()
		<-waitCh
		sliceInfo, _ = sr.sharedMetadata.findSliceForGlob(glob)
		if sliceInfo != nil {
			return sliceInfo, false, nil
		}
	}

	doneCh := make(chan struct{})
	sr.sliceDiscovering[globKey] = doneCh
	sr.sliceDiscoveryMu.Unlock()

	defer func() {
		sr.sliceDiscoveryMu.Lock()
		delete(sr.sliceDiscovering, globKey)
		sr.sliceDiscoveryMu.Unlock()
		close(doneCh)
	}()

	sliceInfo, err := sr.findSliceForGlobLegacy(glob)
	if err != nil {
		return nil, true, err
	}

	if sliceInfo != nil && sliceInfo.GlobMin > 0 && sliceInfo.GlobMax > 0 {
		if sr.sharedMetadata.addSlice(*sliceInfo) {
			logger.Printf("sync", "Added newly discovered slice %d to cache (glob range %d-%d)",
				sliceInfo.SliceNum, sliceInfo.GlobMin, sliceInfo.GlobMax)
		}
	}

	return sliceInfo, true, nil
}

func (sr *SliceReader) findSliceForGlobWithRangeIndex(glob uint64) (*SliceInfo, error) {
	sliceInfo, _, err := sr.findSliceForGlobWithRangeIndexTracked(glob)
	return sliceInfo, err
}

func (sr *SliceReader) findSliceForGlobWithRangeIndexTracked(glob uint64) (*SliceInfo, bool, error) {
	sr.globRangeIndex.mu.RLock()
	defer sr.globRangeIndex.mu.RUnlock()

	if len(sr.globRangeIndex.ranges) > 0 {
		lastIndexedRange := sr.globRangeIndex.ranges[len(sr.globRangeIndex.ranges)-1]
		if glob > lastIndexedRange.MaxGlob {
			sliceInfo, err := sr.findSliceForGlobFromDisk(glob, sr.globRangeIndex.highestIndexedSlice+1)
			if err != nil {
				logger.Printf("warning", "Glob %d not found in unindexed slices, falling back to full search", glob)
				sliceInfo, err := sr.findSliceForGlobLegacy(glob)
				return sliceInfo, true, err
			}
			return sliceInfo, true, nil // hit disk for unindexed slice
		}
	}

	idx := sort.Search(len(sr.globRangeIndex.ranges), func(i int) bool {
		return sr.globRangeIndex.ranges[i].MaxGlob >= glob
	})

	if idx < len(sr.globRangeIndex.ranges) {
		sliceRange := sr.globRangeIndex.ranges[idx]

		if glob >= sliceRange.MinGlob && glob <= sliceRange.MaxGlob {
			return sr.getSliceInfoByNum(sliceRange.SliceNum), false, nil // range index hit
		}
	}

	return nil, false, fmt.Errorf("glob %d not found in any slice", glob)
}

func (sr *SliceReader) findSliceForGlobFromDisk(glob uint64, startSliceNum uint32) (*SliceInfo, error) {
	sliceCount := sr.sharedMetadata.getSliceCount()
	if sliceCount == 0 {
		return nil, fmt.Errorf("no slices available")
	}

	logger.Printf("debug", "Searching for glob %d in unindexed slices (startSliceNum=%d)", glob, startSliceNum)
	logger.Printf("debug", "Total slices available: %d", sliceCount)

	checkedCount := 0
	for i := sliceCount - 1; i >= 0; i-- {
		slice := sr.sharedMetadata.getSlice(i)

		if slice.SliceNum < startSliceNum {
			break
		}

		checkedCount++
		logger.Printf("debug", "Checking slice %d (num=%d, blocks=%d-%d, finalized=%v)", i, slice.SliceNum, slice.StartBlock, slice.EndBlock, slice.Finalized)

		if !slice.Finalized && slice.GlobMax > 0 {
			cachedMin, cachedMax := slice.GlobMin, slice.GlobMax
			logger.Printf("debug", "Slice %d (non-finalized) cached glob range: %d-%d (looking for %d)", slice.SliceNum, cachedMin, cachedMax, glob)

			if glob > cachedMax {
				slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
				blockIndexPath := filepath.Join(slicePath, "blocks.index")
				_, freshGlobMin, freshGlobMax, err := findLastBlockInIndex(blockIndexPath)
				if err == nil && freshGlobMax > cachedMax {
					logger.Printf("debug", "Refreshed slice %d glob range: %d-%d → %d-%d",
						slice.SliceNum, cachedMin, cachedMax, freshGlobMin, freshGlobMax)
					sr.sharedMetadata.updateLastSliceGlobRange(freshGlobMin, freshGlobMax)
					cachedMin, cachedMax = freshGlobMin, freshGlobMax
				}
			}

			if glob >= cachedMin && glob <= cachedMax {
				logger.Printf("debug", "Found glob %d in non-finalized slice %d (using cached range)", glob, slice.SliceNum)
				return &slice, nil
			}
			continue
		}

		reader, err := sr.getSliceReader(slice.SliceNum)
		if err != nil {
			logger.Printf("debug", "Failed to load slice %d reader: %v", slice.SliceNum, err)
			continue
		}
		globMin, globMax := reader.getGlobRangeFromBlockIndex()
		sr.releaseSliceReader(slice.SliceNum)
		if globMin == 0 && globMax == 0 {
			logger.Printf("debug", "Slice %d has empty block index", slice.SliceNum)
			continue
		}

		logger.Printf("debug", "Slice %d glob range: %d-%d (looking for %d)", slice.SliceNum, globMin, globMax, glob)

		if glob >= globMin && glob <= globMax {
			logger.Printf("debug", "Found glob %d in slice %d", glob, slice.SliceNum)
			return &slice, nil
		}
	}

	logger.Printf("debug", "Checked %d slices, glob %d not found", checkedCount, glob)
	return nil, fmt.Errorf("glob %d not found in any slice (checked %d slices)", glob, checkedCount)
}

func (sr *SliceReader) findSliceForGlobLegacy(glob uint64) (*SliceInfo, error) {
	slicesCopy := sr.sharedMetadata.getSlices()

	logger.Printf("warning", "Legacy binary search for glob %d across %d slices", glob, len(slicesCopy))

	left, right := 0, len(slicesCopy)-1
	iteration := 0

	for left <= right {
		iteration++
		mid := (left + right) / 2
		slice := &slicesCopy[mid]

		var globMin, globMax uint64

		if !slice.Finalized && slice.GlobMax > 0 {
			globMin = slice.GlobMin
			globMax = slice.GlobMax

			if globMin > globMax || globMax > 1<<40 {
				logger.Printf("error", "Slice %d has invalid cached glob range [%d-%d] (looking for %d)",
					slice.SliceNum, globMin, globMax, glob)
			}

			if glob > globMax && mid == len(slicesCopy)-1 {
				slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
				blockIndexPath := filepath.Join(slicePath, "blocks.index")
				_, freshGlobMin, freshGlobMax, err := findLastBlockInIndex(blockIndexPath)
				if err == nil && freshGlobMax > globMax {
					logger.Printf("warning", "Slice %d cache stale, reloaded from disk: [%d-%d] -> [%d-%d]",
						slice.SliceNum, globMin, globMax, freshGlobMin, freshGlobMax)
					globMin = freshGlobMin
					globMax = freshGlobMax

					sr.sharedMetadata.updateLastSliceGlobRange(freshGlobMin, freshGlobMax)
				}
			}

			logger.Printf("warning", "Slice %d (non-finalized) cached range [%d-%d], looking for %d",
				slice.SliceNum, globMin, globMax, glob)
		} else {
			reader, err := sr.getSliceReader(slice.SliceNum)
			if err != nil {
				logger.Printf("warning", "Slice %d failed to load, skipping: %v", slice.SliceNum, err)
				left = mid + 1
				continue
			}
			globMin, globMax = reader.getGlobRangeFromBlockIndex()
			sr.releaseSliceReader(slice.SliceNum)
			if globMin == 0 && globMax == 0 {
				logger.Printf("warning", "Slice %d has empty block index, skipping", slice.SliceNum)
				left = mid + 1
				continue
			}

			logger.Printf("warning", "Slice %d range [%d-%d], looking for %d",
				slice.SliceNum, globMin, globMax, glob)
		}

		if glob < globMin {
			logger.Printf("warning", "Glob %d < min, searching left (right=%d->%d)", glob, right, mid-1)
			right = mid - 1
		} else if glob > globMax {
			logger.Printf("warning", "Glob %d > max, searching right (left=%d->%d)", glob, left, mid+1)
			left = mid + 1
		} else {
			logger.Printf("warning", "Found glob %d in slice %d", glob, slice.SliceNum)
			result := slicesCopy[mid]
			result.GlobMin = globMin
			result.GlobMax = globMax
			return &result, nil
		}
	}

	logger.Printf("warning", "Legacy search exhausted after %d iterations, glob %d not found", iteration, glob)
	return nil, fmt.Errorf("glob %d not found in any slice", glob)
}

func (sr *SliceReader) getSliceInfoByNum(sliceNum uint32) *SliceInfo {
	sr.sharedMetadata.mu.RLock()
	defer sr.sharedMetadata.mu.RUnlock()

	if idx, ok := sr.sharedMetadata.sliceIndex[sliceNum]; ok {
		slice := sr.sharedMetadata.slices[idx]
		return &slice
	}
	return nil
}

func (sr *SliceReader) UpdateGlobRangeIndexWithNewSlices() error {
	if sr.globRangeIndex == nil {
		return nil // No range index loaded
	}

	sr.globRangeIndex.updateMu.Lock()
	defer sr.globRangeIndex.updateMu.Unlock()

	if time.Since(sr.globRangeIndex.lastUpdateCheck) < sr.globRangeIndex.updateCooldown {
		return nil
	}
	sr.globRangeIndex.lastUpdateCheck = time.Now()

	sliceInfos, err := scanSlicesFromFilesystem(sr.basePath)
	if err != nil {
		return fmt.Errorf("failed to rescan slices: %w", err)
	}

	if len(sliceInfos) < 3 {
		return nil // Need at least 3 slices
	}

	sort.Slice(sliceInfos, func(i, j int) bool {
		return sliceInfos[i].SliceNum < sliceInfos[j].SliceNum
	})

	maxIndexable := sliceInfos[len(sliceInfos)-3].SliceNum // N-2 (last 2 are active)

	if maxIndexable <= sr.globRangeIndex.highestIndexedSlice {
		return nil // No new slices to index
	}

	newRanges := make([]SliceGlobRange, 0, 10) // Typically only 1-2 new slices per update
	newSliceCount := 0

	for _, slice := range sliceInfos {
		if slice.SliceNum <= sr.globRangeIndex.highestIndexedSlice {
			continue
		}
		if slice.SliceNum > maxIndexable {
			break
		}

		slicePath := filepath.Join(sr.basePath,
			fmt.Sprintf("history_%010d-%010d", slice.StartBlock, slice.MaxBlock))
		blockIndexPath := filepath.Join(slicePath, "blocks.index")

		minGlob, maxGlob, err := readGlobRangeFromBlockIndex(blockIndexPath)
		if err != nil {
			logger.Printf("warning", "Failed to read glob range for newly finalized slice %d: %v",
				slice.SliceNum, err)
			continue
		}

		newRanges = append(newRanges, SliceGlobRange{
			SliceNum: slice.SliceNum,
			MinGlob:  minGlob,
			MaxGlob:  maxGlob,
		})

		newSliceCount++
	}

	if len(newRanges) == 0 {
		return nil // Nothing to add
	}

	sr.globRangeIndex.mu.Lock()
	sr.globRangeIndex.ranges = append(sr.globRangeIndex.ranges, newRanges...)

	sr.globRangeIndex.highestIndexedSlice = maxIndexable
	sr.globRangeIndex.totalSlices += newSliceCount
	sr.globRangeIndex.memoryUsedKB += int64(len(newRanges) * 20 / 1024)

	sr.globRangeIndex.mu.Unlock()

	oldSliceCount := sr.sharedMetadata.getSliceCount()

	// Replace entire slice array in shared metadata
	sr.sharedMetadata.mu.Lock()
	sr.sharedMetadata.slices = sliceInfos
	sr.sharedMetadata.mu.Unlock()

	// Rebuild sliceMapByBlock with new indices
	newMap := make(map[uint32]int)
	for i := range sliceInfos {
		key := sliceInfos[i].StartBlock / 10000
		newMap[key] = i
	}
	sr.sliceMapByBlock = newMap

	logger.Printf("config", "Glob range index updated: added %d slices (+%d KB), now indexed through slice %d",
		newSliceCount, len(newRanges)*20/1024, maxIndexable)
	logger.Printf("config", "SliceReader now tracks %d total slices (was %d, +%d)",
		len(sliceInfos), oldSliceCount, len(sliceInfos)-oldSliceCount)

	return nil
}

func (sr *SliceReader) GetGlobRangeIndexStats() *GlobRangeIndexStats {
	if sr.globRangeIndex == nil {
		return nil
	}

	sr.globRangeIndex.mu.RLock()
	defer sr.globRangeIndex.mu.RUnlock()

	slicesAdded := 0
	if sr.globRangeIndex.lastStatsSliceCount > 0 {
		slicesAdded = len(sr.globRangeIndex.ranges) - sr.globRangeIndex.lastStatsSliceCount
	}
	sr.globRangeIndex.lastStatsSliceCount = len(sr.globRangeIndex.ranges)

	var highestGlob uint64
	if len(sr.globRangeIndex.ranges) > 0 {
		highestGlob = sr.globRangeIndex.ranges[len(sr.globRangeIndex.ranges)-1].MaxGlob
	}

	return &GlobRangeIndexStats{
		IndexedSlices:           len(sr.globRangeIndex.ranges),
		HighestIndexed:          sr.globRangeIndex.highestIndexedSlice,
		HighestGlob:             highestGlob,
		MemoryKB:                sr.globRangeIndex.memoryUsedKB,
		LastUpdate:              sr.globRangeIndex.lastUpdateCheck,
		SlicesAddedSinceLastLog: slicesAdded,
	}
}

func (sr *SliceReader) SetGlobRangeUpdateCooldown(d time.Duration) {
	if sr.globRangeIndex != nil {
		sr.globRangeIndex.updateCooldown = d
	}
}

func (sr *SliceReader) isPinnedSlice(sliceNum uint32) bool {
	if sr.globRangeIndex == nil {
		return false
	}

	return sliceNum > sr.globRangeIndex.highestIndexedSlice
}

func (sr *SliceReader) getPinnedSliceReader(sliceNum uint32) (*sliceReader, error) {
	sr.pinnedSlicesMu.RLock()
	if reader, found := sr.pinnedSlices[sliceNum]; found {
		sr.pinnedSlicesMu.RUnlock()
		return reader, nil
	}
	sr.pinnedSlicesMu.RUnlock()

	sr.pinnedSlicesMu.Lock()
	defer sr.pinnedSlicesMu.Unlock()

	if reader, found := sr.pinnedSlices[sliceNum]; found {
		return reader, nil
	}

	reader, err := sr.loadSliceReaderFromDisk(sliceNum)
	if err != nil {
		return nil, err
	}

	sr.pinnedSlices[sliceNum] = reader
	return reader, nil
}

func (sr *SliceReader) loadSliceReaderFromDisk(sliceNum uint32) (*sliceReader, error) {
	sliceInfo := sr.sharedMetadata.getSliceByNum(sliceNum)
	if sliceInfo == nil {
		return nil, fmt.Errorf("slice %d not found in metadata", sliceNum)
	}

	slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", sliceInfo.StartBlock, sliceInfo.MaxBlock))
	logger.Printf("debug", "Loading slice %d from %s (EndBlock=%d, Finalized=%v)",
		sliceNum, slicePath, sliceInfo.EndBlock, sliceInfo.Finalized)

	reader, err := newSliceReader(slicePath, sliceNum)
	if err != nil {
		return nil, fmt.Errorf("failed to load slice %d: %w", sliceNum, err)
	}

	logger.Printf("debug", "Slice %d loaded: blockIndex has %d entries",
		sliceNum, len(reader.blockIndex))

	return reader, nil
}

func (sr *SliceReader) getSliceReader(sliceNum uint32) (*sliceReader, error) {
	if sr.isPinnedSlice(sliceNum) {
		return sr.getPinnedSliceReader(sliceNum)
	}

	sliceInfo := sr.sharedMetadata.getSliceByNum(sliceNum)
	if sliceInfo == nil {
		return nil, fmt.Errorf("slice %d not found in metadata", sliceNum)
	}

	if !sliceInfo.Finalized {
		logger.Printf("debug", "Slice %d is not finalized, reloading from disk", sliceNum)
		return sr.loadSliceReaderFromDisk(sliceNum)
	}

	sr.sliceReadersMu.RLock()
	if reader, found := sr.sliceReaders[sliceNum]; found {
		reader.refCount.Add(1)
		sr.sliceReadersMu.RUnlock()
		sr.sliceReadersMu.Lock()
		sr.updateCacheOrder(sliceNum)
		sr.sliceReadersMu.Unlock()
		return reader, nil
	}
	loadingChan, isLoading := sr.sliceReaderLoading[sliceNum]
	sr.sliceReadersMu.RUnlock()

	if isLoading {
		<-loadingChan
		sr.sliceReadersMu.RLock()
		reader, found := sr.sliceReaders[sliceNum]
		if found {
			reader.refCount.Add(1)
		}
		sr.sliceReadersMu.RUnlock()
		if found {
			return reader, nil
		}
	}

	sr.sliceReadersMu.Lock()
	if reader, found := sr.sliceReaders[sliceNum]; found {
		reader.refCount.Add(1)
		sr.updateCacheOrder(sliceNum)
		sr.sliceReadersMu.Unlock()
		return reader, nil
	}
	if loadingChan, isLoading = sr.sliceReaderLoading[sliceNum]; isLoading {
		sr.sliceReadersMu.Unlock()
		<-loadingChan
		sr.sliceReadersMu.RLock()
		reader, found := sr.sliceReaders[sliceNum]
		if found {
			reader.refCount.Add(1)
		}
		sr.sliceReadersMu.RUnlock()
		if found {
			return reader, nil
		}
	} else {
		doneChan := make(chan struct{})
		sr.sliceReaderLoading[sliceNum] = doneChan
		sr.sliceReadersMu.Unlock()

		reader, err := sr.loadSliceReaderFromDisk(sliceNum)

		sr.sliceReadersMu.Lock()
		delete(sr.sliceReaderLoading, sliceNum)
		close(doneChan)

		if err != nil {
			sr.sliceReadersMu.Unlock()
			return nil, err
		}

		sr.evictIfNeededLocked()

		reader.refCount.Add(1)
		sr.sliceReaders[sliceNum] = reader
		sr.cacheOrder = append(sr.cacheOrder, sliceNum)
		sr.sliceReadersMu.Unlock()

		return reader, nil
	}

	reader, err := sr.loadSliceReaderFromDisk(sliceNum)
	if err != nil {
		return nil, err
	}

	sr.sliceReadersMu.Lock()
	sr.evictIfNeededLocked()
	reader.refCount.Add(1)
	sr.sliceReaders[sliceNum] = reader
	sr.cacheOrder = append(sr.cacheOrder, sliceNum)
	sr.sliceReadersMu.Unlock()

	return reader, nil
}

func (sr *SliceReader) evictIfNeededLocked() {
	for len(sr.sliceReaders) >= sr.maxCached && sr.maxCached > 0 && len(sr.cacheOrder) > 0 {
		evicted := false
		for i := 0; i < len(sr.cacheOrder); i++ {
			sliceNum := sr.cacheOrder[i]
			if oldReader, found := sr.sliceReaders[sliceNum]; found {
				if oldReader.refCount.Load() == 0 {
					oldReader.close()
					delete(sr.sliceReaders, sliceNum)
					sr.cacheOrder = append(sr.cacheOrder[:i], sr.cacheOrder[i+1:]...)
					evicted = true
					break
				}
			} else {
				sr.cacheOrder = append(sr.cacheOrder[:i], sr.cacheOrder[i+1:]...)
				evicted = true
				break
			}
		}
		if !evicted {
			break
		}
	}
}

func (sr *SliceReader) updateCacheOrder(sliceNum uint32) {
	for i, num := range sr.cacheOrder {
		if num == sliceNum {
			sr.cacheOrder = append(sr.cacheOrder[:i], sr.cacheOrder[i+1:]...)
			sr.cacheOrder = append(sr.cacheOrder, sliceNum)
			break
		}
	}
}

func (sr *SliceReader) releaseSliceReader(sliceNum uint32) {
	sr.sliceReadersMu.RLock()
	reader, found := sr.sliceReaders[sliceNum]
	sr.sliceReadersMu.RUnlock()
	if found {
		reader.refCount.Add(-1)
	}
}

func newSliceReader(basePath string, sliceNum uint32) (*sliceReader, error) {
	reader := &sliceReader{
		sliceNum: sliceNum,
		basePath: basePath,
	}

	blockIdxPath := filepath.Join(basePath, "blocks.index")
	blockIdx, err := loadBlockIndexSimple(blockIdxPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}
	reader.blockIndex = blockIdx

	dataLogPath := filepath.Join(basePath, "data.log")
	blockFile, err := os.Open(dataLogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data log: %w", err)
	}
	reader.blockFile = blockFile

	fileInfo, err := blockFile.Stat()
	if err != nil {
		blockFile.Close()
		return nil, fmt.Errorf("failed to stat data log: %w", err)
	}

	if fileInfo.Size() > 0 {
		mmapData, err := syscall.Mmap(
			int(blockFile.Fd()),
			0,
			int(fileInfo.Size()),
			syscall.PROT_READ,
			syscall.MAP_SHARED,
		)
		if err != nil {
			blockFile.Close()
			return nil, fmt.Errorf("failed to mmap data log: %w", err)
		}
		reader.mmapData = mmapData

		if len(mmapData) >= DataLogHeaderSize {
			header, err := ParseDataLogHeader(mmapData[:DataLogHeaderSize])
			if err == nil && header.Validate() == nil {
				reader.header = header
			}
		}
	}

	reader.loaded = true
	return reader, nil
}

func (r *sliceReader) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mmapData != nil {
		syscall.Munmap(r.mmapData)
		r.mmapData = nil
	}

	if r.blockFile != nil {
		r.blockFile.Close()
		r.blockFile = nil
	}

	r.loaded = false
	return nil
}

func (r *sliceReader) getGlobRangeFromBlockIndex() (uint64, uint64) {
	if len(r.blockIndex) == 0 {
		return 0, 0
	}
	var minGlob, maxGlob uint64
	first := true
	for _, entry := range r.blockIndex {
		if first {
			minGlob = entry.GlobMin
			maxGlob = entry.GlobMax
			first = false
		} else {
			if entry.GlobMin < minGlob {
				minGlob = entry.GlobMin
			}
			if entry.GlobMax > maxGlob {
				maxGlob = entry.GlobMax
			}
		}
	}
	return minGlob, maxGlob
}

func (r *sliceReader) remapDataLog() error {
	fileInfo, err := r.blockFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat data log: %w", err)
	}
	newSize := int(fileInfo.Size())

	if newSize <= len(r.mmapData) {
		return fmt.Errorf("file has not grown (current=%d, mmap=%d)", newSize, len(r.mmapData))
	}

	if err := syscall.Munmap(r.mmapData); err != nil {
		return fmt.Errorf("failed to munmap old region: %w", err)
	}

	mmapData, err := syscall.Mmap(
		int(r.blockFile.Fd()),
		0,
		newSize,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap new region: %w", err)
	}

	r.mmapData = mmapData
	return nil
}

func (r *sliceReader) readBlockData(blockNum uint32) ([]byte, error) {
	r.mu.RLock()
	entry, found := r.blockIndex[blockNum]
	if !found {
		indexSize := len(r.blockIndex)

		var minBlock, maxBlock uint32
		first := true
		for bn := range r.blockIndex {
			if first {
				minBlock, maxBlock = bn, bn
				first = false
			} else {
				if bn < minBlock {
					minBlock = bn
				}
				if bn > maxBlock {
					maxBlock = bn
				}
			}
		}
		r.mu.RUnlock()

		logger.Printf("debug", "Block %d not in cached index (slice %d has %d blocks, range %d-%d), attempting reload",
			blockNum, r.sliceNum, indexSize, minBlock, maxBlock)

		r.mu.Lock()
		entry, found = r.blockIndex[blockNum]
		if !found {
			blockIdxPath := filepath.Join(r.basePath, "blocks.index")
			logger.Printf("debug", "Reloading index from %s", blockIdxPath)
			freshIdx, err := loadBlockIndexSimple(blockIdxPath)
			if err != nil {
				r.mu.Unlock()
				return nil, fmt.Errorf("failed to reload block index: %w", err)
			}

			entry, found = freshIdx[blockNum]
			if !found {
				var minBlock, maxBlock uint32
				first := true
				for bn := range freshIdx {
					if first {
						minBlock, maxBlock = bn, bn
						first = false
					} else {
						if bn < minBlock {
							minBlock = bn
						}
						if bn > maxBlock {
							maxBlock = bn
						}
					}
				}
				r.mu.Unlock()
				logger.Printf("debug", "Block %d not found even after reload (slice %d fresh index has %d blocks, range %d-%d)",
					blockNum, r.sliceNum, len(freshIdx), minBlock, maxBlock)
				return nil, fmt.Errorf("block %d not found in slice %d", blockNum, r.sliceNum)
			}

			oldSize := len(r.blockIndex)
			r.blockIndex = freshIdx
			logger.Printf("debug", "Reloaded slice %d index: %d → %d blocks", r.sliceNum, oldSize, len(freshIdx))
		}
		entry = r.blockIndex[blockNum]
		r.mu.Unlock()

		r.mu.RLock()
	}
	r.mu.RUnlock()

	dataOffset := int64(entry.Offset) + 4
	dataEndOffset := dataOffset + int64(entry.Size)

	isFinalized := r.header != nil && r.header.IsFinalized()

	var data []byte

	if isFinalized {
		r.mu.RLock()
		needsRemap := r.mmapData == nil || dataEndOffset > int64(len(r.mmapData))
		r.mu.RUnlock()

		if needsRemap {
			r.mu.Lock()
			if r.mmapData == nil || dataEndOffset > int64(len(r.mmapData)) {
				if err := r.remapDataLog(); err != nil {
					r.mu.Unlock()
					return nil, fmt.Errorf("block %d extends beyond mmap bounds and remap failed: %w", blockNum, err)
				}
				if dataEndOffset > int64(len(r.mmapData)) {
					r.mu.Unlock()
					return nil, fmt.Errorf("block %d extends beyond mmap bounds after remap", blockNum)
				}
			}
			r.mu.Unlock()
		}

		r.mu.RLock()
		rawData := r.mmapData[dataOffset:dataEndOffset]
		data = make([]byte, len(rawData))
		copy(data, rawData)
		r.mu.RUnlock()
	} else {
		const maxCRCRetries = 3
		const crcRetryDelay = 50 * time.Millisecond

		crcOffset := dataEndOffset
		endOffset := crcOffset + 4

		for retry := 0; retry < maxCRCRetries; retry++ {
			r.mu.RLock()
			needsRemap := r.mmapData == nil || endOffset > int64(len(r.mmapData))
			r.mu.RUnlock()

			if needsRemap {
				r.mu.Lock()
				if r.mmapData == nil || endOffset > int64(len(r.mmapData)) {
					if err := r.remapDataLog(); err != nil {
						r.mu.Unlock()
						return nil, fmt.Errorf("block %d extends beyond mmap bounds and remap failed: %w", blockNum, err)
					}
					if endOffset > int64(len(r.mmapData)) {
						r.mu.Unlock()
						if retry < maxCRCRetries-1 {
							time.Sleep(crcRetryDelay)
							continue
						}
						return nil, fmt.Errorf("block %d CRC extends beyond mmap bounds after remap", blockNum)
					}
				}
				r.mu.Unlock()
			}

			r.mu.RLock()
			rawData := r.mmapData[dataOffset:crcOffset]
			storedCRC := binary.LittleEndian.Uint32(r.mmapData[crcOffset:endOffset])
			r.mu.RUnlock()

			calculatedCRC := crc32.ChecksumIEEE(rawData)
			if calculatedCRC == storedCRC {
				data = make([]byte, len(rawData))
				copy(data, rawData)
				break
			}

			if retry < maxCRCRetries-1 {
				logger.Printf("warning", "Block %d CRC mismatch in slice %d (stored=%08x calc=%08x), retrying (%d/%d)",
					blockNum, r.sliceNum, storedCRC, calculatedCRC, retry+1, maxCRCRetries)
				time.Sleep(crcRetryDelay)
				r.mu.Lock()
				r.remapDataLog()
				r.mu.Unlock()
			} else {
				return nil, fmt.Errorf("block %d CRC mismatch in slice %d after %d retries (stored=%08x calc=%08x)",
					blockNum, r.sliceNum, maxCRCRetries, storedCRC, calculatedCRC)
			}
		}
	}

	if len(data) >= 4 && binary.LittleEndian.Uint32(data[0:4]) == 0xFD2FB528 {
		decompressed, err := compression.ZstdDecompress(nil, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block %d in slice %d: %w", blockNum, r.sliceNum, err)
		}
		return decompressed, nil
	}

	return data, nil
}

func (r *sliceReader) binarySearchGlob(glob uint64) (uint32, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.blockIndex) == 0 {
		return 0, fmt.Errorf("glob %d not found in slice %d (empty block index)", glob, r.sliceNum)
	}

	startBlock := r.header.StartBlock
	numBlocks := uint32(len(r.blockIndex))

	idx := sort.Search(int(numBlocks), func(i int) bool {
		blockNum := startBlock + uint32(i)
		entry, exists := r.blockIndex[blockNum]
		if !exists {
			return true // Shouldn't happen, but be safe
		}
		return entry.GlobMax >= glob
	})

	if idx < int(numBlocks) {
		blockNum := startBlock + uint32(idx)
		entry, exists := r.blockIndex[blockNum]
		if exists && glob >= entry.GlobMin && glob <= entry.GlobMax {
			return blockNum, nil
		}
	}

	return 0, fmt.Errorf("glob %d not found in slice %d (binary search on %d blocks)",
		glob, r.sliceNum, len(r.blockIndex))
}

func (sr *SliceReader) GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		errMsg := err.Error()
		if !strings.Contains(errMsg, "outside slice") &&
			!strings.Contains(errMsg, "stale metadata") &&
			!strings.Contains(errMsg, "not found in any slice") &&
			!strings.Contains(errMsg, "index not ready") {
			logger.Printf("error", "ERROR: Block %d - findSliceForBlock failed: %v", blockNum, err)
		}
		return nil, nil, "", "", err
	}

	logger.Printf("debug", "Block %d: in slice %d (blocks %d-%d, finalized=%v)",
		blockNum, sliceInfo.SliceNum, sliceInfo.StartBlock, sliceInfo.EndBlock, sliceInfo.Finalized)

	var blockData []byte
	if sr.bufferPool != nil && sr.bufferPool.enabled && sliceInfo.Finalized {
		buffer := sr.bufferPool.GetSliceBuffer(sliceInfo.SliceNum)
		if buffer != nil {
			blockData, err = buffer.GetBlockData(blockNum)
			if err == nil {
				logger.Printf("debug", "Block %d: read from buffer pool", blockNum)
				goto parseBlock
			}
			logger.Printf("debug", "Block %d: buffer pool extraction failed, reading from disk", blockNum)
		}
	}

	{
		reader, err := sr.getSliceReader(sliceInfo.SliceNum)
		if err != nil {
			logger.Printf("error", "ERROR: Block %d - getSliceReader(%d) failed: %v", blockNum, sliceInfo.SliceNum, err)
			return nil, nil, "", "", err
		}

		blockData, err = reader.readBlockData(blockNum)
		sr.releaseSliceReader(sliceInfo.SliceNum)
		if err != nil {
			logger.Printf("error", "ERROR: Block %d - readBlockData failed: %v", blockNum, err)
			return nil, nil, "", "", err
		}

		logger.Printf("debug", "Block %d: read %d bytes from disk", blockNum, len(blockData))

		if sr.bufferPool != nil && sr.bufferPool.enabled && sliceInfo.Finalized {
			sr.loadSliceBuffer(*sliceInfo)
		}
	}

parseBlock:
	blob := bytesToBlockBlob(blockData)

	blockID := fmt.Sprintf("%x", blob.Block.ProducerBlockID[:])
	previous := ""

	notifs := make(map[uint64][]uint64)
	actionMeta := make([]ActionMetadata, 0, len(blob.Cats))

	for i, cat := range blob.Cats {
		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)
		notifiedAccounts := make([]uint64, 0, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifiedAccounts = append(notifiedAccounts, receiver)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifiedAccounts = append(notifiedAccounts, account)
			}
		}

		globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
		for _, account := range notifiedAccounts {
			notifs[account] = append(notifs[account], globSeq)
		}

		contractName := blob.Block.NamesInBlock[cat.ContractNameIndex]
		actionName := blob.Block.NamesInBlock[cat.ActionNameIndex]

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: globSeq,
			Contract:  contractName,
			Action:    actionName,
		})
	}

	return notifs, actionMeta, blockID, previous, nil
}

func (sr *SliceReader) GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		errMsg := err.Error()
		if !strings.Contains(errMsg, "outside slice") &&
			!strings.Contains(errMsg, "stale metadata") &&
			!strings.Contains(errMsg, "not found in any slice") &&
			!strings.Contains(errMsg, "index not ready") {
			logger.Printf("error", "ERROR: Block %d - findSliceForBlock failed: %v", blockNum, err)
		}
		return nil, nil, "", "", 0, 0, err
	}

	logger.Printf("debug", "Block %d: in slice %d (blocks %d-%d, finalized=%v)",
		blockNum, sliceInfo.SliceNum, sliceInfo.StartBlock, sliceInfo.EndBlock, sliceInfo.Finalized)

	var blockData []byte
	if sr.bufferPool != nil && sr.bufferPool.enabled && sliceInfo.Finalized {
		buffer := sr.bufferPool.GetSliceBuffer(sliceInfo.SliceNum)
		if buffer != nil {
			blockData, err = buffer.GetBlockData(blockNum)
			if err == nil {
				logger.Printf("debug", "Block %d: read from buffer pool", blockNum)
				goto parseBlock
			}
			logger.Printf("debug", "Block %d: buffer pool extraction failed, reading from disk", blockNum)
		}
	}

	{
		reader, err := sr.getSliceReader(sliceInfo.SliceNum)
		if err != nil {
			logger.Printf("error", "ERROR: Block %d - getSliceReader(%d) failed: %v", blockNum, sliceInfo.SliceNum, err)
			return nil, nil, "", "", 0, 0, err
		}

		blockData, err = reader.readBlockData(blockNum)
		sr.releaseSliceReader(sliceInfo.SliceNum)
		if err != nil {
			errMsg := err.Error()
			if !strings.Contains(errMsg, "not found in slice") {
				logger.Printf("error", "ERROR: Block %d - readBlockData failed: %v", blockNum, err)
			}
			return nil, nil, "", "", 0, 0, err
		}

		logger.Printf("debug", "Block %d: read %d bytes from disk", blockNum, len(blockData))

		if sr.bufferPool != nil && sr.bufferPool.enabled && sliceInfo.Finalized {
			sr.loadSliceBuffer(*sliceInfo)
		}
	}

parseBlock:
	blob := bytesToBlockBlob(blockData)

	blockID := fmt.Sprintf("%x", blob.Block.ProducerBlockID[:])
	previous := ""
	blockTime := blob.Block.BlockTimeUint32 // Extract blockTime for indexing

	notifs := make(map[uint64][]uint64)
	actionMeta := make([]ActionMetadata, 0, len(blob.Cats))
	filteredCount := 0

	for i, cat := range blob.Cats {
		contractName := blob.Block.NamesInBlock[cat.ContractNameIndex]
		actionName := blob.Block.NamesInBlock[cat.ActionNameIndex]

		if filterFunc != nil && filterFunc(contractName, actionName) {
			filteredCount++
			continue
		}

		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)
		notifiedAccounts := make([]uint64, 0, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifiedAccounts = append(notifiedAccounts, receiver)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifiedAccounts = append(notifiedAccounts, account)
			}
		}

		globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
		for _, account := range notifiedAccounts {
			notifs[account] = append(notifs[account], globSeq)
		}

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: globSeq,
			Contract:  contractName,
			Action:    actionName,
		})
	}

	return notifs, actionMeta, blockID, previous, filteredCount, blockTime, nil
}

func (sr *SliceReader) GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, "", "", err
	}

	reader, err := sr.getSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", "", err
	}

	blockData, err := reader.readBlockData(blockNum)
	sr.releaseSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlob(blockData)

	blockID := fmt.Sprintf("%x", blob.Block.ProducerBlockID[:])
	previous := ""

	notifs := make(map[uint64][]uint64)

	for i, cat := range blob.Cats {
		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)
		notifiedAccounts := make([]uint64, 0, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifiedAccounts = append(notifiedAccounts, receiver)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifiedAccounts = append(notifiedAccounts, account)
			}
		}

		globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
		for _, account := range notifiedAccounts {
			notifs[account] = append(notifs[account], globSeq)
		}
	}

	return notifs, blockID, previous, nil
}

func (sr *SliceReader) GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, "", "", err
	}

	reader, err := sr.getSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", "", err
	}

	blockData, err := reader.readBlockData(blockNum)
	sr.releaseSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlobBlockOnly(blockData)

	blockID := fmt.Sprintf("%x", blob.ProducerBlockID[:])
	previous := ""

	trxIDs := make([]string, 0, len(blob.TrxIDInBlock))
	for _, trxID := range blob.TrxIDInBlock {
		trxIDs = append(trxIDs, fmt.Sprintf("%x", trxID[:]))
	}

	return trxIDs, blockID, previous, nil
}

// GetTransactionIDsRaw returns raw transaction IDs as [32]byte without hex encoding.
// This is more efficient for internal use where hex strings are not needed.
func (sr *SliceReader) GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, err
	}

	reader, err := sr.getSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, err
	}

	blockData, err := reader.readBlockData(blockNum)
	sr.releaseSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlobBlockOnly(blockData)
	return blob.TrxIDInBlock, nil
}

func (sr *SliceReader) GetActionsForTransaction(blockNum uint32, trxID string) ([]chain.ActionTrace, string, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, "", err
	}

	reader, err := sr.getSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", err
	}

	blockData, err := reader.readBlockData(blockNum)
	sr.releaseSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, "", err
	}

	blob := bytesToBlockBlob(blockData)
	blockTime := blob.Block.BlockTime

	var actions []chain.ActionTrace
	for i, cat := range blob.Cats {
		actionTrxID := fmt.Sprintf("%x", blob.Block.TrxIDInBlock[cat.TrxIDIndex][:])
		if actionTrxID == trxID {
			globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			action := cat.At(blob.Block, blockNum, globSeq)
			actions = append(actions, *action)
		}
	}

	return actions, blockTime, nil
}

type TransactionData struct {
	Actions        []chain.ActionTrace
	BlockTime      string
	Status         uint8
	CpuUsageUs     uint32
	NetUsageWords  uint32
	Expiration     uint32
	RefBlockNum    uint16
	RefBlockPrefix uint32
	Signatures     [][]byte
	TrxIndex       int // Position in block (transaction_num)
}

func (sr *SliceReader) GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error) {
	sliceInfo, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, err
	}

	reader, err := sr.getSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, err
	}

	blockData, err := reader.readBlockData(blockNum)
	sr.releaseSliceReader(sliceInfo.SliceNum)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlob(blockData)

	trxIndex := -1
	for i, id := range blob.Block.TrxIDInBlock {
		if fmt.Sprintf("%x", id[:]) == trxID {
			trxIndex = i
			break
		}
	}

	if trxIndex < 0 {
		return nil, fmt.Errorf("transaction %s not found in block %d", trxID, blockNum)
	}

	var actions []chain.ActionTrace
	for i, cat := range blob.Cats {
		if int(cat.TrxIDIndex) == trxIndex {
			globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			action := cat.At(blob.Block, blockNum, globSeq)
			actions = append(actions, *action)
		}
	}

	meta := &blob.Block.TrxMetaInBlock[trxIndex]

	return &TransactionData{
		Actions:        actions,
		BlockTime:      blob.Block.BlockTime,
		Status:         meta.Status,
		CpuUsageUs:     meta.CpuUsageUs,
		NetUsageWords:  meta.NetUsageWords,
		Expiration:     meta.Expiration,
		RefBlockNum:    meta.RefBlockNum,
		RefBlockPrefix: meta.RefBlockPrefix,
		Signatures:     meta.Signatures,
		TrxIndex:       trxIndex,
	}, nil
}

func (sr *SliceReader) GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error) {
	return sr.GetRawBlockBatchFiltered(startBlock, endBlock, nil)
}

func (sr *SliceReader) GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	if endBlock < startBlock {
		return nil, fmt.Errorf("endBlock (%d) must be >= startBlock (%d)", endBlock, startBlock)
	}

	sliceSize := uint32(10000)
	startSlice := (startBlock - 1) / sliceSize
	endSlice := (endBlock - 1) / sliceSize

	type localSliceBuffer struct {
		data  []byte
		index map[uint32]blockIndexEntry
	}
	localBuffers := make(map[uint32]*localSliceBuffer)
	defer func() {
		for _, buf := range localBuffers {
			if buf.data != nil {
				ReturnSliceData(buf.data)
			}
		}
	}()

	batchSliceReaders := make(map[uint32]*sliceReader)
	defer func() {
		for _, reader := range batchSliceReaders {
			reader.close()
		}
	}()

	for sliceNum := startSlice; sliceNum <= endSlice; sliceNum++ {
		sliceInfo, err := sr.findSliceForBlock(sliceNum*sliceSize + 1)
		if err != nil {
			break
		}
		if sliceInfo.Finalized {
			data, index, err := loadSliceData(sr.basePath, *sliceInfo)
			if err != nil {
				continue
			}
			localBuffers[sliceNum] = &localSliceBuffer{data: data, index: index}
		} else {
			reader, err := sr.loadSliceReaderFromDisk(sliceNum)
			if err != nil {
				logger.Printf("debug", "Failed to pre-load non-finalized slice %d: %v", sliceNum, err)
				continue
			}
			batchSliceReaders[sliceNum] = reader
			logger.Printf("debug", "Pre-loaded non-finalized slice %d with %d blocks", sliceNum, len(reader.blockIndex))
		}
	}

	blockCount := int(endBlock - startBlock + 1)
	results := make([]RawBlock, blockCount)

	for i := 0; i < blockCount; i++ {
		bn := startBlock + uint32(i)
		sliceNum := (bn - 1) / sliceSize

		var blockData []byte
		var err error

		if localBuf, found := localBuffers[sliceNum]; found {
			blockData, err = extractBlockFromLocalBuffer(localBuf.data, localBuf.index, bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		} else if reader, found := batchSliceReaders[sliceNum]; found {
			blockData, err = reader.readBlockData(bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		} else {
			sliceInfo, err := sr.findSliceForBlock(bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
			reader, err := sr.getSliceReader(sliceInfo.SliceNum)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
			blockData, err = reader.readBlockData(bn)
			sr.releaseSliceReader(sliceInfo.SliceNum)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		}

		notifs, actionMeta, actions, namesInBlock, _, _, blockTime, _ := parseBlockWithCanonical(blockData, filterFunc)

		results[i] = RawBlock{
			BlockNum:      bn,
			BlockTime:     blockTime,
			Notifications: notifs,
			ActionMeta:    actionMeta,
			Actions:       actions,
			NamesInBlock:  namesInBlock,
		}
	}

	return results, nil
}

type RawBlockData struct {
	BlockNum uint32
	Data     []byte
}

// SliceBatch holds a reference to a slice's mmap'd data and index,
// allowing on-demand block extraction without upfront decompression.
type SliceBatch struct {
	sliceStart         uint32
	sliceEnd           uint32
	mmapData           []byte                     // pooled slice data (compressed blocks)
	index              map[uint32]blockIndexEntry // block index for offset lookup
	preExtractedBlocks map[uint32][]byte          // for non-finalized slices (already decompressed)
	poolPtr            *[]byte                    // pool pointer for returning buffer (nil if not pooled)
}

// Close releases the memory (returns pooled buffer if applicable).
func (sb *SliceBatch) Close() {
	if sb.poolPtr != nil {
		buf := *sb.poolPtr
		used := len(sb.mmapData)
		if used > 0 && cap(buf) > 100*1024*1024 && cap(buf) > used*2 {
			newBuf := make([]byte, used)
			*sb.poolPtr = newBuf
		}
		sliceDataPool.Put(sb.poolPtr)
		sb.poolPtr = nil
	}
	sb.mmapData = nil
}

// ExtractBlock decompresses and returns a single block's data.
// The returned slice is newly allocated and safe to use after Close().
func (sb *SliceBatch) ExtractBlock(blockNum uint32) ([]byte, error) {
	if sb.preExtractedBlocks != nil {
		data, found := sb.preExtractedBlocks[blockNum]
		if !found {
			return nil, fmt.Errorf("block %d not in pre-extracted blocks", blockNum)
		}
		return data, nil
	}

	entry, found := sb.index[blockNum]
	if !found {
		return nil, fmt.Errorf("block %d not in slice index", blockNum)
	}

	offset := entry.Offset + 4 // Skip 4-byte size prefix
	size := entry.Size

	if offset+uint64(size) > uint64(len(sb.mmapData)) {
		return nil, fmt.Errorf("block %d offset out of bounds", blockNum)
	}

	rawData := sb.mmapData[offset : offset+uint64(size)]

	if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
		decompressed, err := compression.ZstdDecompress(nil, rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block %d: %w", blockNum, err)
		}
		return decompressed, nil
	}

	result := make([]byte, len(rawData))
	copy(result, rawData)
	return result, nil
}

// PreExtractAll decompresses all blocks in the batch sequentially.
// This converts random mmap access into sequential I/O followed by CPU work.
// After calling this, ExtractBlock returns from memory instead of mmap.
// Uses a reusable buffer to minimize memory allocations during decompression.
func (sb *SliceBatch) PreExtractAll() error {
	if sb.preExtractedBlocks != nil || sb.mmapData == nil {
		return nil
	}

	sb.preExtractedBlocks = make(map[uint32][]byte, sb.sliceEnd-sb.sliceStart+1)

	decompBuf := make([]byte, 2*1024*1024)

	for blockNum := sb.sliceStart; blockNum <= sb.sliceEnd; blockNum++ {
		entry, found := sb.index[blockNum]
		if !found {
			continue
		}

		offset := entry.Offset + 4
		size := entry.Size

		if offset+uint64(size) > uint64(len(sb.mmapData)) {
			continue
		}

		rawData := sb.mmapData[offset : offset+uint64(size)]

		var blockData []byte
		if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
			n, err := compression.ZstdDecompressInto(decompBuf, rawData)
			if err != nil {
				if compression.ZstdIsDstSizeTooSmallError(err) {
					newSize := len(decompBuf) * 2
					if newSize > 16*1024*1024 {
						newSize = 16 * 1024 * 1024
					}
					decompBuf = make([]byte, newSize)
					n, err = compression.ZstdDecompressInto(decompBuf, rawData)
					if err != nil {
						decompressed, err2 := compression.ZstdDecompress(nil, rawData)
						if err2 != nil {
							continue
						}
						blockData = decompressed
						goto storeBlock
					}
				} else {
					continue
				}
			}
			blockData = make([]byte, n)
			copy(blockData, decompBuf[:n])
		} else {
			blockData = make([]byte, len(rawData))
			copy(blockData, rawData)
		}

	storeBlock:
		sb.preExtractedBlocks[blockNum] = blockData
	}

	if sb.poolPtr != nil {
		sliceDataPool.Put(sb.poolPtr)
		sb.poolPtr = nil
	}
	sb.mmapData = nil
	sb.index = nil

	return nil
}

// PreExtractAllParallel decompresses all blocks using parallel workers.
func (sb *SliceBatch) PreExtractAllParallel(numWorkers int) error {
	if sb.preExtractedBlocks != nil || sb.mmapData == nil {
		return nil
	}

	if numWorkers < 1 {
		numWorkers = 1
	}

	type compressedBlock struct {
		blockNum uint32
		data     []byte
		isZstd   bool
	}

	blocks := make([]compressedBlock, 0, sb.sliceEnd-sb.sliceStart+1)
	for blockNum := sb.sliceStart; blockNum <= sb.sliceEnd; blockNum++ {
		entry, found := sb.index[blockNum]
		if !found {
			continue
		}

		offset := entry.Offset + 4
		size := entry.Size

		if offset+uint64(size) > uint64(len(sb.mmapData)) {
			continue
		}

		rawData := sb.mmapData[offset : offset+uint64(size)]
		dataCopy := make([]byte, len(rawData))
		copy(dataCopy, rawData)

		isZstd := len(dataCopy) >= 4 && binary.LittleEndian.Uint32(dataCopy[0:4]) == 0xFD2FB528
		blocks = append(blocks, compressedBlock{
			blockNum: blockNum,
			data:     dataCopy,
			isZstd:   isZstd,
		})
	}

	if sb.poolPtr != nil {
		sliceDataPool.Put(sb.poolPtr)
		sb.poolPtr = nil
	}
	sb.mmapData = nil
	sb.index = nil

	results := make(map[uint32][]byte, len(blocks))
	var resultsMu sync.Mutex

	workChan := make(chan compressedBlock, len(blocks))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decompBuf := make([]byte, 2*1024*1024)

			for block := range workChan {
				var blockData []byte
				if block.isZstd {
					n, err := compression.ZstdDecompressInto(decompBuf, block.data)
					if err != nil {
						if compression.ZstdIsDstSizeTooSmallError(err) {
							decompressed, err2 := compression.ZstdDecompress(nil, block.data)
							if err2 != nil {
								continue
							}
							blockData = decompressed
						} else {
							continue
						}
					} else {
						blockData = make([]byte, n)
						copy(blockData, decompBuf[:n])
					}
				} else {
					blockData = block.data
				}

				resultsMu.Lock()
				results[block.blockNum] = blockData
				resultsMu.Unlock()
			}
		}()
	}

	for _, block := range blocks {
		workChan <- block
	}
	close(workChan)
	wg.Wait()

	sb.preExtractedBlocks = results
	return nil
}

// TransactionIDsResult holds the extracted transaction IDs for a block.
type TransactionIDsResult struct {
	BlockNum uint32
	TrxIDs   []string
}

// ExtractTransactionIDsOnly extracts transaction IDs from all blocks in the batch.
// Processes blocks directly from mmap without bulk copying to minimize memory usage.
// Returns map of blockNum -> raw 32-byte transaction IDs.
func (sb *SliceBatch) ExtractTransactionIDsOnly(numWorkers int) (map[uint32][][32]byte, error) {
	if sb.preExtractedBlocks != nil {
		results := make(map[uint32][][32]byte, len(sb.preExtractedBlocks))
		for blockNum, data := range sb.preExtractedBlocks {
			results[blockNum] = ExtractTransactionIDsRaw(data)
		}
		return results, nil
	}

	if sb.mmapData == nil {
		return nil, fmt.Errorf("no data available")
	}

	if numWorkers < 1 {
		numWorkers = 1
	}

	type blockWork struct {
		blockNum uint32
		offset   uint64
		size     uint32
	}

	blocks := make([]blockWork, 0, sb.sliceEnd-sb.sliceStart+1)
	for blockNum := sb.sliceStart; blockNum <= sb.sliceEnd; blockNum++ {
		entry, found := sb.index[blockNum]
		if !found {
			continue
		}

		offset := entry.Offset + 4
		size := entry.Size

		if offset+uint64(size) > uint64(len(sb.mmapData)) {
			continue
		}

		blocks = append(blocks, blockWork{
			blockNum: blockNum,
			offset:   offset,
			size:     size,
		})
	}

	results := make(map[uint32][][32]byte, len(blocks))
	var resultsMu sync.Mutex

	workChan := make(chan blockWork, numWorkers*2)
	var wg sync.WaitGroup

	mmapData := sb.mmapData

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decompBuf := make([]byte, 300*1024)

			for block := range workChan {
				rawData := mmapData[block.offset : block.offset+uint64(block.size)]

				var trxIDs [][32]byte

				if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
					n, err := compression.ZstdDecompressInto(decompBuf, rawData)
					if err != nil {
						if compression.ZstdIsDstSizeTooSmallError(err) {
							decompressed, err2 := compression.ZstdDecompress(nil, rawData)
							if err2 == nil {
								trxIDs = extractTransactionIDsFromBytes(decompressed)
							}
						}
					} else {
						trxIDs = extractTransactionIDsFromBytes(decompBuf[:n])
					}
				} else {
					trxIDs = extractTransactionIDsFromBytes(rawData)
				}

				if trxIDs != nil {
					resultsMu.Lock()
					results[block.blockNum] = trxIDs
					resultsMu.Unlock()
				}
			}
		}()
	}

	for _, block := range blocks {
		workChan <- block
	}
	close(workChan)
	wg.Wait()

	return results, nil
}

// BlockCount returns the number of blocks in this batch.
func (sb *SliceBatch) BlockCount() int {
	return int(sb.sliceEnd - sb.sliceStart + 1)
}

// StartBlock returns the first block number in this batch.
func (sb *SliceBatch) StartBlock() uint32 {
	return sb.sliceStart
}

// EndBlock returns the last block number in this batch.
func (sb *SliceBatch) EndBlock() uint32 {
	return sb.sliceEnd
}

// Size returns the memory footprint of this batch in bytes.
func (sb *SliceBatch) Size() int64 {
	if sb.mmapData != nil {
		return int64(len(sb.mmapData))
	}
	var total int64
	for _, data := range sb.preExtractedBlocks {
		total += int64(len(data))
	}
	return total
}

// GetSliceBatch returns a batch handle for a 10K-block slice.
// The batch holds compressed data and extracts/decompresses on demand.
// Caller must call Close() when done.
func (sr *SliceReader) GetSliceBatch(sliceStart, sliceEnd uint32) (*SliceBatch, error) {
	if sliceEnd < sliceStart {
		return nil, fmt.Errorf("sliceEnd (%d) must be >= sliceStart (%d)", sliceEnd, sliceStart)
	}

	sliceSize := uint32(10000)
	sliceNum := (sliceStart - 1) / sliceSize

	sliceInfo, err := sr.findSliceForBlock(sliceStart)
	if err != nil {
		return nil, fmt.Errorf("finding slice for block %d: %w", sliceStart, err)
	}

	if sliceInfo.Finalized {
		data, index, poolPtr, err := loadSliceDataPooled(sr.basePath, *sliceInfo)
		if err != nil {
			return nil, fmt.Errorf("loading slice %d: %w", sliceNum, err)
		}
		return &SliceBatch{
			sliceStart: sliceStart,
			sliceEnd:   sliceEnd,
			mmapData:   data,
			index:      index,
			poolPtr:    poolPtr,
		}, nil
	}

	// Non-finalized slice - read directly
	reader, err := sr.loadSliceReaderFromDisk(sliceNum)
	if err != nil {
		return nil, fmt.Errorf("loading non-finalized slice %d: %w", sliceNum, err)
	}
	defer reader.close()

	preExtracted := make(map[uint32][]byte, sliceEnd-sliceStart+1)
	for bn := sliceStart; bn <= sliceEnd; bn++ {
		blockData, err := reader.readBlockData(bn)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", bn, err)
		}
		preExtracted[bn] = blockData
	}

	return &SliceBatch{
		sliceStart:         sliceStart,
		sliceEnd:           sliceEnd,
		mmapData:           nil,
		index:              nil,
		preExtractedBlocks: preExtracted,
	}, nil
}

func (sr *SliceReader) GetRawBlocksBatch(startBlock, endBlock uint32) ([]RawBlockData, error) {
	if endBlock < startBlock {
		return nil, fmt.Errorf("endBlock (%d) must be >= startBlock (%d)", endBlock, startBlock)
	}

	sliceSize := uint32(10000)
	startSlice := (startBlock - 1) / sliceSize
	endSlice := (endBlock - 1) / sliceSize

	type localSliceBuffer struct {
		data  []byte
		index map[uint32]blockIndexEntry
	}
	localBuffers := make(map[uint32]*localSliceBuffer)
	defer func() {
		for _, buf := range localBuffers {
			if buf.data != nil {
				ReturnSliceData(buf.data)
			}
		}
	}()

	batchSliceReaders := make(map[uint32]*sliceReader)
	defer func() {
		for _, reader := range batchSliceReaders {
			reader.close()
		}
	}()

	for sliceNum := startSlice; sliceNum <= endSlice; sliceNum++ {
		sliceInfo, err := sr.findSliceForBlock(sliceNum*sliceSize + 1)
		if err != nil {
			break
		}
		if sliceInfo.Finalized {
			data, index, err := loadSliceData(sr.basePath, *sliceInfo)
			if err != nil {
				continue
			}
			localBuffers[sliceNum] = &localSliceBuffer{data: data, index: index}
		} else {
			reader, err := sr.loadSliceReaderFromDisk(sliceNum)
			if err != nil {
				continue
			}
			batchSliceReaders[sliceNum] = reader
		}
	}

	blockCount := int(endBlock - startBlock + 1)
	results := make([]RawBlockData, blockCount)

	for i := 0; i < blockCount; i++ {
		bn := startBlock + uint32(i)
		sliceNum := (bn - 1) / sliceSize

		var blockData []byte
		var err error

		if localBuf, found := localBuffers[sliceNum]; found {
			blockData, err = extractBlockFromLocalBuffer(localBuf.data, localBuf.index, bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		} else if reader, found := batchSliceReaders[sliceNum]; found {
			blockData, err = reader.readBlockData(bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		} else {
			sliceInfo, err := sr.findSliceForBlock(bn)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
			reader, err := sr.getSliceReader(sliceInfo.SliceNum)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
			blockData, err = reader.readBlockData(bn)
			sr.releaseSliceReader(sliceInfo.SliceNum)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", bn, err)
			}
		}

		results[i] = RawBlockData{
			BlockNum: bn,
			Data:     blockData,
		}
	}

	return results, nil
}

func extractBlockFromLocalBuffer(data []byte, index map[uint32]blockIndexEntry, blockNum uint32) ([]byte, error) {
	entry, found := index[blockNum]
	if !found {
		return nil, fmt.Errorf("block %d not in slice index", blockNum)
	}

	offset := entry.Offset + 4 // Skip 4-byte size prefix
	size := entry.Size

	if offset+uint64(size) > uint64(len(data)) {
		return nil, fmt.Errorf("block %d offset out of bounds", blockNum)
	}

	rawData := data[offset : offset+uint64(size)]

	if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
		decompressed, err := compression.ZstdDecompress(nil, rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block %d: %w", blockNum, err)
		}
		return decompressed, nil
	}

	result := make([]byte, len(rawData))
	copy(result, rawData)
	return result, nil
}

func parseBlockWithCanonical(blockData []byte, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, []CanonicalAction, []uint64, string, string, uint32, int) {
	blob := bytesToSyncBlockBlob(blockData)
	defer releaseSyncBlockBlob(blob)

	blockID := ""
	previous := ""
	blockTime := blob.Block.BlockTimeUint32
	numActions := len(blob.Actions)

	// Count total auth indexes for pooled allocation
	totalAuths := 0
	for i := range blob.Actions {
		totalAuths += len(blob.Actions[i].AuthAccountIndexes)
	}

	// Pre-allocate with estimated sizes
	// Estimate ~80 unique accounts per block based on typical distribution
	notifs := make(map[uint64][]uint64, 80)
	actionMeta := make([]ActionMetadata, 0, numActions)
	actions := make([]CanonicalAction, 0, numActions)
	filteredCount := 0

	// Pooled auth buffer - single allocation for all auth indexes
	authPool := make([]uint32, 0, totalAuths)

	// Small fixed array for notified account deduplication (no map allocation)
	// Most actions have <16 unique notified accounts (receiver + authorizers)
	var notifiedAccounts [16]uint64
	var notifiedCount int

	for i, act := range blob.Actions {
		contractName := blob.Block.NamesInBlock[act.ContractNameIndex]
		actionName := blob.Block.NamesInBlock[act.ActionNameIndex]

		globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(act.CatOffset)
		if globSeq < blob.Block.MinGlobInBlock || globSeq > blob.Block.MaxGlobInBlock {
			logger.Printf("error", "Block %d action %d globSeq=%d out of range [%d, %d]",
				blob.Block.BlockNum, i, globSeq, blob.Block.MinGlobInBlock, blob.Block.MaxGlobInBlock)
		}

		// Slice into pooled buffer instead of per-action allocation
		authStart := len(authPool)
		authPool = append(authPool, act.AuthAccountIndexes...)
		authSlice := authPool[authStart:len(authPool):len(authPool)]

		action := CanonicalAction{
			ActionOrdinal:      act.ActionOrdinal,
			CreatorAO:          act.CreatorAO,
			ReceiverUint64:     blob.Block.NamesInBlock[act.ReceiverIndex],
			DataIndex:          act.DataIndex,
			AuthAccountIndexes: authSlice,
			GlobalSeqUint64:    globSeq,
			TrxIndex:           act.TrxIDIndex,
			ContractUint64:     contractName,
			ActionUint64:       actionName,
		}
		actions = append(actions, action)

		if filterFunc != nil && filterFunc(contractName, actionName) {
			filteredCount++
			continue
		}

		// Use fixed array for dedup instead of map (zero allocations)
		notifiedCount = 0
		receiver := blob.Block.NamesInBlock[act.ReceiverIndex]
		notifiedAccounts[0] = receiver
		notifiedCount = 1

		for _, idx := range act.AuthAccountIndexes {
			account := blob.Block.NamesInBlock[idx]
			// Linear search in small array is faster than map for <16 elements
			found := false
			for j := 0; j < notifiedCount; j++ {
				if notifiedAccounts[j] == account {
					found = true
					break
				}
			}
			if !found && notifiedCount < 16 {
				notifiedAccounts[notifiedCount] = account
				notifiedCount++
			}
		}

		for j := 0; j < notifiedCount; j++ {
			account := notifiedAccounts[j]
			notifs[account] = append(notifs[account], globSeq)
		}

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: globSeq,
			Contract:  contractName,
			Action:    actionName,
		})
	}

	namesInBlock := make([]uint64, len(blob.Block.NamesInBlock))
	copy(namesInBlock, blob.Block.NamesInBlock)

	return notifs, actionMeta, actions, namesInBlock, blockID, previous, blockTime, filteredCount
}

func parseRawBlock(blockData []byte, blockNum uint32, filterFunc ActionFilterFunc) RawBlock {
	notifs, actionMeta, actions, namesInBlock, _, _, blockTime, _ := parseBlockWithCanonical(blockData, filterFunc)
	return RawBlock{
		BlockNum:      blockNum,
		BlockTime:     blockTime,
		Notifications: notifs,
		ActionMeta:    actionMeta,
		Actions:       actions,
		NamesInBlock:  namesInBlock,
	}
}

func (sr *SliceReader) GetStateProps(bypassCache bool) (uint32, uint32, error) {
	sliceCount := sr.sharedMetadata.getSliceCount()

	if sliceCount == 0 {
		return 0, 0, fmt.Errorf("no slices available")
	}

	if bypassCache {
		for {
			lastSlice := sr.sharedMetadata.getSlice(sr.sharedMetadata.getSliceCount() - 1)

			nextSliceNum := lastSlice.SliceNum + 1
			nextSliceStart := nextSliceNum*10000 + 1
			nextSliceMax := nextSliceStart + 10000 - 1
			nextSliceName := fmt.Sprintf("history_%010d-%010d", nextSliceStart, nextSliceMax)
			nextSlicePath := filepath.Join(sr.basePath, nextSliceName)

			if _, err := os.Stat(nextSlicePath); err != nil {
				break
			}

			blockIndexPath := filepath.Join(nextSlicePath, "blocks.index")
			endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
			if err != nil {
				logger.Printf("warning", "New slice %d exists but can't read blocks.index: %v", nextSliceNum, err)
				break
			}

			if globMin > globMax {
				logger.Printf("error", "Invalid new slice: globMin (%d) > globMax (%d)", globMin, globMax)
			}
			if globMax > 1<<40 {
				logger.Printf("error", "Suspicious new slice: globMax=%d seems too large", globMax)
			}

			newSlice := SliceInfo{
				SliceNum:       nextSliceNum,
				StartBlock:     nextSliceStart,
				EndBlock:       endBlock,
				MaxBlock:       nextSliceMax,
				BlocksPerSlice: 10000,
				Finalized:      false,
				GlobMin:        globMin,
				GlobMax:        globMax,
			}

			// Mark previous slice as finalized and append new slice
			currentCount := sr.sharedMetadata.getSliceCount()
			if currentCount > 0 {
				sr.sharedMetadata.markSliceFinalized(currentCount - 1)
				prevSlice := sr.sharedMetadata.getSlice(currentCount - 1)
				if sr.blockIndexCache != nil {
					sr.blockIndexCache.FinalizeSlice(prevSlice.SliceNum)
				}
			}
			newIdx := sr.sharedMetadata.appendSlice(newSlice)
			sr.sliceMapByBlock[nextSliceNum] = newIdx

			alreadyKnown := false
			if sr.blockIndexCache != nil {
				alreadyKnown = sr.blockIndexCache.HasSlice(nextSliceNum)
				if err := sr.blockIndexCache.LoadSliceFromBlocksIndex(nextSlicePath, nextSliceNum, nextSliceStart, false); err != nil {
					logger.Printf("debug", "BlockIndexCache: failed to load new slice %d: %v", nextSliceNum, err)
				}
			}

			if !alreadyKnown {
				logger.Printf("sync", "New slice %d (blocks %d-%d, globs %d-%d)",
					nextSliceNum, nextSliceStart, endBlock, globMin, globMax)
			}
		}

		lastSlice := sr.sharedMetadata.getSlice(sr.sharedMetadata.getSliceCount() - 1)

		slicePath := filepath.Join(sr.basePath, fmt.Sprintf("history_%010d-%010d", lastSlice.StartBlock, lastSlice.MaxBlock))
		blockIndexPath := filepath.Join(slicePath, "blocks.index")
		endBlock, globMin, globMax, err := findLastBlockInIndex(blockIndexPath)
		if err != nil {
			return lastSlice.EndBlock, lastSlice.EndBlock, nil
		}

		if globMin > globMax {
			logger.Printf("error", "Invalid state: globMin (%d) > globMax (%d) from %s",
				globMin, globMax, blockIndexPath)
		}
		if globMax > 1<<40 {
			logger.Printf("error", "Suspicious state: globMax=%d seems too large (path: %s)",
				globMax, blockIndexPath)
		}

		currentCount := sr.sharedMetadata.getSliceCount()
		if currentCount > 0 && endBlock != lastSlice.EndBlock {
			sr.sharedMetadata.updateLastSliceEndBlock(endBlock, globMin, globMax)
			if sr.blockIndexCache != nil {
				if err := sr.blockIndexCache.LoadSliceFromBlocksIndex(slicePath, lastSlice.SliceNum, lastSlice.StartBlock, false); err != nil {
					logger.Printf("debug", "BlockIndexCache: failed to refresh active slice %d: %v", lastSlice.SliceNum, err)
				}
			}
		}

		return endBlock, endBlock, nil
	}

	var lastFinalized *SliceInfo
	sliceCount = sr.sharedMetadata.getSliceCount()
	for i := sliceCount - 1; i >= 0; i-- {
		slice := sr.sharedMetadata.getSlice(i)
		if slice.Finalized {
			lastFinalized = &slice
			break
		}
	}

	if lastFinalized == nil {
		firstSlice := sr.sharedMetadata.getSlice(0)
		return firstSlice.EndBlock, firstSlice.EndBlock, nil
	}

	return lastFinalized.EndBlock, lastFinalized.EndBlock, nil
}

func (sr *SliceReader) GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error) {
	return nil, "", "", fmt.Errorf("GetRawActionsFiltered not yet implemented for slice-based storage")
}

func (sr *SliceReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error) {
	if len(globalSeqs) == 0 {
		return []chain.ActionTrace{}, nil, nil
	}

	totalStart := time.Now()

	type target struct {
		globalSeq uint64
		origIndex int
		sliceNum  uint32
	}

	type lookupResult struct {
		globalSeq uint64
		sliceNum  uint32
		blockNum  uint32
	}

	phase1Start := time.Now()
	lookupResults := make([]lookupResult, len(globalSeqs))

	var blockIndexCacheHits int

	// Phase 1a: Slice lookup (O(log n) via sharedMetadata)
	phase1aStart := time.Now()
	slicesNeeded := make(map[uint32]bool)
	globToSlice := make(map[uint64]uint32, len(globalSeqs))

	var metadataHits, diskLookups int
	for i, glob := range globalSeqs {
		// Use sharedMetadata for slice lookup - always up-to-date, O(log n)
		sliceInfo, _ := sr.sharedMetadata.findSliceForGlob(glob)
		if sliceInfo != nil {
			metadataHits++
			slicesNeeded[sliceInfo.SliceNum] = true

			// Try blockIndexCache for block lookup
			if sr.blockIndexCache != nil {
				if blockNum, found := sr.blockIndexCache.FindBlock(sliceInfo.SliceNum, glob); found {
					lookupResults[i] = lookupResult{globalSeq: glob, sliceNum: sliceInfo.SliceNum, blockNum: blockNum}
					blockIndexCacheHits++
					continue
				}
			}

			// Block not in cache, will need slice reader for Phase 1c
			globToSlice[glob] = sliceInfo.SliceNum
			continue
		}

		// Fallback to disk lookup (shouldn't happen if metadata is up-to-date)
		diskLookups++
		sliceInfoDisk, _, err := sr.findSliceForGlobTracked(glob)
		if err != nil {
			logger.Printf("error", "Glob %d not found | index %d/%d | all requested: %v",
				glob, i, len(globalSeqs), globalSeqs)
			return nil, nil, fmt.Errorf("glob %d not found in any slice: %w", glob, err)
		}
		slicesNeeded[sliceInfoDisk.SliceNum] = true
		globToSlice[glob] = sliceInfoDisk.SliceNum
	}
	phase1aDuration := time.Since(phase1aStart)
	_ = metadataHits

	// Phase 1b: Parallel slice initialization
	phase1bStart := time.Now()
	var sliceLoadWg sync.WaitGroup
	sliceCache := make(map[uint32]*sliceReader)
	var sliceCacheMu sync.Mutex
	var sliceLoadErr error
	var sliceLoadErrMu sync.Mutex

	// Only load slices if we have unresolved globs (legacy path) or need them for Phase 2
	needSliceLoad := len(globToSlice) > 0
	if needSliceLoad {
		for sliceNum := range slicesNeeded {
			sliceLoadWg.Add(1)
			go func(sn uint32) {
				defer sliceLoadWg.Done()
				reader, err := sr.getSliceReader(sn)
				if err != nil {
					sliceLoadErrMu.Lock()
					if sliceLoadErr == nil {
						sliceLoadErr = fmt.Errorf("failed to load slice %d: %w", sn, err)
					}
					sliceLoadErrMu.Unlock()
					return
				}
				sliceCacheMu.Lock()
				sliceCache[sn] = reader
				sliceCacheMu.Unlock()
			}(sliceNum)
		}
		sliceLoadWg.Wait()

		if sliceLoadErr != nil {
			return nil, nil, sliceLoadErr
		}
	}
	phase1bDuration := time.Since(phase1bStart)

	// Phase 1c: Block lookups for unresolved globs
	phase1cStart := time.Now()
	for i, glob := range globalSeqs {
		if lookupResults[i].blockNum != 0 || lookupResults[i].sliceNum != 0 {
			continue // Already resolved from BlockIndexCache
		}

		sliceNum := globToSlice[glob]
		reader := sliceCache[sliceNum]

		blockNum, err := reader.binarySearchGlob(glob)
		if err != nil {
			return nil, nil, fmt.Errorf("glob %d not found in slice %d: %w", glob, sliceNum, err)
		}

		lookupResults[i] = lookupResult{globalSeq: glob, sliceNum: sliceNum, blockNum: blockNum}
	}
	phase1cDuration := time.Since(phase1cStart)
	phase1Duration := time.Since(phase1Start)

	for sliceNum := range sliceCache {
		sr.releaseSliceReader(sliceNum)
	}

	// Group blocks by slice for efficient batch processing
	// This reduces LRU cache thrashing by processing all blocks from one slice together
	type sliceBlocks struct {
		blocks map[uint32][]target // blockNum → targets
	}
	sliceBlockMap := make(map[uint32]*sliceBlocks)
	for i, lr := range lookupResults {
		sb, ok := sliceBlockMap[lr.sliceNum]
		if !ok {
			sb = &sliceBlocks{blocks: make(map[uint32][]target)}
			sliceBlockMap[lr.sliceNum] = sb
		}
		sb.blocks[lr.blockNum] = append(sb.blocks[lr.blockNum], target{
			globalSeq: lr.globalSeq,
			origIndex: i,
			sliceNum:  lr.sliceNum,
		})
	}

	type blockResult struct {
		actions map[int]chain.ActionTrace // origIndex → action
		err     error
	}

	phase2Start := time.Now()
	var cacheHits, cacheMisses int64
	var decompressTime, parseTime, matchTime int64 // nanoseconds, aggregated across goroutines
	var decompressCount, parseCount int64
	var totalBytesRead int64

	// Count total blocks for result channel sizing
	totalBlocks := 0
	for _, sb := range sliceBlockMap {
		totalBlocks += len(sb.blocks)
	}

	resultChan := make(chan blockResult, totalBlocks)
	var wg sync.WaitGroup

	for sliceNum, sb := range sliceBlockMap {
		wg.Add(1)
		go func(sn uint32, blocks map[uint32][]target) {
			defer wg.Done()

			// Sort block numbers for sequential access within slice
			blockNums := make([]uint32, 0, len(blocks))
			for bn := range blocks {
				blockNums = append(blockNums, bn)
			}
			sort.Slice(blockNums, func(i, j int) bool { return blockNums[i] < blockNums[j] })

			var reader *sliceReader
			var readerErr error

			for _, bn := range blockNums {
				tgts := blocks[bn]

				var blob *lazyBlockBlob
				if cached := sr.blockCache.Get(bn); cached != nil {
					blob = cached
					atomic.AddInt64(&cacheHits, 1)
				} else {
					atomic.AddInt64(&cacheMisses, 1)

					if reader == nil {
						reader, readerErr = sr.getSliceReader(sn)
						if readerErr != nil {
							resultChan <- blockResult{err: fmt.Errorf("failed to get slice reader for slice %d: %w", sn, readerErr)}
							return
						}
					}

					decompStart := time.Now()
					blockData, err := reader.readBlockData(bn)
					if err != nil {
						resultChan <- blockResult{err: fmt.Errorf("failed to read block %d: %w", bn, err)}
						sr.releaseSliceReader(sn)
						return
					}
					atomic.AddInt64(&decompressTime, int64(time.Since(decompStart)))
					atomic.AddInt64(&decompressCount, 1)
					atomic.AddInt64(&totalBytesRead, int64(len(blockData)))

					parseStart := time.Now()
					blob = bytesToBlockBlobLazy(blockData)
					atomic.AddInt64(&parseTime, int64(time.Since(parseStart)))
					atomic.AddInt64(&parseCount, 1)

					sr.blockCache.Put(bn, blob, estimateBlobSize(blob))
				}

				matchStart := time.Now()
				actions := make(map[int]chain.ActionTrace)

				targetSet := make(map[uint64][]target, len(tgts))
				for _, tgt := range tgts {
					targetSet[tgt.globalSeq] = append(targetSet[tgt.globalSeq], tgt)
				}

				for i, globSeq := range blob.GlobalSeqs {
					if targets, found := targetSet[globSeq]; found {
						cat := blob.GetAction(i)
						at := cat.At(blob.Block, bn, globSeq)
						for _, tgt := range targets {
							actions[tgt.origIndex] = *at
						}
					}
				}
				atomic.AddInt64(&matchTime, int64(time.Since(matchStart)))

				resultChan <- blockResult{actions: actions}
			}

			// Release slice reader after processing all blocks
			if reader != nil {
				sr.releaseSliceReader(sn)
			}
		}(sliceNum, sb.blocks)
	}

	wg.Wait()
	close(resultChan)
	phase2Duration := time.Since(phase2Start)

	results := make([]chain.ActionTrace, len(globalSeqs))
	for res := range resultChan {
		if res.err != nil {
			return nil, nil, res.err
		}
		for origIdx, action := range res.actions {
			results[origIdx] = action
		}
	}

	totalDuration := time.Since(totalStart)

	var sliceMissSummary string
	sliceCount := len(sliceBlockMap)
	if sliceCount == 0 {
		sliceMissSummary = "none"
	} else {
		for sn, sb := range sliceBlockMap {
			if sliceMissSummary != "" {
				sliceMissSummary += " "
			}
			sliceMissSummary += fmt.Sprintf("s%d:%d", sn, len(sb.blocks))
		}
	}

	bicStatus := "nil"
	if sr.blockIndexCache != nil {
		stats := sr.blockIndexCache.GetStats()
		bicStatus = fmt.Sprintf("%d/%d", stats.FinalizedSlices, stats.TotalSlices)
	}
	metaSlices := sr.sharedMetadata.getSliceCount()
	logger.Printf("debug-perf", "GetActionsByGlobalSeqs: total=%v phase1=%v(a=%v[disk=%d,bic=%d] b=%v c=%v) phase2=%v | blocks=%d globs=%d | blockCache=%d/%d | decompress=%v(%d) parse=%v(%d) match=%v | bytes=%.1fKB slices=%d [%s] bic=%s meta=%d",
		totalDuration, phase1Duration, phase1aDuration, diskLookups, blockIndexCacheHits, phase1bDuration, phase1cDuration, phase2Duration,
		totalBlocks, len(globalSeqs),
		cacheHits, cacheHits+cacheMisses,
		time.Duration(decompressTime), decompressCount,
		time.Duration(parseTime), parseCount,
		time.Duration(matchTime),
		float64(totalBytesRead)/1024.0, sliceCount, sliceMissSummary, bicStatus, metaSlices)

	timings := &FetchTimings{
		Total:             totalDuration,
		Phase1:            phase1Duration,
		Phase1a:           phase1aDuration,
		Phase1b:           phase1bDuration,
		Phase1c:           phase1cDuration,
		Phase1DiskLookups: diskLookups,
		Phase2:            phase2Duration,
		DecompressTime:    time.Duration(decompressTime),
		DecompressCount:   int(decompressCount),
		ParseTime:         time.Duration(parseTime),
		ParseCount:        int(parseCount),
		MatchTime:         time.Duration(matchTime),
		CacheHits:         int(cacheHits),
		CacheMisses:       int(cacheMisses),
		TotalBytes:        totalBytesRead,
		BlockCount:        totalBlocks,
		SliceCount:        sliceCount,
	}

	return results, timings, nil
}

func (sr *SliceReader) loadSliceBuffer(sliceInfo SliceInfo) {
	if !sliceInfo.Finalized {
		return
	}

	if sr.bufferPool.Has(sliceInfo.SliceNum) {
		return // Already buffered
	}

	sr.bufferLoadMu.Lock()

	if sr.sliceLoading[sliceInfo.SliceNum] {
		sr.bufferLoadMu.Unlock()

		for i := 0; i < 100; i++ { // Max 1 second wait (10ms * 100)
			time.Sleep(10 * time.Millisecond)
			if sr.bufferPool.Has(sliceInfo.SliceNum) {
				return // Now buffered
			}
		}
		logger.Warning("Timeout waiting for slice %d to be buffered, skipping duplicate load",
			sliceInfo.SliceNum)
		return
	}

	if sr.bufferPool.Has(sliceInfo.SliceNum) {
		sr.bufferLoadMu.Unlock()
		return // Already buffered
	}

	sr.sliceLoading[sliceInfo.SliceNum] = true
	sr.bufferLoadMu.Unlock()

	data, index, err := loadSliceData(sr.basePath, sliceInfo)

	sr.bufferLoadMu.Lock()
	delete(sr.sliceLoading, sliceInfo.SliceNum)
	sr.bufferLoadMu.Unlock()

	if err != nil {
		logger.Warning("Failed to buffer slice %d (blocks %d-%d): %v",
			sliceInfo.SliceNum, sliceInfo.StartBlock, sliceInfo.EndBlock, err)
		return
	}

	wasAdded := sr.bufferPool.LoadSlice(sliceInfo.SliceNum, data, index, sliceInfo.Finalized)

	_ = wasAdded
}

func (sr *SliceReader) GetBufferPoolStats() (slicesLoaded, blocksServed, evictions, skipped int64, usedMB, maxMB int64, bufferCount int) {
	if sr.bufferPool == nil {
		return 0, 0, 0, 0, 0, 0, 0
	}
	return sr.bufferPool.GetStats()
}

func (sr *SliceReader) LogBufferPoolStats() {
	if sr.bufferPool != nil {
		sr.bufferPool.LogStats()
	}
}
