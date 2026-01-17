package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/config"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/corestream"
	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/profiler"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/libraries/tracereader"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version = "dev"

var (
	logCategories = []string{
		"startup", "sync", "config", "store", "validation", "http", "stream", "shutdown", "download",
		"debug", "debug-timing", "debug-glob", "debug-ordering", "debug-shutdown",
		"debug-slice", "debug-trace", "debug-profiling", "debug-startup", "profiler", "slice",
	}
)

type workType struct {
	res    *blockBlobHelper
	blknum uint32
}

type streamBlockProvider struct {
	store appendlog.StoreInterface
}

func (p *streamBlockProvider) GetBlock(blockNum uint32) ([]byte, error) {
	return p.store.GetBlock(blockNum)
}

func (p *streamBlockProvider) GetBlockByGlob(glob uint64) (uint32, error) {
	return p.store.GetBlockByGlob(glob)
}

func (p *streamBlockProvider) GetLIB() uint32 {
	return p.store.GetLIB()
}

func (p *streamBlockProvider) GetHead() uint32 {
	return p.store.GetHead()
}

func (p *streamBlockProvider) GetActionsByGlobs(globalSeqs []uint64) ([]corestream.ActionData, error) {
	if len(globalSeqs) == 0 {
		return []corestream.ActionData{}, nil
	}

	type target struct {
		globalSeq uint64
		origIndex int
	}
	blockMap := make(map[uint32][]target)

	for origIdx, glob := range globalSeqs {
		blockNum, err := p.store.GetBlockByGlob(glob)
		if err != nil {
			return nil, fmt.Errorf("glob %d not found: %w", glob, err)
		}
		blockMap[blockNum] = append(blockMap[blockNum], target{globalSeq: glob, origIndex: origIdx})
	}

	results := make([]corestream.ActionData, len(globalSeqs))

	for blockNum, targets := range blockMap {
		blockBytes, err := p.store.GetBlock(blockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %w", blockNum, err)
		}

		blob := bytesToBlockBlob(blockBytes)
		blockTimeUint32 := chain.TimeToUint32(blob.Block.BlockTime)

		for i, cat := range blob.Cats {
			globSeq := cat.GlobalSequence
			for _, tgt := range targets {
				if globSeq == tgt.globalSeq {
					actionBytes := blob.CatsBytes[i]
					results[tgt.origIndex] = corestream.ActionData{
						GlobalSeq: globSeq,
						BlockNum:  blockNum,
						BlockTime: blockTimeUint32,
						Data:      actionBytes,
					}
					break
				}
			}
		}
	}

	return results, nil
}

type prefetchedStride struct {
	strideNum   uint32
	strideStart uint32
	strideEnd   uint32
	rawBlocks   []tracereader.RawBlockData // OPTIMIZATION: Use raw blocks (skip conversion)
	err         error
}

type writeBatch struct {
	entries []appendlog.BlockEntry
	done    chan error // Signal completion
}

type processedStride struct {
	strideNum   uint32
	strideStart uint32
	strideEnd   uint32
	blocks      []appendlog.BlockEntry // All blocks in stride, in order
	abis        []abiEntry             // ABIs extracted from setabi actions
}

type abiEntry struct {
	blockNum uint32
	contract uint64
	abiJSON  []byte
}

var (
	totalGetBlocksNs  int64 // Time in tracereader.GetBlocks (reading trace files - INPUT I/O)
	totalProcessNs    int64 // Time processing blocks (compress + serialize - CPU)
	totalChanSendNs   int64 // Time waiting on channel sends (backpressure)
	totalMainLoopNs   int64 // Time in main loop (sequential processing + coordination)
	totalStorageNs    int64 // Time writing to storage (slice writes - OUTPUT I/O)
	totalStridesCount int64 // Number of strides processed
	totalBlocksCount  int64

	mainLoopLockNs    int64
	mainLoopMapNs     int64 // Time doing map lookup/delete operations
	mainLoopDrainNs   int64 // Time draining from merged channel
	mainLoopOtherNs   int64 // Time in gap detection, logging, etc.
	mainLoopBlocksCnt int64
)

var (
	statsActions         int64  // Total actions processed
	statsTxns            int64  // Total unique transactions
	statsBytes           int64  // Total compressed bytes written
	statsGlobMax         uint64 // Max global sequence seen
	statsProcessedBlocks int64  // Blocks successfully processed from trace files
)

var (
	flushStatsCount     int64
	flushStatsTotalNs   int64
	flushStatsWaitNs    int64
	flushStatsTotalBlks int64
	flushStatsMaxNs     int64
	flushStatsLastLog   time.Time
	flushStatsMu        sync.Mutex
)

var (
	statsPendingBlocks     int64  // Current size of pendingBlocks map
	statsLastHeapAlloc     uint64 // HeapAlloc at last log (for delta calculation)
	statsLastNumGC         uint32 // NumGC at last log
	statsLastPauseTotalNs  uint64 // PauseTotalNs at last log
	statsDecodeBufferBytes int64  // Total capacity of all decode buffers (updated by workers)
)

const maxWorkers = 64
const maxPrefetchers = 8

var (
	workerBlocksProcessed [maxWorkers]int64 // Blocks processed per worker (atomic)
)

var (
	statsMinPendingBlock uint32 // Minimum block number in pending map (atomic via mutex)
	statsMaxPendingBlock uint32 // Maximum block number in pending map
	statsNextNeededBlock uint32 // The block we're waiting for (nextBlock)
)

var (
	drainLoopIterations  int64 // Total iterations in drain loop (looking for consecutive blocks)
	drainLoopHits        int64 // Successful finds (block was in pending map)
	drainLoopMisses      int64 // Miss count (block not yet in pending map)
	drainLoopMinMaxIters int64 // Iterations spent computing min/max (O(n) cost)
)

var (
	prefetcherReadTimeNs   [maxPrefetchers]int64
	prefetcherStridesRead  [maxPrefetchers]int64
	prefetcherLastStrideNs [maxPrefetchers]int64

	workerLastStrideNum   [maxWorkers]uint32
	workerStrideTimeNs    [maxWorkers]int64 // Time processing strides per worker
	workerStridesInterval [maxWorkers]int64 // Strides processed this interval
)

// printMemoryStats outputs memory usage breakdown for leak detection
func printMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate deltas since last log
	heapDelta := int64(m.HeapAlloc) - int64(statsLastHeapAlloc)
	gcCount := m.NumGC - statsLastNumGC
	gcPauseNs := m.PauseTotalNs - statsLastPauseTotalNs

	// Get pending blocks count and decode buffer size
	pending := atomic.LoadInt64(&statsPendingBlocks)
	decodeBufs := atomic.LoadInt64(&statsDecodeBufferBytes)

	// Format helper (inline to avoid conflict with trace_cleanup.go)
	fmtBytes := func(b uint64) string {
		if b >= 1024*1024*1024 {
			return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
		} else if b >= 1024*1024 {
			return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
		}
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	}

	// Log memory stats
	// HeapAlloc: bytes allocated and still in use
	// HeapSys: bytes obtained from OS for heap
	// HeapObjects: number of allocated heap objects
	// Pending: blocks waiting in pendingBlocks map
	// DecodeBufs: total capacity of worker decode buffers
	logger.Printf("debug-timing", "MEMORY: Heap=%s (Δ%+.1f MB) | Sys=%s | Objects=%d | GCs=%d (%.1fms) | Pending=%d | DecodeBufs=%s",
		fmtBytes(m.HeapAlloc),
		float64(heapDelta)/(1024*1024),
		fmtBytes(m.HeapSys),
		m.HeapObjects,
		gcCount,
		float64(gcPauseNs)/1e6,
		pending,
		fmtBytes(uint64(decodeBufs)))

	// Additional breakdown if heap is growing significantly (>100MB delta)
	if heapDelta > 100*1024*1024 {
		logger.Printf("debug-timing", "MEMORY GROWTH: HeapIdle=%s | HeapReleased=%s | StackInuse=%s | MSpanInuse=%s",
			fmtBytes(m.HeapIdle),
			fmtBytes(m.HeapReleased),
			fmtBytes(m.StackInuse),
			fmtBytes(m.MSpanInuse))
	}

	// Update last values for next delta calculation
	statsLastHeapAlloc = m.HeapAlloc
	statsLastNumGC = m.NumGC
	statsLastPauseTotalNs = m.PauseTotalNs
}

// Per-worker stats tracking (reset each interval)
var (
	lastWorkerBlocks [maxWorkers]int64
)

// printWorkerStats outputs per-worker BPS and block gap analysis
func printWorkerStats(workerCount uint32, intervalSeconds float64) {
	if workerCount == 0 || workerCount > maxWorkers || intervalSeconds <= 0 {
		return
	}

	// Calculate per-worker BPS for this interval
	workerBPSValues := make([]int64, workerCount)
	var workerBPSStrs []string
	var minBPS, maxBPS int64 = 1 << 62, 0

	for i := uint32(0); i < workerCount; i++ {
		current := atomic.LoadInt64(&workerBlocksProcessed[i])
		delta := current - lastWorkerBlocks[i]
		lastWorkerBlocks[i] = current

		bps := int64(float64(delta) / intervalSeconds)
		workerBPSValues[i] = bps

		if bps < minBPS {
			minBPS = bps
		}
		if bps > maxBPS {
			maxBPS = bps
		}

		workerBPSStrs = append(workerBPSStrs, fmt.Sprintf("%d", bps))
	}

	// Count stragglers (workers producing < 50% of max)
	stragglers := 0
	if maxBPS > 0 {
		threshold := maxBPS / 2
		for i := uint32(0); i < workerCount; i++ {
			if workerBPSValues[i] < threshold {
				stragglers++
			}
		}
	}

	// Log per-worker BPS
	logger.Printf("debug-timing", "WorkerBPS: [%s] | range: %d-%d | stragglers: %d (<%d BPS)",
		strings.Join(workerBPSStrs, ","), minBPS, maxBPS, stragglers, maxBPS/2)

	// Log block gap info (min/max tracked incrementally - O(1))
	nextNeeded := atomic.LoadUint32(&statsNextNeededBlock)
	minPending := atomic.LoadUint32(&statsMinPendingBlock)
	maxPending := atomic.LoadUint32(&statsMaxPendingBlock)
	pendingCount := atomic.LoadInt64(&statsPendingBlocks)

	if pendingCount > 0 {
		gap := int64(minPending) - int64(nextNeeded)
		spread := int64(maxPending) - int64(minPending)
		logger.Printf("debug-timing", "BlockGap: need=%d min=%d max=%d | gap=%d spread=%d pending=%d",
			nextNeeded, minPending, maxPending, gap, spread, pendingCount)
	}
}

// printStrideStats outputs stride-level diagnostics for identifying bottlenecks
func printStrideStats(workerCount uint32, prefetcherCount uint32, strideSize uint32, intervalSeconds float64) {
	if strideSize == 0 || intervalSeconds <= 0 {
		return
	}

	// Calculate which stride contains nextBlock
	nextNeeded := atomic.LoadUint32(&statsNextNeededBlock)
	neededStride := nextNeeded / strideSize

	// Get min/max pending blocks to determine stride range in pending
	minPending := atomic.LoadUint32(&statsMinPendingBlock)
	maxPending := atomic.LoadUint32(&statsMaxPendingBlock)

	minPendingStride := uint32(0)
	maxPendingStride := uint32(0)
	if minPending > 0 {
		minPendingStride = minPending / strideSize
		maxPendingStride = maxPending / strideSize
	}

	// Per-prefetcher stats
	var prefetcherStrs []string
	var slowestPrefetcher int = -1
	var slowestAvgMs float64 = 0
	for p := uint32(0); p < prefetcherCount && p < maxPrefetchers; p++ {
		readTime := atomic.SwapInt64(&prefetcherReadTimeNs[p], 0)
		strides := atomic.SwapInt64(&prefetcherStridesRead[p], 0)
		lastNs := atomic.LoadInt64(&prefetcherLastStrideNs[p])

		avgMs := float64(0)
		if strides > 0 {
			avgMs = float64(readTime) / float64(strides) / 1e6
		}
		lastMs := float64(lastNs) / 1e6

		prefetcherStrs = append(prefetcherStrs, fmt.Sprintf("P%d:%d@%.0fms", p, strides, avgMs))

		if avgMs > slowestAvgMs {
			slowestAvgMs = avgMs
			slowestPrefetcher = int(p)
		}

		// Warn if last stride was very slow (>1s)
		if lastMs > 1000 {
			logger.Printf("debug-timing", "⚠️  Prefetcher %d slow: last stride took %.0fms", p, lastMs)
		}
	}

	// Per-worker stride stats - find which workers are processing which strides
	var workerStrideStrs []string
	for w := uint32(0); w < workerCount && w < maxWorkers; w++ {
		lastStride := atomic.LoadUint32(&workerLastStrideNum[w])
		stridesThisInterval := atomic.SwapInt64(&workerStridesInterval[w], 0)
		strideTimeNs := atomic.SwapInt64(&workerStrideTimeNs[w], 0)

		avgStrideMs := float64(0)
		if stridesThisInterval > 0 {
			avgStrideMs = float64(strideTimeNs) / float64(stridesThisInterval) / 1e6
		}

		// Calculate how far ahead this worker is from needed stride
		strideAhead := int32(lastStride) - int32(neededStride)
		workerStrideStrs = append(workerStrideStrs, fmt.Sprintf("W%d:s%d(%+d)@%.0fms", w, lastStride, strideAhead, avgStrideMs))
	}

	// Stride gap analysis
	strideGap := int32(minPendingStride) - int32(neededStride)
	strideSpread := int32(maxPendingStride) - int32(minPendingStride)

	logger.Printf("debug-timing", "StrideGap: need=s%d min=s%d max=s%d | gap=%d spread=%d",
		neededStride, minPendingStride, maxPendingStride, strideGap, strideSpread)

	// Log prefetcher performance
	if len(prefetcherStrs) > 0 {
		logger.Printf("debug-timing", "Prefetchers: [%s] | slowest: P%d (%.0fms avg)",
			strings.Join(prefetcherStrs, " "), slowestPrefetcher, slowestAvgMs)
	}

	// Log which strides each worker is on (shows if one worker is behind)
	if len(workerStrideStrs) > 0 {
		logger.Printf("debug-timing", "WorkerStrides: [%s]", strings.Join(workerStrideStrs, " "))
	}
}

// printTimingStats outputs timing breakdown - call periodically during sync
func printTimingStats() {
	getBlocks := atomic.LoadInt64(&totalGetBlocksNs)
	process := atomic.LoadInt64(&totalProcessNs)
	chanSend := atomic.LoadInt64(&totalChanSendNs)
	mainLoop := atomic.LoadInt64(&totalMainLoopNs)
	storage := atomic.LoadInt64(&totalStorageNs)
	strides := atomic.LoadInt64(&totalStridesCount)
	blocks := atomic.LoadInt64(&totalBlocksCount)

	if strides == 0 || blocks == 0 {
		return
	}

	totalNs := getBlocks + process + chanSend + mainLoop + storage
	if totalNs == 0 {
		return
	}

	// Per-unit averages
	avgGetBlocksPerStride := float64(getBlocks) / float64(strides) / 1e6 // ms
	avgProcessPerBlock := float64(process) / float64(blocks) / 1e3       // µs
	avgChanSendPerBlock := float64(chanSend) / float64(blocks) / 1e3     // µs
	avgMainLoopPerBlock := float64(mainLoop) / float64(blocks) / 1e3     // µs
	avgStoragePerBlock := float64(storage) / float64(blocks) / 1e3       // µs

	// Percentages
	pctGetBlocks := float64(getBlocks) / float64(totalNs) * 100
	pctProcess := float64(process) / float64(totalNs) * 100
	pctChanSend := float64(chanSend) / float64(totalNs) * 100
	pctMainLoop := float64(mainLoop) / float64(totalNs) * 100
	pctStorage := float64(storage) / float64(totalNs) * 100

	// Combined I/O percentages for bottleneck identification
	pctInputIO := pctGetBlocks // Trace file reads
	pctOutputIO := pctStorage  // Slice writes
	pctTotalIO := pctInputIO + pctOutputIO

	logger.Printf("debug-timing", "TraceRead=%.1f%% (%.1fms/stride) | Process=%.1f%% (%.0fµs/blk) | ChanSend=%.1f%% (%.0fµs/blk) | MainLoop=%.1f%% (%.0fµs/blk) | Storage=%.1f%% (%.0fµs/blk) | TotalIO=%.1f%%",
		pctGetBlocks, avgGetBlocksPerStride,
		pctProcess, avgProcessPerBlock,
		pctChanSend, avgChanSendPerBlock,
		pctMainLoop, avgMainLoopPerBlock,
		pctStorage, avgStoragePerBlock,
		pctTotalIO)

	// Print flush batch stats if available
	flushStatsMu.Lock()
	if flushStatsCount > 0 {
		n := float64(flushStatsCount)
		logger.Printf("debug-timing", "FlushBatch: %d flushes, %d blocks, avg=%.2fms max=%.2fms, wait=%.2fms",
			flushStatsCount, flushStatsTotalBlks,
			float64(flushStatsTotalNs)/n/1e6,
			float64(flushStatsMaxNs)/1e6,
			float64(flushStatsWaitNs)/n/1e6)
		flushStatsCount = 0
		flushStatsTotalNs = 0
		flushStatsWaitNs = 0
		flushStatsTotalBlks = 0
		flushStatsMaxNs = 0
	}
	flushStatsMu.Unlock()

	// Print main loop breakdown if available
	mlLock := atomic.LoadInt64(&mainLoopLockNs)
	mlMap := atomic.LoadInt64(&mainLoopMapNs)
	mlDrain := atomic.LoadInt64(&mainLoopDrainNs)
	mlOther := atomic.LoadInt64(&mainLoopOtherNs)
	mlBlocks := atomic.LoadInt64(&mainLoopBlocksCnt)

	if mlBlocks > 0 {
		mlTotal := mlLock + mlMap + mlDrain + mlOther
		if mlTotal > 0 {
			logger.Printf("debug-timing", "MainLoopBreakdown: lock=%.0fµs/blk (%.1f%%) | map=%.0fµs/blk (%.1f%%) | drain=%.0fµs/blk (%.1f%%) | other=%.0fµs/blk (%.1f%%)",
				float64(mlLock)/float64(mlBlocks)/1e3, float64(mlLock)/float64(mlTotal)*100,
				float64(mlMap)/float64(mlBlocks)/1e3, float64(mlMap)/float64(mlTotal)*100,
				float64(mlDrain)/float64(mlBlocks)/1e3, float64(mlDrain)/float64(mlTotal)*100,
				float64(mlOther)/float64(mlBlocks)/1e3, float64(mlOther)/float64(mlTotal)*100)
		}
		// Reset counters
		atomic.StoreInt64(&mainLoopLockNs, 0)
		atomic.StoreInt64(&mainLoopMapNs, 0)
		atomic.StoreInt64(&mainLoopDrainNs, 0)
		atomic.StoreInt64(&mainLoopOtherNs, 0)
		atomic.StoreInt64(&mainLoopBlocksCnt, 0)
	}

	// Print drain loop efficiency stats
	iterations := atomic.SwapInt64(&drainLoopIterations, 0)
	hits := atomic.SwapInt64(&drainLoopHits, 0)
	misses := atomic.SwapInt64(&drainLoopMisses, 0)
	minMaxIters := atomic.SwapInt64(&drainLoopMinMaxIters, 0)

	if iterations > 0 {
		hitRate := float64(hits) / float64(iterations) * 100
		// Average consecutive hits before miss (efficiency metric)
		avgConsecutive := float64(0)
		if misses > 0 {
			avgConsecutive = float64(hits) / float64(misses)
		}
		minMaxOverhead := float64(minMaxIters) / float64(iterations)
		logger.Printf("debug-timing", "DrainLoop: iters=%d hits=%d (%.1f%%) misses=%d | avgConsecutive=%.1f | minMaxIters=%d (%.1f×overhead)",
			iterations, hits, hitRate, misses, avgConsecutive, minMaxIters, minMaxOverhead)
	}
}

// BroadcastFunc is called after each block is written to storage (for streaming to remote clients)
type BroadcastFunc func(blockNum uint32, data []byte)

// saveSliceCache saves slice metadata cache for fast reader startup
func saveSliceCache(store *appendlog.SliceStore) error {
	sliceInfos := store.GetSliceInfos()
	if len(sliceInfos) == 0 {
		return nil
	}

	hrSlices := make([]corereader.SliceInfo, len(sliceInfos))
	for i, s := range sliceInfos {
		hrSlices[i] = corereader.SliceInfo{
			SliceNum:       s.SliceNum,
			StartBlock:     s.StartBlock,
			EndBlock:       s.EndBlock,
			MaxBlock:       s.MaxBlock,
			BlocksPerSlice: s.BlocksPerSlice,
			Finalized:      s.Finalized,
			GlobMin:        s.GlobMin,
			GlobMax:        s.GlobMax,
		}
	}
	return corereader.SaveSlicesToCache(store.GetBasePath(), hrSlices)
}

// isSliceError checks if an error is slice-related and should trigger validation/repair
func isSliceError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	slicePatterns := []string{
		"block count mismatch",
		"slice validation failed",
		"failed to finalize slice",
		"failed to rotate slice",
		"failed to get active slice",
		"failed to append block",
		"CRC mismatch",
		"unexpected EOF",
	}
	for _, pattern := range slicePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}

// Sync blocks from trace files into append-only store
func syncFromTraceFiles(config *server.Config, traceConfig *tracereader.Config, workercount uint32, prefetchercount uint32, store appendlog.StoreInterface, abiWriter *abicache.Writer, start uint32, end uint32, exit *bool, exitChan <-chan struct{}, broadcast BroadcastFunc, logInterval time.Duration) error {
	stride := uint32(traceConfig.Stride)

	// Calculate stride range
	startStride := start / stride
	endStride := end / stride

	// Stride output channel: workers send complete strides, main loop reorders
	// Buffer allows workers to get ahead, but bounded to limit memory (~20 strides max)
	// 20 strides × 500 blocks × ~170KB/block ≈ 1.7GB max buffered
	strideChanSize := workercount + prefetchercount
	if strideChanSize > 24 {
		strideChanSize = 24
	}
	strideChan := make(chan *processedStride, strideChanSize)

	// Pending strides for reordering (typically <20 items)
	pendingStrides := make(map[uint32]*processedStride)

	// Detect small batch sync for detailed per-block logging
	blocksToSync := end - start + 1
	isSmallBatch := blocksToSync < 100

	// Create write queue for async storage writes (overlaps I/O with processing)
	writeQueue := make(chan *writeBatch, 10)
	var writeWg sync.WaitGroup

	// Start async writer goroutine (parallelizes storage writes with main loop)
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		for batch := range writeQueue {
			tStart := time.Now()
			err := store.AppendBlockBatch(batch.entries)
			tElapsed := time.Since(tStart)

			atomic.AddInt64(&totalStorageNs, tElapsed.Nanoseconds())

			if err == nil && broadcast != nil {
				for _, entry := range batch.entries {
					broadcast(entry.BlockNum, entry.Data)
				}
			}

			batch.done <- err
			close(batch.done)
		}
	}()

	// Start workers (they now output complete strides)
	go gophers(config, traceConfig, strideChan, workercount, prefetchercount, start, end, exit, exitChan)

	// Process strides in order
	startTime := time.Now()
	lastLogTime := startTime
	nextBlock := start
	nextStrideNum := startStride

	// Auto-detect bulk sync mode
	isBulkSync := blocksToSync > 100000

	flushInterval := (*config).PrintSyncEvery
	sliceStore, ok := store.(*appendlog.SliceStore)
	if ok && isBulkSync {
		sliceSize := sliceStore.GetBlocksPerSlice()
		if flushInterval < sliceSize {
			flushInterval = sliceSize
			if (*config).Debug {
				logger.Printf("sync", "Bulk sync mode detected (%d blocks): aligning flush-interval=%d with slice-size",
					blocksToSync, flushInterval)
			}
		}
	}

	const syncEvery = 50000
	const batchSize = 200

	var lastLogActions int64
	var lastLogTxns int64
	var lastLogBytes int64
	var lastLogProcessed int64
	lastLogBlock := start

	batch := make([]appendlog.BlockEntry, 0, batchSize)
	var sliceErr error

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		flushStart := time.Now()
		batchLen := len(batch)

		wb := &writeBatch{
			entries: make([]appendlog.BlockEntry, len(batch)),
			done:    make(chan error, 1),
		}
		copy(wb.entries, batch)

		writeQueue <- wb

		waitStart := time.Now()
		err := <-wb.done
		waitTime := time.Since(waitStart)
		if err != nil {
			return err
		}

		totalFlushTime := time.Since(flushStart)

		flushStatsMu.Lock()
		flushStatsCount++
		flushStatsTotalNs += totalFlushTime.Nanoseconds()
		flushStatsWaitNs += waitTime.Nanoseconds()
		flushStatsTotalBlks += int64(batchLen)
		if totalFlushTime.Nanoseconds() > flushStatsMaxNs {
			flushStatsMaxNs = totalFlushTime.Nanoseconds()
		}
		flushStatsMu.Unlock()

		batch = batch[:0]
		return nil
	}

	// writeStrideBlocks writes all blocks from a stride in order
	writeStrideBlocks := func(ps *processedStride) error {
		for _, entry := range ps.blocks {
			if *exit {
				break
			}

			// Check for missing block
			if entry.Data == nil {
				logger.Error("========================================================")
				logger.Error("FATAL: Block %d is missing from trace files", entry.BlockNum)
				logger.Error("========================================================")
				logger.Error("This would create a gap in the block sequence and corrupt indexes.")
				logger.Error("Tracewriter has synced up to block %d.", entry.BlockNum-1)
				logger.Error("")
				logger.Error("Resolution options:")
				logger.Error("  1. Restore missing trace files for block %d", entry.BlockNum)
				logger.Error("  2. Use --start-block %d to skip past the gap", entry.BlockNum+1)
				logger.Error("  3. Use --replay to erase all data and restart from block 2")
				logger.Error("========================================================")
				logger.Fatal("Block %d missing from trace files (sync stopped to prevent corruption)", entry.BlockNum)
			}

			batch = append(batch, entry)

			// Per-block logging for small batches
			if isSmallBatch {
				blockBytes := len(entry.Data)
				var bytesStr string
				if blockBytes >= 1024*1024 {
					bytesStr = fmt.Sprintf("%.1f MB", float64(blockBytes)/(1024*1024))
				} else if blockBytes >= 1024 {
					bytesStr = fmt.Sprintf("%.1f KB", float64(blockBytes)/1024)
				} else {
					bytesStr = fmt.Sprintf("%d B", blockBytes)
				}
				logger.Printf("sync", "Block: %d / %d | %s | glob: %d-%d",
					entry.BlockNum, end, bytesStr, entry.GlobMin, entry.GlobMax)
			}

			// Flush batch when full
			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}

			// Periodic flush
			if entry.BlockNum%flushInterval == 0 {
				if err := flushBatch(); err != nil {
					return err
				}
				if err := store.Flush(); err != nil {
					logger.Error("Failed to flush: %v", err)
				}
			}

			// Save indices periodically
			if entry.BlockNum%syncEvery == 0 {
				if err := flushBatch(); err != nil {
					return err
				}
				if err := store.Sync(); err != nil {
					logger.Error("Failed to sync indices: %v", err)
				}
			}

			nextBlock = entry.BlockNum + 1
		}

		for _, abi := range ps.abis {
			if err := abiWriter.Write(abi.blockNum, abi.contract, abi.abiJSON); err != nil {
				logger.Printf("warning", "Failed to write ABI for %d at block %d: %v", abi.contract, abi.blockNum, err)
			}
		}
		return nil
	}

	// Main loop: receive strides, reorder, write blocks
mainLoop:
	for !(*exit) && nextStrideNum <= endStride {
		// Try to drain consecutive strides we already have
		for {
			ps, ok := pendingStrides[nextStrideNum]
			if !ok {
				break
			}
			delete(pendingStrides, nextStrideNum)

			if err := writeStrideBlocks(ps); err != nil {
				logger.Error("Failed to write stride %d: %v", nextStrideNum, err)
				if isSliceError(err) {
					sliceErr = err
				}
				*exit = true
				break mainLoop
			}

			nextStrideNum++
		}

		// Check if we're done
		if nextStrideNum > endStride {
			break
		}

		// Progress logging
		if time.Since(lastLogTime) >= logInterval {
			intervalElapsed := time.Since(lastLogTime).Seconds()
			pctComplete := float64(nextBlock) / float64(end) * 100.0

			actions := atomic.LoadInt64(&statsActions)
			txns := atomic.LoadInt64(&statsTxns)
			bytes := atomic.LoadInt64(&statsBytes)
			globMax := atomic.LoadUint64(&statsGlobMax)
			processed := atomic.LoadInt64(&statsProcessedBlocks)

			intervalActions := actions - lastLogActions
			intervalTxns := txns - lastLogTxns
			intervalBytes := bytes - lastLogBytes
			intervalProcessed := processed - lastLogProcessed

			intervalBlocks := nextBlock - lastLogBlock
			var storageBPS int64
			if intervalElapsed > 0 {
				storageBPS = int64(float64(intervalBlocks) / intervalElapsed)
			}
			var workerBPS int64
			if intervalElapsed > 0 {
				workerBPS = int64(float64(intervalProcessed) / intervalElapsed)
			}

			var avgActions, avgTxns, avgBytes float64
			if intervalProcessed > 0 {
				avgActions = float64(intervalActions) / float64(intervalProcessed)
				avgTxns = float64(intervalTxns) / float64(intervalProcessed)
				avgBytes = float64(intervalBytes) / float64(intervalProcessed)
			}

			var bytesStr string
			if intervalBytes >= 1024*1024*1024 {
				bytesStr = fmt.Sprintf("%.1f GB", float64(intervalBytes)/(1024*1024*1024))
			} else if intervalBytes >= 1024*1024 {
				bytesStr = fmt.Sprintf("%.1f MB", float64(intervalBytes)/(1024*1024))
			} else if intervalBytes >= 1024 {
				bytesStr = fmt.Sprintf("%.1f KB", float64(intervalBytes)/1024)
			} else {
				bytesStr = fmt.Sprintf("%d B", intervalBytes)
			}

			pendingCount := len(pendingStrides)
			logger.Printf("sync", "Block: %d / %d (%.1f%%)\t%d BPS (w:%d) | %s (%.0f B/blk) | %.1f acts/blk, %.1f txns/blk | glob: %d | pending: %d strides",
				nextBlock, end, pctComplete, storageBPS, workerBPS, bytesStr, avgBytes, avgActions, avgTxns, globMax, pendingCount)

			if config.PrintTiming {
				printTimingStats()
				printMemoryStats()
				printWorkerStats(workercount, intervalElapsed)
				printStrideStats(workercount, prefetchercount, stride, intervalElapsed)
			}

			lastLogActions = actions
			lastLogTxns = txns
			lastLogBytes = bytes
			lastLogProcessed = processed
			lastLogBlock = nextBlock
			lastLogTime = time.Now()
		}

		// Receive next stride (blocking)
		select {
		case ps, ok := <-strideChan:
			if !ok {
				// Channel closed, workers done
				break mainLoop
			}
			// Add to pending map
			pendingStrides[ps.strideNum] = ps
			atomic.StoreInt64(&statsPendingBlocks, int64(len(pendingStrides)))

		case <-exitChan:
			break mainLoop
		}
	}

	// Process any remaining pending strides in order
	for nextStrideNum <= endStride && !(*exit) {
		ps, ok := pendingStrides[nextStrideNum]
		if !ok {
			// Stride not available yet - drain from channel
			ps, ok = <-strideChan
			if !ok {
				break
			}
			if ps.strideNum != nextStrideNum {
				pendingStrides[ps.strideNum] = ps
				continue
			}
		} else {
			delete(pendingStrides, nextStrideNum)
		}

		if err := writeStrideBlocks(ps); err != nil {
			logger.Error("Failed to write stride %d: %v", nextStrideNum, err)
			if isSliceError(err) {
				sliceErr = err
			}
			break
		}
		nextStrideNum++
	}

	// Final flush
	if len(batch) > 0 {
		if *exit {
			logger.Printf("debug-shutdown", "Flushing final batch (%d blocks)...", len(batch))
		}
		if err := flushBatch(); err != nil {
			logger.Error("Failed to flush final batch: %v", err)
		}
	}

	if *exit {
		logger.Println("debug-shutdown", "Performing final sync to disk...")
	}
	if err := store.Sync(); err != nil {
		logger.Error("Failed to sync: %v", err)
	}
	if *exit {
		logger.Println("debug-shutdown", "Final sync complete")
		lastWritten := nextBlock - 1
		if nextBlock > start {
			logger.Printf("shutdown", "Sync stopped at block %d (started at %d, wrote %d blocks)", lastWritten, start, lastWritten-start+1)
		} else {
			logger.Printf("shutdown", "Sync stopped at block %d (no blocks written)", start-1)
		}
	}

	if *exit {
		logger.Println("debug-shutdown", "Closing write queue...")
	}
	close(writeQueue)
	writeWg.Wait()
	if *exit {
		logger.Println("debug-shutdown", "Async writer finished")
	}

	return sliceErr
}

// strideWork represents a stride to be prefetched
type strideWork struct {
	strideNum   uint32
	strideStart uint32
	strideEnd   uint32
}

// prefetchWorker reads strides from a work channel and sends results to output channel.
// Multiple prefetch workers run in parallel, each grabbing the next available stride.
// This keeps strides roughly in order since work is assigned sequentially.
func prefetchWorker(prefetcherID uint32, traceConfig *tracereader.Config, workChan <-chan strideWork, outputChan chan<- *prefetchedStride, exit *bool, exitChan <-chan struct{}) {
	for {
		select {
		case work, ok := <-workChan:
			if !ok {
				return
			}
			if *exit {
				return
			}

			// Read stride from disk
			t0 := time.Now()
			expectedCount := int(work.strideEnd - work.strideStart + 1)
			rawBlocks, err := tracereader.GetRawBlocksWithMetadata(work.strideStart, expectedCount, traceConfig)
			readTimeNs := time.Since(t0).Nanoseconds()
			atomic.AddInt64(&totalGetBlocksNs, readTimeNs)
			atomic.AddInt64(&totalStridesCount, 1)

			// Track per-prefetcher stats
			if prefetcherID < maxPrefetchers {
				atomic.AddInt64(&prefetcherReadTimeNs[prefetcherID], readTimeNs)
				atomic.AddInt64(&prefetcherStridesRead[prefetcherID], 1)
				atomic.StoreInt64(&prefetcherLastStrideNs[prefetcherID], readTimeNs)
			}

			if *exit {
				return
			}

			// Send result
			select {
			case outputChan <- &prefetchedStride{
				strideNum:   work.strideNum,
				strideStart: work.strideStart,
				strideEnd:   work.strideEnd,
				rawBlocks:   rawBlocks,
				err:         err,
			}:
			case <-exitChan:
				return
			}

		case <-exitChan:
			return
		}
	}
}

// Worker goroutines that process pre-read blocks (pure CPU work, no I/O blocking)
// Output: processedStride containing all blocks in the stride, ready for sequential write
func gophers(config *server.Config, traceConfig *tracereader.Config, strideChan chan *processedStride, workercount uint32, prefetchercount uint32, start uint32, end uint32, exit *bool, exitChan <-chan struct{}) {
	var wg sync.WaitGroup

	stride := traceConfig.Stride
	startStride := start / stride
	endStride := end / stride

	// Work channel: generator sends stride assignments, prefetchers grab them
	// Small buffer keeps strides roughly in order (prefetcher grabs next available)
	workChan := make(chan strideWork, prefetchercount)

	// Shared prefetch channel: all prefetchers send here, all workers receive
	// Buffer allows some read-ahead without excessive memory
	sharedPrefetchChan := make(chan *prefetchedStride, prefetchercount*2)

	// Start stride generator - feeds work to prefetchers sequentially
	go func() {
		defer close(workChan)
		for strideNum := startStride; strideNum <= endStride && !(*exit); strideNum++ {
			strideStart := strideNum * stride
			strideEnd := strideStart + stride - 1
			if strideEnd > end {
				strideEnd = end
			}
			if strideStart < start {
				strideStart = start
			}

			select {
			case workChan <- strideWork{
				strideNum:   strideNum,
				strideStart: strideStart,
				strideEnd:   strideEnd,
			}:
			case <-exitChan:
				return
			}
		}
	}()

	// Start prefetch workers - they grab strides from work channel
	var prefetchWg sync.WaitGroup
	prefetchWg.Add(int(prefetchercount))
	for p := uint32(0); p < prefetchercount; p++ {
		go func(id uint32) {
			defer prefetchWg.Done()
			prefetchWorker(id, traceConfig, workChan, sharedPrefetchChan, exit, exitChan)
			if *exit {
				logger.Printf("debug-shutdown", "Prefetcher %d exiting", id)
			}
		}(p)
	}

	// Goroutine to close shared channel after all prefetchers are done
	go func() {
		prefetchWg.Wait()
		if *exit {
			logger.Println("debug-shutdown", "All prefetchers done, closing shared prefetch channel...")
		}
		close(sharedPrefetchChan)
		if *exit {
			logger.Println("debug-shutdown", "Shared prefetch channel closed")
		}
	}()

	// Workers consume from shared channel - any worker grabs any available stride
	// Each worker processes entire stride, outputs as single processedStride
	wg.Add(int(workercount))
	for i := uint32(0); i < workercount; i++ {
		go func(i uint32) {
			defer func() {
				if *exit {
					logger.Printf("debug-shutdown", "Worker %d exiting", i)
				}
				wg.Done()
			}()

			// Per-worker reusable decode buffers (eliminates allocation in hot path)
			decodeBuffers := NewWorkerBuffers()

			// Per-worker reusable slice for action sorting (GC optimization)
			type actionWithGlob struct {
				glob     uint64
				cat      *compressedActionTrace
				catBytes []byte
			}
			var actionsWithGlobs []actionWithGlob

			for prefetched := range sharedPrefetchChan {
				strideProcessStart := time.Now()

				// Check exit immediately
				if *exit {
					logger.Printf("debug-shutdown", "Worker %d stopping on exit signal...", i)
					break
				}

				// Check for I/O errors
				if prefetched.err != nil {
					logger.Fatal("Failed to read trace files for blocks %d-%d: %v (sync requires complete trace files)",
						prefetched.strideStart, prefetched.strideEnd, prefetched.err)
				}

				// Track stride being processed by this worker
				if i < maxWorkers {
					atomic.StoreUint32(&workerLastStrideNum[i], prefetched.strideNum)
					atomic.AddInt64(&workerStridesInterval[i], 1)
				}

				rawBlocks := prefetched.rawBlocks
				strideStart := prefetched.strideStart
				strideEnd := prefetched.strideEnd

				// Check for incomplete stride (missing blocks at end)
				expectedBlocks := int(strideEnd - strideStart + 1)
				actualBlocks := len(rawBlocks)

				if actualBlocks < expectedBlocks {
					missingBlocks := expectedBlocks - actualBlocks
					lastAvailableBlock := strideStart + uint32(actualBlocks) - 1
					firstMissingBlock := lastAvailableBlock + 1

					logger.Printf("warning", "⚠️  Incomplete stride %d-%d: has %d blocks, expected %d",
						strideStart, strideEnd, actualBlocks, expectedBlocks)
					logger.Printf("warning", "Missing blocks: %d-%d (%d blocks) - trace files incomplete",
						firstMissingBlock, strideEnd, missingBlocks)
					logger.Printf("warning", "Sync will fail when it reaches block %d (fail-fast to prevent gaps)", firstMissingBlock)
				}

				// Accumulate all blocks in stride for single output
				strideBlocks := make([]appendlog.BlockEntry, 0, expectedBlocks)
				var strideABIs []abiEntry
				exitRequested := false

				// Process all blocks in stride (pure CPU work)
				for idx, blockNum := 0, strideStart; blockNum <= strideEnd; blockNum++ {
					if *exit {
						exitRequested = true
						break
					}

					// Check if this block has trace data available
					if idx >= len(rawBlocks) {
						// Block is missing - append marker entry (nil data)
						strideBlocks = append(strideBlocks, appendlog.BlockEntry{
							BlockNum: blockNum,
							Data:     nil, // Marker for missing block
						})
						continue
					}

					// INSTRUMENTATION: Time block processing
					t1 := time.Now()

					rawBlock := &rawBlocks[idx]
					actions, blkData, trxIDToIndex, err := convertRawBlockToActionsBuffered(rawBlock.RawBytes, decodeBuffers)
					if err != nil {
						logger.Fatal("Failed to convert raw block %d: %v", blockNum, err)
					}
					if actions == nil {
						// Unsupported variant - append marker entry
						strideBlocks = append(strideBlocks, appendlog.BlockEntry{
							BlockNum: blockNum,
							Data:     nil,
						})
						idx++
						continue
					}

					estimatedActions := len(actions)
					blob := &blockBlob{Block: blkData}

					// Build temporary slice with glob+cat+bytes, then sort by glob
					if cap(actionsWithGlobs) < estimatedActions {
						actionsWithGlobs = make([]actionWithGlob, 0, estimatedActions)
					} else {
						actionsWithGlobs = actionsWithGlobs[:0]
					}
					decodeBuffers.ResetCompressBuffers()
					for actIdx := range actions {
						glob := actions[actIdx].GlobalSequence
						catPtr := compressActionTraceOptimized(&actions[actIdx], blob.Block, trxIDToIndex, true, decodeBuffers)
						actionsWithGlobs = append(actionsWithGlobs, actionWithGlob{
							glob:     glob,
							cat:      catPtr,
							catBytes: catPtr.Bytes(),
						})

						// Extract ABIs from setabi actions
						if abicache.IsSetabi(actions[actIdx].AccountName, actions[actIdx].ActionName) {
							contract, abiJSON, err := abicache.ParseSetabi(actions[actIdx].ActionData)
							if err != nil {
								// Early chain ABIs (eosio::abi/1.0) may fail to decode - log as debug
								logger.Printf("debug-abi", "Failed to extract ABI at block %d: %v", blockNum, err)
							} else {
								strideABIs = append(strideABIs, abiEntry{
									blockNum: blockNum,
									contract: contract,
									abiJSON:  abiJSON,
								})
							}
						}
					}

					// Sort by globalSeq
					sort.Slice(actionsWithGlobs, func(i, j int) bool {
						return actionsWithGlobs[i].glob < actionsWithGlobs[j].glob
					})

					// Build final slices in sorted order
					blob.Cats = make([]*compressedActionTrace, 0, estimatedActions)
					blob.CatsBytes = make([][]byte, 0, estimatedActions)
					blob.CatsOffset = make([]uint32, 0, estimatedActions)
					for j, awg := range actionsWithGlobs {
						blob.Cats = append(blob.Cats, awg.cat)
						blob.CatsBytes = append(blob.CatsBytes, awg.catBytes)
						blob.CatsOffset = append(blob.CatsOffset, uint32(awg.glob-uint64(blob.Block.MinGlobInBlock)-uint64(j)))
					}

					blobBytes := blob.Bytes()

					// Append to stride output
					strideBlocks = append(strideBlocks, appendlog.BlockEntry{
						BlockNum: blockNum,
						Data:     blobBytes,
						GlobMin:  blob.Block.MinGlobInBlock,
						GlobMax:  blob.Block.MaxGlobInBlock,
					})

					// Accumulate stats
					atomic.AddInt64(&statsProcessedBlocks, 1)
					if i < maxWorkers {
						atomic.AddInt64(&workerBlocksProcessed[i], 1)
					}
					atomic.AddInt64(&statsActions, int64(len(blob.Cats)))
					atomic.AddInt64(&statsTxns, int64(len(blob.Block.TrxIDInBlock)))
					atomic.AddInt64(&statsBytes, int64(len(blobBytes)))
					// Update max glob (atomic max via CAS loop)
					for {
						current := atomic.LoadUint64(&statsGlobMax)
						if blob.Block.MaxGlobInBlock <= current {
							break
						}
						if atomic.CompareAndSwapUint64(&statsGlobMax, current, blob.Block.MaxGlobInBlock) {
							break
						}
					}

					atomic.AddInt64(&totalProcessNs, time.Since(t1).Nanoseconds())
					atomic.AddInt64(&totalBlocksCount, 1)
					idx++
				}

				// Shrink buffers if they grew too large during dense blocks
				decodeBuffers.ShrinkIfNeeded()

				// Update decode buffer stats after each stride
				atomic.StoreInt64(&statsDecodeBufferBytes, decodeBuffers.CapacityBytes()*int64(workercount))

				// Track per-worker stride processing time
				if i < maxWorkers {
					atomic.AddInt64(&workerStrideTimeNs[i], time.Since(strideProcessStart).Nanoseconds())
				}

				// Send completed stride (unless exit requested)
				if !exitRequested && len(strideBlocks) > 0 {
					t2 := time.Now()
					select {
					case strideChan <- &processedStride{
						strideNum:   prefetched.strideNum,
						strideStart: strideStart,
						strideEnd:   strideEnd,
						blocks:      strideBlocks,
						abis:        strideABIs,
					}:
						atomic.AddInt64(&totalChanSendNs, time.Since(t2).Nanoseconds())
					case <-exitChan:
						return
					}
				}
			}
		}(i)
	}

	if *exit {
		logger.Println("debug-shutdown", "Waiting for all workers to finish...")
	}
	wg.Wait()
	if *exit {
		logger.Println("debug-shutdown", "All workers finished")
	}

	// Close output channel
	if *exit {
		logger.Println("debug-shutdown", "Closing stride output channel...")
	}
	close(strideChan)
	if *exit {
		logger.Println("debug-shutdown", "Stride output channel closed")
	}
}

func doHandleRPC(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	if !(*config).AcceptHTTP {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}

	inpath := r.URL.EscapedPath()
	api, apiMethod := path.Split(inpath)
	apiVersion, apiCall := path.Split(path.Clean(api))

	if (*config).PrintTiming {
		mark := time.Now()
		defer func() {
			logger.Println("http", time.Since(mark), ": "+apiMethod)
		}()
	}

	w.Header().Set("Content-Type", "application/json")

	if apiVersion == "/v1/" && apiCall == "history" {
		switch apiMethod {
		case "get_info":
			handleGetInfo(config, store, w, r)
			return
		case "get_notified_in_block":
			handleGetNotifiedInBlock(config, store, w, r)
			return
		case "get_raw_actions_in_block":
			handleGetRawActionsInBlock(config, store, w, r)
			return
		case "get_block_binary":
			handleGetBlockBinary(config, store, w, r)
			return
		case "get_actions_by_globs_binary":
			handleGetActionsByGlobsBinary(config, store, w, r)
			return
		case "get_state_binary":
			handleGetStateBinary(config, store, w, r)
			return
		case "get_blocks_batch_binary":
			handleGetBlocksBatchBinary(config, store, w, r)
			return
		}
	}

	if apiVersion == "/v1/" && apiCall == "chain" && apiMethod == "get_info" {
		handleGetInfo(config, store, w, r)
		return
	}

	if inpath == "/openapi.json" || inpath == "/openapi.yaml" {
		handleOpenAPI(w, r)
		return
	}

	http.Error(w, "Not found", http.StatusNotFound)
}

func handleGetInfo(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	lib := store.GetLIB()
	head := store.GetHead()
	cacheHits, cacheMisses := store.GetCacheStats()
	cacheSize := store.GetCacheSize()

	response := map[string]interface{}{
		"last_irreversible_block_num": lib,
		"head_block_num":              head,
		"cache": map[string]interface{}{
			"hits":   cacheHits,
			"misses": cacheMisses,
			"size":   cacheSize,
		},
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(responseBytes)
}

func handleGetNotifiedInBlock(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	var req map[string]interface{}
	err := encoding.JSONiter.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	blockNum, ok := maybeGetInt64(req["block_num"])
	if !ok {
		http.Error(w, "Missing block_num", http.StatusBadRequest)
		return
	}

	var globNotifies []map[string]interface{}
	var blockId string = ""
	var previousId string = "0000000000000000000000000000000000000000000000000000000000000000"

	if blockNum < 2 {
		http.Error(w, "block_num must be >= 2 (block 1 does not exist)", http.StatusBadRequest)
		return
	}

	blockBytes, err := store.GetBlock(uint32(blockNum))
	if err == appendlog.ErrNotFound {
		globNotifies = make([]map[string]interface{}, 0)
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		blob := bytesToBlockBlob(blockBytes)
		blockId = hex.EncodeToString(blob.Block.ProducerBlockID[:])

		globNotifies = make([]map[string]interface{}, 0)
		for i, cat := range blob.Cats {
			glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
			receiverStr := chain.NameToString(receiver)

			globNotifies = append(globNotifies, map[string]interface{}{
				"global_action_seq": glob,
				"notifies":          []string{receiverStr},
			})
		}
	}

	lib := store.GetLIB()
	head := store.GetHead()

	response := map[string]interface{}{
		"glob_notifies":           globNotifies,
		"id":                      blockId,
		"previous":                previousId,
		"last_irreversible_block": lib,
		"head_block_num":          head,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(responseBytes)
}

func handleGetRawActionsInBlock(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	var req map[string]interface{}
	err := encoding.JSONiter.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	blockNum, ok := maybeGetInt64(req["block_num"])
	if !ok {
		http.Error(w, "Missing block_num", http.StatusBadRequest)
		return
	}

	var actions []map[string]interface{}
	var blockId string = "0000000000000000000000000000000000000000000000000000000000000000"
	var previousId string = "0000000000000000000000000000000000000000000000000000000000000000"

	if blockNum < 2 {
		http.Error(w, "block_num must be >= 2 (block 1 does not exist)", http.StatusBadRequest)
		return
	}

	blockBytes, err := store.GetBlock(uint32(blockNum))
	if err == appendlog.ErrNotFound {
		actions = make([]map[string]interface{}, 0)
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		blob := bytesToBlockBlob(blockBytes)
		blockId = hex.EncodeToString(blob.Block.ProducerBlockID[:])

		actions = make([]map[string]interface{}, 0)
		for i, cat := range blob.Cats {
			glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			at := cat.At(blob.Block, blob.Block.BlockNum, glob)

			action := map[string]interface{}{
				"action_ordinal":         at.ActionOrdinal,
				"creator_action_ordinal": at.CreatorAO,
				"receiver":               at.Receiver,
				"act":                    at.Act,
				"context_free":           at.ContextFree,
				"elapsed":                at.Elapsed,
				"account_ram_deltas":     at.AccountRAMDeltas,
				"trx_id":                 at.TrxID,
				"block_num":              at.BlockNum,
				"block_time":             at.BlockTime,
				"producer_block_id":      at.ProducerBlockID,
				"receipt":                at.Receipt,
			}
			actions = append(actions, action)
		}
	}

	lib := store.GetLIB()
	head := store.GetHead()

	response := map[string]interface{}{
		"actions":                 actions,
		"id":                      blockId,
		"previous":                previousId,
		"last_irreversible_block": lib,
		"head_block_num":          head,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(responseBytes)
}

func handleGetBlockBinary(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	var req struct {
		BlockNum uint32 `json:"block_num"`
	}
	if err := encoding.JSONiter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if req.BlockNum < 2 {
		http.Error(w, "block_num must be >= 2 (block 1 does not exist)", http.StatusBadRequest)
		return
	}

	blockBytes, err := store.GetBlock(req.BlockNum)
	if err == appendlog.ErrNotFound {
		http.Error(w, "Block not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	blob := bytesToBlockBlob(blockBytes)

	resp := &corestream.GetBlockResponse{
		BlockNum: req.BlockNum,
		LIB:      store.GetLIB(),
		HEAD:     store.GetHead(),
		BlockID:  blob.Block.ProducerBlockID,
	}
	copy(resp.Data, blockBytes)
	resp.Data = blockBytes

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(corestream.EncodeGetBlockResponse(resp))
}

func handleGetActionsByGlobsBinary(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	var req struct {
		GlobalSeqs []uint64 `json:"global_seqs"`
	}
	if err := encoding.JSONiter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(req.GlobalSeqs) == 0 {
		resp := &corestream.GetActionsByGlobsResponse{
			LIB:     store.GetLIB(),
			HEAD:    store.GetHead(),
			Actions: []corestream.ActionData{},
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(corestream.EncodeGetActionsByGlobsResponse(resp))
		return
	}

	if len(req.GlobalSeqs) > 1000 {
		http.Error(w, "Too many global sequences (max 1000)", http.StatusBadRequest)
		return
	}

	type target struct {
		globalSeq uint64
		origIndex int
	}
	blockMap := make(map[uint32][]target)

	for origIdx, glob := range req.GlobalSeqs {
		blockNum, err := store.GetBlockByGlob(glob)
		if err != nil {
			http.Error(w, fmt.Sprintf("glob %d not found: %v", glob, err), http.StatusNotFound)
			return
		}
		blockMap[blockNum] = append(blockMap[blockNum], target{globalSeq: glob, origIndex: origIdx})
	}

	results := make([]corestream.ActionData, len(req.GlobalSeqs))

	for blockNum, targets := range blockMap {
		blockBytes, err := store.GetBlock(blockNum)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read block %d: %v", blockNum, err), http.StatusInternalServerError)
			return
		}

		blob := bytesToBlockBlob(blockBytes)
		blockTimeUint32 := chain.TimeToUint32(blob.Block.BlockTime)

		for i, cat := range blob.Cats {
			globSeq := cat.GlobalSequence
			for _, tgt := range targets {
				if globSeq == tgt.globalSeq {
					actionBytes := blob.CatsBytes[i]
					results[tgt.origIndex] = corestream.ActionData{
						GlobalSeq: globSeq,
						BlockNum:  blockNum,
						BlockTime: blockTimeUint32,
						Data:      actionBytes,
					}
					break
				}
			}
		}
	}

	resp := &corestream.GetActionsByGlobsResponse{
		LIB:     store.GetLIB(),
		HEAD:    store.GetHead(),
		Actions: results,
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(corestream.EncodeGetActionsByGlobsResponse(resp))
}

func handleGetStateBinary(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	resp := &corestream.GetStateResponse{
		HEAD: store.GetHead(),
		LIB:  store.GetLIB(),
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(corestream.EncodeGetStateResponse(resp))
}

func handleGetBlocksBatchBinary(config *server.Config, store appendlog.StoreInterface, w http.ResponseWriter, r *http.Request) {
	var req struct {
		StartBlock uint32 `json:"start_block"`
		EndBlock   uint32 `json:"end_block"`
	}

	if err := encoding.JSONiter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if req.StartBlock < 2 {
		http.Error(w, "start_block must be >= 2", http.StatusBadRequest)
		return
	}
	if req.EndBlock < req.StartBlock {
		http.Error(w, "end_block must be >= start_block", http.StatusBadRequest)
		return
	}

	batchSize := req.EndBlock - req.StartBlock + 1
	if batchSize > 10000 {
		http.Error(w, "Batch size too large (max 10000 blocks)", http.StatusBadRequest)
		return
	}

	head := store.GetHead()
	if req.StartBlock > head {
		http.Error(w, fmt.Sprintf("start_block %d beyond HEAD %d", req.StartBlock, head), http.StatusNotFound)
		return
	}

	if req.EndBlock > head {
		req.EndBlock = head
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		logger.Printf("http", "ERROR: ResponseWriter does not support flushing")
		return
	}

	for blockNum := req.StartBlock; blockNum <= req.EndBlock; blockNum++ {
		blockData, err := store.GetBlock(blockNum)
		if err != nil {
			logger.Printf("http", "ERROR: Batch request failed at block %d: %v", blockNum, err)
			return
		}

		if err := writeBlockChunk(w, blockNum, blockData); err != nil {
			logger.Printf("http", "ERROR: Network error writing block %d: %v", blockNum, err)
			return
		}

		flusher.Flush()
	}

	if config.Debug {
		logger.Printf("http", "Batch request completed: blocks %d-%d (%d blocks)",
			req.StartBlock, req.EndBlock, req.EndBlock-req.StartBlock+1)
	}
}

func writeBlockChunk(w http.ResponseWriter, blockNum uint32, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, blockNum); err != nil {
		return fmt.Errorf("failed to write block_num: %w", err)
	}

	dataLen := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, dataLen); err != nil {
		return fmt.Errorf("failed to write data_length: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

func main() {
	config.CheckVersion(Version)

	cfg := &Config{}
	if err := config.Load(cfg, os.Args[1:]); err != nil {
		logger.Fatal("Config error: %v", err)
	}

	// Register all log categories for column alignment
	logger.RegisterCategories(logCategories...)

	// Configure logging based on config
	if cfg.Debug {
		logger.SetMinLevel(logger.LevelDebug)
	}
	if len(cfg.LogFilter) > 0 {
		logger.SetCategoryFilter(cfg.LogFilter)
	}
	if cfg.LogFile != "" {
		if err := logger.SetLogFile(cfg.LogFile); err != nil {
			logger.Fatal("Failed to open log file %s: %v", cfg.LogFile, err)
		}
		defer logger.Close()
		logger.Printf("startup", "Logging to file: %s", cfg.LogFile)
	}

	serverConfig := &server.Config{
		Debug:          cfg.Debug,
		PrintTiming:    cfg.Timing,
		AcceptHTTP:     false,
		PrintSyncEvery: uint32(cfg.FlushInterval),
	}

	traceConfig := &tracereader.Config{
		Debug:  cfg.Debug,
		Stride: uint32(cfg.Stride),
		Dir:    cfg.TracePath,
	}

	logger.Println("startup", "coreindex "+Version+" starting...")

	if err := initOpenAPI(); err != nil {
		logger.Fatal("Failed to load OpenAPI spec: %v", err)
	}
	if err := validateOpenAPIRoutes(); err != nil {
		logger.Fatal("%v", err)
	}

	// Validate trace cleanup configuration
	if err := cfg.ValidateTraceCleanup(); err != nil {
		logger.Fatal("Invalid trace cleanup configuration: %v", err)
	}
	logger.Println("startup", "")

	// Storage configuration
	compressionStr := "disabled"
	if cfg.Zstd {
		compressionStr = fmt.Sprintf("zstd level %d", cfg.ZstdLevel)
	}
	logger.Printf("startup", "Storage: %s (slice-size: %d, compression: %s)", cfg.Path, cfg.SliceSize, compressionStr)
	logger.Printf("startup", "Trace source: %s (stride: %d)", traceConfig.Dir, cfg.Stride)
	logger.Printf("startup", "Sync: %d workers, %d prefetchers, flush every %d blocks", cfg.Workers, cfg.Prefetchers, cfg.FlushInterval)
	if cfg.StartBlock > 0 {
		logger.Printf("startup", "Start block: %d (skipping early blocks)", cfg.StartBlock)
	}
	logger.Println("startup", "")

	// Data access section - the 3 ways to read data
	logger.Println("startup", "Data access:")
	logger.Printf("startup", "  File storage:   %s", cfg.Path)

	// HTTP API addresses
	httpAddrs := []string{}
	if cfg.HTTPListen != "" {
		httpAddrs = append(httpAddrs, cfg.HTTPListen)
	}
	if cfg.HTTPSocket != "" {
		httpAddrs = append(httpAddrs, cfg.HTTPSocket)
	}
	if len(httpAddrs) > 0 {
		logger.Printf("startup", "  HTTP API:       %s", strings.Join(httpAddrs, ", "))
	} else {
		logger.Printf("startup", "  HTTP API:       disabled")
	}

	// Streaming API addresses
	if cfg.StreamEnabled {
		streamAddrs := []string{}
		if cfg.StreamListen != "" {
			streamAddrs = append(streamAddrs, cfg.StreamListen)
		}
		if cfg.StreamSocket != "" {
			streamAddrs = append(streamAddrs, cfg.StreamSocket)
		}
		if len(streamAddrs) > 0 {
			logger.Printf("startup", "  Streaming API:  %s (max %d clients)", strings.Join(streamAddrs, ", "), cfg.StreamMaxClients)
		}
	} else {
		logger.Printf("startup", "  Streaming API:  disabled")
	}

	// Optional features (only show if enabled)
	if cfg.Profile {
		logger.Println("startup", "")
		logger.Printf("startup", "Profiling: enabled (interval %ds)", cfg.ProfileInterval)
	}

	if cfg.IsTraceCleanupEnabled() {
		logger.Println("startup", "")
		if cfg.TraceCleanupMode == "delete" {
			logger.Printf("startup", "Trace cleanup: DELETE mode (delay: %d blocks)", cfg.TraceCleanupBlockDelay)
			if !cfg.TraceCleanupDryRun {
				logger.Println("startup", "  WARNING: Trace files will be permanently deleted")
			}
		} else {
			logger.Printf("startup", "Trace cleanup: MOVE to %s (delay: %d blocks)", cfg.TraceCleanupArchivePath, cfg.TraceCleanupBlockDelay)
		}
		if cfg.TraceCleanupDryRun {
			logger.Println("startup", "  (dry-run mode - no files will be modified)")
		}
	}

	logger.Println("startup", "")

	// Handle replay command (destructive operation)
	if cfg.Replay {
		logger.Println("startup", "")
		logger.Println("startup", "Replay mode:")
		logger.Println("startup", "  WARNING: Will erase all data and restart from block 2")

		// Check if database path exists
		if _, err := os.Stat(cfg.Path); err == nil {
			logger.Printf("startup", "  Removing all data from %s...", cfg.Path)
			if err := os.RemoveAll(cfg.Path); err != nil {
				logger.Fatal("Failed to remove data directory: %v", err)
			}
			logger.Println("startup", "  All data erased successfully")
		} else {
			logger.Println("startup", "  Database path does not exist - nothing to erase")
		}

		logger.Println("startup", "  Continuing with fresh sync from block 2...")
	}

	if _, err := os.Stat(traceConfig.Dir); os.IsNotExist(err) {
		logger.Fatal("Trace directory does not exist: %s", traceConfig.Dir)
	}

	// Open slice store (mandatory)
	opts := appendlog.SliceStoreOptions{
		Debug:            cfg.Debug,
		BlocksPerSlice:   uint32(cfg.SliceSize),
		MaxCachedSlices:  cfg.MaxCachedSlices,
		BlockCacheSize:   500,
		EnableZstd:       cfg.Zstd,
		ZstdLevel:        cfg.ZstdLevel,
		LogSliceInterval: uint32(cfg.LogSliceInterval),
	}
	store, err := appendlog.NewSliceStore(cfg.Path, opts)
	if err != nil {
		logger.Fatal("Failed to open store: %v", err)
	}
	defer func() {
		logger.Println("debug-shutdown", "Closing store...")
		store.Close()
		logger.Println("debug-shutdown", "Closed safely")
	}()
	logger.Println("debug-shutdown", "Opened store")

	// Create/update slice metadata cache for fast reader startup
	if err := saveSliceCache(store); err != nil {
		logger.Printf("warning", "Failed to save slice cache on startup: %v", err)
	} else if len(store.GetSliceInfos()) > 0 {
		logger.Printf("startup", "Slice cache saved (%d slices)", len(store.GetSliceInfos()))
	}

	// Open ABI cache writer
	abiPath := cfg.ABIPath
	if abiPath == "" {
		abiPath = filepath.Join(cfg.Path, "abis")
	}
	abiWriter, err := abicache.NewWriter(abiPath)
	if err != nil {
		logger.Fatal("Failed to open ABI cache: %v", err)
	}
	defer func() {
		logger.Println("debug-shutdown", "Closing ABI cache...")
		abiWriter.Close()
		logger.Println("debug-shutdown", "ABI cache closed")
	}()
	logger.Printf("startup", "ABI cache: %s", abiPath)

	// Handle --repair-slice N BEFORE validation: repair specific slice and exit
	if cfg.RepairSlice >= 0 {
		sliceNum := uint32(cfg.RepairSlice)
		logger.Println("startup", "")
		logger.Printf("startup", "Repair slice %d:", sliceNum)

		// Verify slice exists
		sliceInfos := store.GetSliceInfos()
		found := false
		for _, info := range sliceInfos {
			if info.SliceNum == sliceNum {
				found = true
				break
			}
		}
		if !found {
			logger.Fatal("Slice %d not found", sliceNum)
		}

		// Get slice path
		slicePath := filepath.Join(cfg.Path, fmt.Sprintf("history_%010d-%010d",
			sliceNum*uint32(cfg.SliceSize)+1, (sliceNum+1)*uint32(cfg.SliceSize)))

		logger.Printf("startup", "  Repairing slice %d from data.log...", sliceNum)
		startTime := time.Now()

		blockCount, globCount, err := appendlog.RepairSliceIndexes(slicePath)
		if err != nil {
			logger.Fatal("Repair failed: %v", err)
		}

		elapsed := time.Since(startTime)
		logger.Printf("startup", "  ✓ Slice %d repaired: %d blocks, %d globs recovered in %.2fs",
			sliceNum, blockCount, globCount, elapsed.Seconds())
		os.Exit(0)
	}

	// Handle --repair-all BEFORE validation: repair all invalid slices and exit
	if cfg.RepairAll {
		logger.Println("startup", "")
		logger.Println("startup", "Repair all invalid slices:")
		logger.Printf("startup", "  Scanning all slices and repairing any with invalid indexes...")
		logger.Printf("startup", "  Using %d parallel workers", cfg.Workers)
		startTime := time.Now()

		// Use BuildGlobIndexParallelWithRepair with repair=true
		globCount, err := store.BuildGlobIndexParallelWithRepair(cfg.Workers, true)
		if err != nil {
			logger.Fatal("Repair all failed: %v", err)
		}

		elapsed := time.Since(startTime)
		logger.Printf("startup", "  ✓ Repair complete: %d glob entries in %.2fs", globCount, elapsed.Seconds())
		os.Exit(0)
	}

	// Glob indices are now built incrementally during AppendBlock()
	// No background builder needed - indices are always current

	// Start periodic profiling and enable pprof endpoints if enabled
	if cfg.Profile {
		// Enable block profiling for pprof endpoint
		profiler.EnableBlockProfiling()
		logger.Println("debug-profiling", "Block profiling enabled (access via /debug/pprof/block endpoint)")

		// Start periodic profiling
		profiler.Start(profiler.Config{
			ServiceName: "coreindex",
			Interval:    time.Duration(cfg.ProfileInterval) * time.Second,
		})
	}

	// Smart validation: quick by default, full on crash or --verify flag
	logger.Println("startup", "")
	logger.Println("startup", "Validation:")
	if cfg.Verify {
		logger.Println("startup", "  Full validation requested (--verify flag)")
	}

	if err := store.StartupValidation(cfg.Verify, cfg.Workers); err != nil {
		logger.Fatal("Validation failed: %v", err)
	}

	// Exit after validation if --verify flag was used
	if cfg.Verify {
		logger.Println("validation", "✓ Validation complete (--verify)")
		logger.Println("validation", "If invalid slices were found, use --repair-slice N or --repair-all to fix them")
		logger.Println("validation", "Exiting (--verify does not sync blocks)")
		os.Exit(0)
	}

	// Handle --verify-only mode: validate all slices and exit
	if cfg.VerifyOnly {
		logger.Println("startup", "")
		logger.Println("startup", "Full slice verification mode:")

		sliceInfos := store.GetSliceInfos()
		if len(sliceInfos) == 0 {
			logger.Println("startup", "  No slices to verify")
			os.Exit(0)
		}

		sliceNums := make([]uint32, len(sliceInfos))
		for i, info := range sliceInfos {
			sliceNums[i] = info.SliceNum
		}

		logger.Printf("startup", "  Verifying %d slices with %d workers...", len(sliceNums), cfg.Workers)
		startTime := time.Now()

		results := store.ValidateSlices(sliceNums, cfg.Workers)

		validCount := 0
		invalidCount := 0
		var invalidSlices []uint32
		for _, result := range results {
			if result.Valid {
				validCount++
			} else {
				invalidCount++
				invalidSlices = append(invalidSlices, result.SliceNum)
				logger.Warning("✗ Slice %d invalid: %v", result.SliceNum, result.Error)
			}
		}

		elapsed := time.Since(startTime)
		logger.Printf("startup", "  Verification complete: %d valid, %d invalid in %.2fs",
			validCount, invalidCount, elapsed.Seconds())

		if invalidCount > 0 {
			if cfg.Repair {
				logger.Printf("startup", "  Repair mode: removing %d invalid slices...", invalidCount)
				for _, sliceNum := range invalidSlices {
					if err := store.RemoveSlice(sliceNum); err != nil {
						logger.Error("Failed to remove slice %d: %v", sliceNum, err)
					}
				}
				if len(invalidSlices) > 0 {
					lastValidSlice := uint32(0)
					for _, info := range sliceInfos {
						isInvalid := false
						for _, inv := range invalidSlices {
							if info.SliceNum == inv {
								isInvalid = true
								break
							}
						}
						if !isInvalid && info.SliceNum > lastValidSlice {
							lastValidSlice = info.SliceNum
						}
					}
					if err := store.AdjustLIBToSlice(lastValidSlice); err != nil {
						logger.Fatal("Failed to adjust LIB: %v", err)
					}
				}
				logger.Printf("startup", "  ✓ Repair complete, LIB=%d", store.GetLIB())
				os.Exit(0)
			} else {
				logger.Println("startup", "  Run with --repair flag to remove invalid slices")
				os.Exit(1)
			}
		}

		logger.Println("startup", "  ✓ All slices valid")
		os.Exit(0)
	}

	// Set up signal handler and cancellation BEFORE downloads and trace sync
	exit := false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Ignore SIGPIPE to prevent death when stdout pipe breaks (e.g., when using | tee)
	signal.Ignore(syscall.SIGPIPE)

	// Signal handler: first Ctrl+C = graceful, second = immediate exit
	shutdownRequests := 0
	go func() {
		for sig := range signalChan {
			shutdownRequests++
			if shutdownRequests == 1 {
				fmt.Fprintln(os.Stderr, "")
				fmt.Fprintf(os.Stderr, "%s shutdown    Received %v signal, shutting down gracefully...\n", time.Now().Format("2006-01-02 15:04:05"), sig)
				cancel() // Cancel context for downloads
				exit = true
			} else {
				fmt.Fprintf(os.Stderr, "%s shutdown    Received second interrupt - forcing immediate exit\n", time.Now().Format("2006-01-02 15:04:05"))
				os.Exit(1)
			}
			_ = sig
		}
	}()

	// Optional: Download pre-built slices before trace processing
	if cfg.SliceDownloadURL != "" {
		logger.Println("startup", "")
		logger.Println("startup", "Slice download:")
		logger.Printf("startup", "  Source: %s", cfg.SliceDownloadURL)
		logger.Printf("startup", "  Parallel downloads: %d, timeout: %d min", cfg.SliceDownloadParallel, cfg.SliceDownloadTimeout)

		downloader := appendlog.NewSliceDownloader(
			cfg.SliceDownloadURL,
			cfg.Path,
			cfg.SliceDownloadParallel,
			uint32(cfg.SliceSize),
			cfg.SliceDownloadTimeout,
			store,
		)

		var startBlock uint32
		currentLIB := store.GetLIB()
		if currentLIB > 0 {
			startBlock = currentLIB + 1
		} else if cfg.StartBlock > 0 {
			startBlock = uint32(cfg.StartBlock)
		} else {
			startBlock = 1
		}

		startSlice := downloader.SliceNumForBlock(startBlock)
		logger.Printf("startup", "  Resuming from block %d (slice %d)", startBlock, startSlice)

		if err := downloader.DownloadSlices(ctx, startSlice); err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Println("startup", "  Download cancelled by user")
			} else {
				logger.Warning("Slice download encountered errors: %v", err)
			}
		}

		logger.Printf("startup", "  Download complete, LIB is now %d", store.GetLIB())
	}

	// Check store state
	myLIB := store.GetLIB()
	myHead := store.GetHead()

	// Get trace file info
	info, err := tracereader.GetInfo(traceConfig)
	if err != nil {
		logger.Fatal("Failed to read trace info: %v", err)
	}
	logger.Printf("startup", "Trace files contain blocks up to LIB: %d, HEAD: %d", info.Lib, info.Head)

	if myLIB == 0 {
		startBlock := uint32(cfg.StartBlock)
		if startBlock > 0 {
			logger.Printf("sync", "Fresh start from block %d", startBlock)
			myLIB = startBlock - 1
			myHead = startBlock - 1
		} else {
			logger.Println("sync", "Fresh start from block 2 (trace files start at block 2)")
			myLIB = 1
			myHead = 1
		}
	} else {
		logger.Printf("sync", "Resuming from block %d (LIB: %d, HEAD: %d)", myLIB+1, myLIB, myHead)
	}

	// Start HTTP API servers (allows queries during sync)
	// Use "none" or empty string to disable a listener
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		doHandleRPC(serverConfig, store, w, r)
	})

	httpListeners := []string{}
	if cfg.HTTPListen != "" && cfg.HTTPListen != "none" {
		listener := server.SocketListen(cfg.HTTPListen)
		httpListeners = append(httpListeners, cfg.HTTPListen)
		go func() {
			if err := http.Serve(listener, nil); err != nil {
				logger.Printf("http", "TCP server error: %v", err)
			}
		}()
	}
	if cfg.HTTPSocket != "" && cfg.HTTPSocket != "none" {
		listener := server.SocketListen(cfg.HTTPSocket)
		httpListeners = append(httpListeners, cfg.HTTPSocket)
		go func() {
			if err := http.Serve(listener, nil); err != nil {
				logger.Printf("http", "Socket server error: %v", err)
			}
		}()
	}

	if len(httpListeners) > 0 {
		serverConfig.AcceptHTTP = true
		logger.Printf("http", "HTTP API ready on %s", strings.Join(httpListeners, ", "))
	}

	if cfg.MetricsListen != "none" && cfg.MetricsListen != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsListener := server.SocketListen(cfg.MetricsListen)
		go func() {
			if err := http.Serve(metricsListener, metricsMux); err != nil {
				logger.Printf("http", "metrics server failed: %v", err)
			}
		}()
		logger.Printf("http", "Metrics server listening on %s", cfg.MetricsListen)
	}

	if cfg.Profile {
		logger.Println("http", "Profiling: /debug/pprof/profile, /debug/pprof/heap, /debug/pprof/goroutine")
	}

	// Start streaming API servers if enabled
	// Use "none" or empty string to disable a listener
	var streamServer *corestream.Server
	if cfg.StreamEnabled {
		streamConfig := corestream.ServerConfig{
			MaxClients:        cfg.StreamMaxClients,
			HeartbeatInterval: 5 * time.Second,
			Debug:             cfg.Debug,
		}
		streamServer = corestream.NewServer(&streamBlockProvider{store: store}, streamConfig)

		streamListeners := []string{}
		if cfg.StreamListen != "" && cfg.StreamListen != "none" {
			if err := streamServer.Listen(cfg.StreamListen); err != nil {
				logger.Fatal("Failed to start stream server on %s: %v", cfg.StreamListen, err)
			}
			streamListeners = append(streamListeners, cfg.StreamListen)
		}
		if cfg.StreamSocket != "" && cfg.StreamSocket != "none" {
			if err := streamServer.Listen(cfg.StreamSocket); err != nil {
				logger.Fatal("Failed to start stream server on %s: %v", cfg.StreamSocket, err)
			}
			streamListeners = append(streamListeners, cfg.StreamSocket)
		}

		if len(streamListeners) > 0 {
			logger.Printf("stream", "Streaming API ready on %s (max %d clients)", strings.Join(streamListeners, ", "), cfg.StreamMaxClients)
		}

		// Start periodic stats logging for stream clients (every 30 seconds)
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for !exit {
				<-ticker.C
				if !exit {
					streamServer.LogStats()
				}
			}
		}()

		defer func() {
			logger.Println("debug-shutdown", "Closing stream server...")
			streamServer.Close()
			logger.Println("debug-shutdown", "Stream server closed")
		}()
	}

	// Continuous sync loop - similar to historydata's pattern
	// Check for new blocks every 500ms (EOS block interval)
	syncWaitStart := time.Now()
	liveSyncAnnounced := false // Track if we've announced live sync mode
	monitoringModeAnnounced := false        // Track if we've announced monitoring mode
	lastMonitoringLog := time.Time{}        // Track last monitoring heartbeat
	lastStoreAheadLog := time.Time{}        // Track last "store ahead" warning
	lastMediumBatchLog := time.Time{}       // Track last medium batch log (rate limit)
	// Periodic slice cache save (every 60 seconds if slice count changed)
	lastSavedSliceCount := len(store.GetSliceInfos())
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for !exit {
			<-ticker.C
			if exit {
				return
			}
			currentCount := len(store.GetSliceInfos())
			if currentCount > lastSavedSliceCount {
				if err := saveSliceCache(store); err != nil {
					logger.Printf("warning", "Failed to save slice cache: %v", err)
				} else {
					lastSavedSliceCount = currentCount
				}
			}
		}
	}()

	for !exit {
		// Sleep to maintain ~500ms loop interval (EOS block time)
		// Add 50ms buffer to ensure GetInfo cache expires (cache TTL is 500ms)
		// Always sleep at least 550ms to ensure cache expiry, even after long syncs
		syncWaitElapsed := time.Since(syncWaitStart)
		minSleep := 550 * time.Millisecond // 500ms + 50ms buffer for cache expiry
		if syncWaitElapsed < minSleep {
			time.Sleep(minSleep - syncWaitElapsed)
		} else {
			// After a long sync, sleep the minimum to ensure cache refresh
			time.Sleep(minSleep)
		}
		syncWaitStart = time.Now()

		// Check for new blocks in trace files
		info, err = tracereader.GetInfo(traceConfig)

		if err != nil {
			logger.Warning("Failed to read trace info: %v (will retry)", err)
			continue
		}

		if cfg.Debug {
			logger.Printf("debug", "Loop iteration: trace LIB=%d HEAD=%d, store LIB=%d HEAD=%d",
				info.Lib, info.Head, myLIB, myHead)
		}

		// Determine sync target based on nodeos state
		var syncTarget uint32

		if info.Lib == 0 && info.Head > 0 {
			continue
		} else if info.Lib > 0 {
			syncTarget = info.Lib
		} else {
			// LIB=0 and HEAD=0 - no blocks available yet
			if myLIB == 0 {
				logger.Printf("sync", "Waiting for nodeos to produce blocks (LIB=0, HEAD=0)...")
			}
			continue
		}

		// Catch up if behind
		if syncTarget > myLIB && !exit {
			blocksToSync := syncTarget - myLIB

			if streamServer != nil && streamServer.IsLiveMode() && !liveSyncAnnounced {
				streamServer.SetLiveMode(false)
			}

			// Announce live sync mode when we first get to small batches
			if !liveSyncAnnounced && blocksToSync < 100 && myLIB > 1000 {
				logger.Printf("sync", "Live sync mode: catching up %d blocks (per-block logging enabled)", blocksToSync)
				liveSyncAnnounced = true
			}

			// Determine if we should log this sync (reduce spam during live mode)
			// Log if: large batch (>500 blocks) OR initial catch-up (first 10K blocks)
			shouldLogSync := blocksToSync > 500 || myLIB < 10000

			// For medium batches (100-500 blocks), log periodically (every 5 seconds) with cumulative stats
			if !shouldLogSync && blocksToSync >= 100 && time.Since(lastMediumBatchLog) >= 5*time.Second {
				// Read current cumulative stats
				actions := atomic.LoadInt64(&statsActions)
				txns := atomic.LoadInt64(&statsTxns)
				bytes := atomic.LoadInt64(&statsBytes)
				globMax := atomic.LoadUint64(&statsGlobMax)

				// Format total bytes nicely
				var bytesStr string
				if bytes >= 1024*1024*1024 {
					bytesStr = fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
				} else if bytes >= 1024*1024 {
					bytesStr = fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
				} else if bytes >= 1024 {
					bytesStr = fmt.Sprintf("%.1f KB", float64(bytes)/1024)
				} else {
					bytesStr = fmt.Sprintf("%d B", bytes)
				}

				logger.Printf("sync", "Syncing blocks from trace files: %d to %d (%d blocks behind) | %s total, %d acts, %d txns | glob: %d",
					myLIB+1, syncTarget, blocksToSync, bytesStr, actions, txns, globMax)
				lastMediumBatchLog = time.Now()
			} else if shouldLogSync {
				logger.Printf("sync", "Syncing blocks from trace files: %d to %d (%d blocks behind)", myLIB+1, syncTarget, blocksToSync)
			}

			// Create broadcast callback if streaming is enabled
			var broadcastFn BroadcastFunc
			if streamServer != nil {
				broadcastFn = streamServer.Broadcast
			}
			syncErr := syncFromTraceFiles(serverConfig, traceConfig, uint32(cfg.Workers), uint32(cfg.Prefetchers), store, abiWriter, myLIB+1, syncTarget, &exit, ctx.Done(), broadcastFn, cfg.GetLogInterval())

			// Handle slice errors with reactive validation
			if syncErr != nil && !exit {
				logger.Warning("Sync error detected: %v", syncErr)
				logger.Println("validation", "Running reactive validation to check/repair slices...")
				if repairErr := store.ValidateAndRepairLastSlices(2); repairErr != nil {
					logger.Error("Reactive validation failed: %v", repairErr)
					logger.Fatal("Cannot continue after slice validation failure")
				}
				logger.Println("validation", "Reactive validation complete, will restart sync from repaired position")
			}

			if exit {
				logger.Println("debug-shutdown", "Sync interrupted by exit signal")
			}

			// Update our position after sync
			myLIB = store.GetLIB()
			myHead = store.GetHead()

			// If we're now caught up, enable streaming immediately
			if myLIB >= syncTarget && streamServer != nil && !streamServer.IsLiveMode() {
				streamServer.SetLiveMode(true)
			}
		} else if syncTarget == myLIB {
			// Caught up - enable streaming and announce monitoring mode
			if streamServer != nil && !streamServer.IsLiveMode() {
				streamServer.SetLiveMode(true)
			}

			if !monitoringModeAnnounced {
				logger.Printf("sync", "Caught up at block %d, monitoring for new irreversible blocks (trace LIB=%d, HEAD=%d)...", myLIB, info.Lib, info.Head)
				monitoringModeAnnounced = true
				lastMonitoringLog = time.Now()
			} else if time.Since(lastMonitoringLog) >= 30*time.Second {
				// Heartbeat every 30 seconds to show we're still alive
				// Show gap between HEAD and LIB to help diagnose sync issues
				gap := int64(info.Head) - int64(info.Lib)
				if gap > 1000 {
					logger.Printf("sync", "Monitoring: at LIB %d (HEAD=%d, gap=%d blocks) - waiting for finality", myLIB, info.Head, gap)
				} else {
					logger.Printf("sync", "Monitoring: at LIB %d (HEAD=%d, %d blocks ahead)", myLIB, info.Head, gap)
				}
				lastMonitoringLog = time.Now()
			}
		} else if syncTarget < myLIB {
			// Store is ahead of trace files - happens during nodeos replay/restart
			// Log periodically (every 10 seconds) to avoid spam
			if time.Since(lastStoreAheadLog) >= 10*time.Second {
				gap := myLIB - info.Lib
				logger.Printf("sync", "Waiting for trace files to catch up: store LIB=%d, trace LIB=%d (gap=%d blocks)", myLIB, info.Lib, gap)
				lastStoreAheadLog = time.Now()
			}
		}
	}

	// Save slice cache on shutdown
	sliceCount := len(store.GetSliceInfos())
	if err := saveSliceCache(store); err != nil {
		logger.Printf("warning", "Failed to save slice cache on shutdown: %v", err)
	} else if sliceCount > 0 {
		logger.Printf("shutdown", "Slice cache saved (%d slices)", sliceCount)
	}

	logger.Println("shutdown", "Clean shutdown complete")
}
