package corereader

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type SyncConfig struct {
	Workers       int
	BulkThreshold uint32
	LogInterval   time.Duration
	ActionFilter  ActionFilterFunc
	Debug         bool
}

type SyncProgress struct {
	CurrentBlock  uint32
	EndBlock      uint32
	BlocksPerSec  float64
	ActionsPerSec float64
	DBSizeMB      int64
	PendingSlices int
	Mode          string
}

type ProgressCallback func(progress SyncProgress)

type DBSizeFunc func() int64

// BulkSyncTiming tracks detailed timing metrics during bulk sync operations.
// This is optional - set via Syncer.SetBulkSyncTiming to enable timing collection.
type BulkSyncTiming struct {
	// Slice extraction breakdown
	SliceGetNs     atomic.Int64 // I/O time (GetSliceBatch)
	SliceExtractNs atomic.Int64 // CPU time (ExtractTransactionIDsOnly / FilterBlocksParallel)
	SliceCount     atomic.Int64
	BlockCount     atomic.Int64
	TrxCount       atomic.Int64

	// Processor time
	ProcessBatchNs atomic.Int64
	CommitNs       atomic.Int64

	// Wait times
	WorkerWaitNs atomic.Int64 // Time workers wait for work
	ResultWaitNs atomic.Int64 // Time main loop waits for results

	// State tracking
	PendingSlices atomic.Int32
	NextSlice     atomic.Uint32
	EndSlice      atomic.Uint32

	// Reporter control
	stopChan chan struct{}
}

// NewBulkSyncTiming creates a new timing tracker for bulk sync operations.
func NewBulkSyncTiming() *BulkSyncTiming {
	return &BulkSyncTiming{
		stopChan: make(chan struct{}),
	}
}

// Reset clears all timing counters.
func (t *BulkSyncTiming) Reset() {
	t.SliceGetNs.Store(0)
	t.SliceExtractNs.Store(0)
	t.SliceCount.Store(0)
	t.BlockCount.Store(0)
	t.TrxCount.Store(0)
	t.ProcessBatchNs.Store(0)
	t.CommitNs.Store(0)
	t.WorkerWaitNs.Store(0)
	t.ResultWaitNs.Store(0)
}

// StartReporter starts a background goroutine that logs timing stats at the given interval.
func (t *BulkSyncTiming) StartReporter(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.PrintStats()
				t.Reset()
			case <-t.stopChan:
				return
			}
		}
	}()
}

// Stop stops the timing reporter.
func (t *BulkSyncTiming) Stop() {
	select {
	case <-t.stopChan:
	default:
		close(t.stopChan)
	}
}

// PrintStats logs timing breakdown to the debug-timing category.
func (t *BulkSyncTiming) PrintStats() {
	slices := t.SliceCount.Load()
	if slices == 0 {
		return
	}

	blocks := t.BlockCount.Load()
	trxs := t.TrxCount.Load()

	sliceGetNs := t.SliceGetNs.Load()
	sliceExtractNs := t.SliceExtractNs.Load()
	processBatchNs := t.ProcessBatchNs.Load()
	commitNs := t.CommitNs.Load()
	workerWaitNs := t.WorkerWaitNs.Load()
	resultWaitNs := t.ResultWaitNs.Load()

	totalNs := sliceGetNs + sliceExtractNs + processBatchNs + commitNs + workerWaitNs + resultWaitNs
	if totalNs == 0 {
		return
	}

	pctSliceGet := float64(sliceGetNs) / float64(totalNs) * 100
	pctSliceExtract := float64(sliceExtractNs) / float64(totalNs) * 100
	pctProcessBatch := float64(processBatchNs) / float64(totalNs) * 100
	pctCommit := float64(commitNs) / float64(totalNs) * 100
	pctWorkerWait := float64(workerWaitNs) / float64(totalNs) * 100
	pctResultWait := float64(resultWaitNs) / float64(totalNs) * 100

	avgSliceGetMs := float64(sliceGetNs) / float64(slices) / 1e6
	avgSliceExtractMs := float64(sliceExtractNs) / float64(slices) / 1e6

	var trxPerBlock float64
	if blocks > 0 {
		trxPerBlock = float64(trxs) / float64(blocks)
	}

	pending := t.PendingSlices.Load()
	nextSlice := t.NextSlice.Load()
	endSlice := t.EndSlice.Load()

	logger.Printf("debug-timing",
		"SliceGet=%.1f%% (%.1fms) | SliceExtract=%.1f%% (%.1fms) | %.1f trx/blk",
		pctSliceGet, avgSliceGetMs,
		pctSliceExtract, avgSliceExtractMs,
		trxPerBlock)

	logger.Printf("debug-timing",
		"ProcessBatch=%.1f%% | Commit=%.1f%% | WorkerWait=%.1f%% | ResultWait=%.1f%%",
		pctProcessBatch, pctCommit, pctWorkerWait, pctResultWait)

	logger.Printf("debug-timing",
		"Slices: pending=%d next=%d end=%d | Blocks=%d Trx=%d",
		pending, nextSlice, endSlice, blocks, trxs)
}

type Syncer struct {
	baseReader     BaseReader
	reader         Reader
	config         SyncConfig
	stopChan       chan struct{}
	running        atomic.Bool
	currentBlock   uint32
	mu             sync.Mutex
	progressCb     ProgressCallback
	dbSizeFunc     DBSizeFunc
	bulkSyncTiming *BulkSyncTiming
}

func NewSyncer(reader interface{}, config SyncConfig) *Syncer {
	s := &Syncer{
		config:   config,
		stopChan: make(chan struct{}),
	}

	if r, ok := reader.(Reader); ok {
		s.reader = r
		if unwrapper, ok := r.(interface{ GetBaseReader() BaseReader }); ok {
			s.baseReader = unwrapper.GetBaseReader()
		}
	} else if br, ok := reader.(BaseReader); ok {
		s.baseReader = br
	}

	return s
}

func (s *Syncer) SetProgressCallback(cb ProgressCallback) {
	s.progressCb = cb
}

func (s *Syncer) SetDBSizeFunc(fn DBSizeFunc) {
	s.dbSizeFunc = fn
}

// SetBulkSyncTiming sets an optional timing tracker for bulk sync operations.
// When set, the syncer will record detailed timing metrics during bulk sync.
func (s *Syncer) SetBulkSyncTiming(timing *BulkSyncTiming) {
	s.bulkSyncTiming = timing
}

func (s *Syncer) SyncActions(processor Processor, startBlock uint32) error {
	if s.running.Swap(true) {
		return fmt.Errorf("syncer already running")
	}
	defer s.running.Store(false)

	s.currentBlock = startBlock

	var sliceReader *SliceReader
	if s.baseReader != nil {
		sliceReader, _ = s.baseReader.(*SliceReader)
	}

	lastLiveLogTime := time.Now()
	lastLiveLogBlock := startBlock
	liveActionCount := 0
	liveNotifCount := 0

	var initialLib uint32
	if s.reader != nil {
		_, initialLib, _ = s.reader.GetStateProps(true)
	} else {
		_, initialLib, _ = s.baseReader.GetStateProps(true)
	}
	initialBulkMode := s.currentBlock+s.config.BulkThreshold < initialLib
	if !initialBulkMode {
		if err := processor.Flush(); err != nil {
			return fmt.Errorf("initial flush: %w", err)
		}
		logger.Printf("sync", "Already caught up at startup (block %d, LIB %d) - transitioned to live mode", s.currentBlock, initialLib)
	}

	for {
		select {
		case <-s.stopChan:
			return nil
		default:
		}

		var lib uint32
		var err error
		if s.reader != nil {
			_, lib, err = s.reader.GetStateProps(true)
		} else {
			_, lib, err = s.baseReader.GetStateProps(true)
		}
		if err != nil {
			return fmt.Errorf("getting state props: %w", err)
		}

		bulkMode := s.currentBlock+s.config.BulkThreshold < lib

		if bulkMode {
			lastLiveLogTime = time.Now()
			lastLiveLogBlock = s.currentBlock
			liveActionCount = 0
			liveNotifCount = 0

			if sliceReader == nil {
				return fmt.Errorf("bulk sync requires SliceReader (local slice access)")
			}

			batchProc, ok := processor.(BatchProcessor)
			if !ok {
				return fmt.Errorf("processor must implement BatchProcessor for bulk sync")
			}

			endBlock, err := s.bulkSyncParallel(sliceReader, lib, batchProc)
			if err != nil {
				return fmt.Errorf("bulk sync: %w", err)
			}
			s.currentBlock = endBlock + 1
			continue
		}

		if s.currentBlock > lib {
			if sr, ok := s.reader.(StreamingReader); !ok || !sr.IsStreaming() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		var notifs []RawBlock
		if s.reader != nil {
			notifs, err = s.reader.GetNextBatch(1, s.config.ActionFilter)
		} else {
			notifs, err = s.baseReader.GetRawBlockBatchFiltered(s.currentBlock, s.currentBlock, s.config.ActionFilter)
		}
		if err != nil {
			return fmt.Errorf("getting block %d: %w", s.currentBlock, err)
		}

		if len(notifs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		block := filterRawBlock(notifs[0], s.config.ActionFilter)
		if err := processor.ProcessBlock(block); err != nil {
			return fmt.Errorf("processing block %d: %w", s.currentBlock, err)
		}

		liveActionCount += len(block.Actions)
		liveNotifCount += len(block.Actions)

		if processor.ShouldCommit(1) {

			if err := processor.Commit(s.currentBlock, false); err != nil {
				return fmt.Errorf("committing block %d: %w", s.currentBlock, err)
			}
		}

		s.logLiveProgress(&lastLiveLogTime, &lastLiveLogBlock, &liveActionCount, &liveNotifCount, lib)

		s.currentBlock++
	}
}

func (s *Syncer) Stop() {
	if s.running.Load() {
		close(s.stopChan)
	}
}

func (s *Syncer) CurrentBlock() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentBlock
}

func (s *Syncer) validateSliceCoverage(sr *SliceReader, startBlock uint32) error {
	sliceInfos := sr.GetSliceInfos()
	if len(sliceInfos) == 0 {
		return fmt.Errorf("no slices available in storage")
	}

	minBlock := sliceInfos[0].StartBlock
	if startBlock < minBlock {
		return fmt.Errorf("start block %d is before first available slice (starts at %d)", startBlock, minBlock)
	}

	for i := 1; i < len(sliceInfos); i++ {
		prev := sliceInfos[i-1]
		curr := sliceInfos[i]
		expectedStart := prev.EndBlock + 1
		if curr.StartBlock > expectedStart {
			if startBlock < curr.StartBlock && startBlock > prev.EndBlock {
				return fmt.Errorf("gap detected: missing blocks %d-%d (start block %d falls in gap)",
					expectedStart, curr.StartBlock-1, startBlock)
			}
		}
	}

	return nil
}

// actionSliceWork represents a slice extraction work item and its results for action syncing.
// Workers receive slice ranges, perform I/O and filtering, and return filtered blocks.
type actionSliceWork struct {
	sliceNum   uint32
	sliceStart uint32
	sliceEnd   uint32
	blocks     []Block
	err        error
}

func (s *Syncer) bulkSyncParallel(sr *SliceReader, endBlock uint32, processor BatchProcessor) (uint32, error) {
	workers := s.config.Workers
	if workers < 1 {
		workers = 4
	}

	timing := s.bulkSyncTiming

	logger.Printf("sync", "bulkSyncParallel: startBlock=%d, endBlock=%d, workers=%d",
		s.currentBlock, endBlock, workers)

	const sliceSize = uint32(10000)
	startSlice := (s.currentBlock - 1) / sliceSize
	endSlice := (endBlock - 1) / sliceSize

	if timing != nil {
		timing.EndSlice.Store(endSlice)
	}

	workChan := make(chan *actionSliceWork, workers*2)
	resultChan := make(chan *actionSliceWork, workers*2)

	var wg sync.WaitGroup
	exit := false

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				workerWaitStart := time.Now()
				work, ok := <-workChan
				if !ok {
					break
				}
				if timing != nil {
					timing.WorkerWaitNs.Add(time.Since(workerWaitStart).Nanoseconds())
				}
				if exit {
					resultChan <- work
					continue
				}
				work.blocks, work.err = s.extractActionSlice(sr, work.sliceStart, work.sliceEnd, workers, timing)
				resultChan <- work
			}
		}()
	}

	go func() {
		for sliceNum := startSlice; sliceNum <= endSlice && !exit; sliceNum++ {
			sliceStart := sliceNum*sliceSize + 1
			sliceEnd := sliceStart + sliceSize - 1

			if sliceEnd > endBlock {
				sliceEnd = endBlock
			}
			if sliceStart < s.currentBlock {
				sliceStart = s.currentBlock
			}
			if sliceStart > endBlock {
				break
			}

			workChan <- &actionSliceWork{
				sliceNum:   sliceNum,
				sliceStart: sliceStart,
				sliceEnd:   sliceEnd,
			}
		}
		close(workChan)
		wg.Wait()
		close(resultChan)
	}()

	pending := make(map[uint32]*actionSliceWork)
	nextSlice := startSlice

	lastLogTime := time.Now()
	lastLogBlock := s.currentBlock
	blocksProcessed := 0
	actionCount := 0

	for {
		resultWaitStart := time.Now()
		work, ok := <-resultChan
		if !ok {
			break
		}
		if timing != nil {
			timing.ResultWaitNs.Add(time.Since(resultWaitStart).Nanoseconds())
		}

		select {
		case <-s.stopChan:
			exit = true
			for range resultChan {
			}
			return s.currentBlock - 1, nil
		default:
		}

		if exit {
			continue
		}

		if work.err != nil {
			logger.Printf("sync", "bulkSyncParallel: error at slice %d: %v", work.sliceNum, work.err)
			continue
		}

		pending[work.sliceNum] = work

		if timing != nil {
			timing.PendingSlices.Store(int32(len(pending)))
			timing.NextSlice.Store(nextSlice)
		}

		for {
			ready, ok := pending[nextSlice]
			if !ok {
				break
			}

			sliceActionCount := 0
			for _, block := range ready.blocks {
				sliceActionCount += len(block.Actions)
			}
			actionCount += sliceActionCount

			processBatchStart := time.Now()
			if err := processor.ProcessBatch(ready.blocks); err != nil {
				exit = true
				return s.currentBlock - 1, fmt.Errorf("processing batch at %d: %w", ready.sliceStart, err)
			}
			if timing != nil {
				timing.ProcessBatchNs.Add(time.Since(processBatchStart).Nanoseconds())
			}

			sliceBlocks := int(ready.sliceEnd - ready.sliceStart + 1)
			blocksProcessed += sliceBlocks
			s.currentBlock = ready.sliceEnd + 1

			if timing != nil {
				timing.SliceCount.Add(1)
				timing.BlockCount.Add(int64(sliceBlocks))
				timing.TrxCount.Add(int64(sliceActionCount))
			}

			delete(pending, nextSlice)
			nextSlice++

			if timing != nil {
				timing.PendingSlices.Store(int32(len(pending)))
				timing.NextSlice.Store(nextSlice)
			}

			if processor.ShouldCommit(blocksProcessed) {
				commitStart := time.Now()
				if err := processor.Commit(ready.sliceEnd, true); err != nil {
					exit = true
					return s.currentBlock - 1, fmt.Errorf("committing at %d: %w", ready.sliceEnd, err)
				}
				if timing != nil {
					timing.CommitNs.Add(time.Since(commitStart).Nanoseconds())
				}
				s.logProgress(&lastLogTime, &lastLogBlock, endBlock, blocksProcessed, actionCount, len(pending))
				blocksProcessed = 0
				actionCount = 0
			}
		}
	}

	if err := processor.Flush(); err != nil {
		return s.currentBlock - 1, fmt.Errorf("flushing: %w", err)
	}

	return s.currentBlock - 1, nil
}

// extractActionSlice reads a slice and filters actions.
// This combines I/O (GetSliceBatch) and CPU work (FilterBlocksParallel) in one call,
// allowing workers to parallelize both. If timing is non-nil, records split timing.
func (s *Syncer) extractActionSlice(sr *SliceReader, startBlock, endBlock uint32, filterWorkers int, timing *BulkSyncTiming) ([]Block, error) {
	sliceGetStart := time.Now()
	batch, err := sr.GetSliceBatch(startBlock, endBlock)
	if err != nil {
		return nil, fmt.Errorf("get slice batch %d-%d: %w", startBlock, endBlock, err)
	}
	defer batch.Close()
	if timing != nil {
		timing.SliceGetNs.Add(time.Since(sliceGetStart).Nanoseconds())
	}

	extractStart := time.Now()
	blocks, err := FilterBlocksParallel(batch, startBlock, endBlock, s.config.ActionFilter, filterWorkers)
	if err != nil {
		return nil, fmt.Errorf("filter blocks %d-%d: %w", startBlock, endBlock, err)
	}
	if timing != nil {
		timing.SliceExtractNs.Add(time.Since(extractStart).Nanoseconds())
	}

	return blocks, nil
}

func countBlockStats(notif *RawBlock) (actions int, notifications int) {
	actions = len(notif.ActionMeta)
	for _, seqs := range notif.Notifications {
		notifications += len(seqs)
	}
	return
}

func filterRawBlock(notif RawBlock, filterFunc ActionFilterFunc) Block {
	filtered, _, _ := FilterRawBlockInto(notif, filterFunc, nil, nil)
	return filtered
}

func (s *Syncer) logProgress(lastLogTime *time.Time, lastLogBlock *uint32, endBlock uint32, blocksProcessed, actionCount, pendingSlices int) {
	now := time.Now()
	if now.Sub(*lastLogTime) < s.config.LogInterval {
		return
	}

	elapsed := now.Sub(*lastLogTime).Seconds()
	currentBlock := s.currentBlock - 1
	blocksDelta := currentBlock - *lastLogBlock
	bps := float64(blocksDelta) / elapsed
	aps := float64(actionCount) / elapsed
	pctComplete := float64(currentBlock) / float64(endBlock) * 100.0

	var dbSizeMB int64
	if s.dbSizeFunc != nil {
		dbSizeMB = s.dbSizeFunc() / 1024 / 1024
	}

	if s.progressCb != nil {
		s.progressCb(SyncProgress{
			CurrentBlock:  currentBlock,
			EndBlock:      endBlock,
			BlocksPerSec:  bps,
			ActionsPerSec: aps,
			DBSizeMB:      dbSizeMB,
			PendingSlices: pendingSlices,
			Mode:          "BULK",
		})
	}

	logger.Printf("sync", "Block: %10d / %10d (%.1f%%) | %s aps | %s bps | DB: %d MB",
		currentBlock, endBlock, pctComplete, logger.FormatNumber(aps), logger.FormatNumber(bps), dbSizeMB)

	*lastLogTime = now
	*lastLogBlock = currentBlock
}

func (s *Syncer) logLiveProgress(lastLogTime *time.Time, lastLogBlock *uint32, actionCount, notifCount *int, lib uint32) {
	now := time.Now()
	if now.Sub(*lastLogTime) < s.config.LogInterval {
		return
	}

	currentBlock := s.currentBlock

	var dbSizeMB int64
	if s.dbSizeFunc != nil {
		dbSizeMB = s.dbSizeFunc() / 1024 / 1024
	}

	elapsed := now.Sub(*lastLogTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	blocksDelta := currentBlock - *lastLogBlock
	bps := float64(blocksDelta) / elapsed
	aps := float64(*actionCount) / elapsed
	ips := float64(*notifCount) / elapsed

	if s.progressCb != nil {
		s.progressCb(SyncProgress{
			CurrentBlock:  currentBlock,
			EndBlock:      lib,
			BlocksPerSec:  bps,
			ActionsPerSec: aps,
			DBSizeMB:      dbSizeMB,
			PendingSlices: 0,
			Mode:          "LIVE",
		})
	}

	if s.config.LogInterval == 0 {
		logger.Printf("sync", "Block: %10d / %10d | %d idx | %d actions | DB: %d MB",
			currentBlock, lib, *notifCount, *actionCount, dbSizeMB)
	} else {
		logger.Printf("sync", "Block: %10d / %10d | %s idx/s | %s aps | %s bps | DB: %d MB",
			currentBlock, lib, logger.FormatNumber(ips), logger.FormatNumber(aps), logger.FormatNumber(bps), dbSizeMB)
	}

	*lastLogTime = now
	*lastLogBlock = currentBlock
	*actionCount = 0
	*notifCount = 0
}

// SyncTransactionIDs syncs transaction IDs using a TransactionProcessor.
// This is optimized for services that only need transaction IDs, not full action data.
func (s *Syncer) SyncTransactionIDs(processor TransactionProcessor, startBlock uint32) error {
	if s.running.Swap(true) {
		return fmt.Errorf("syncer already running")
	}
	defer s.running.Store(false)

	s.currentBlock = startBlock

	var sliceReader *SliceReader
	if s.baseReader != nil {
		sliceReader, _ = s.baseReader.(*SliceReader)
	}

	lastLiveLogTime := time.Now()
	lastLiveLogBlock := startBlock
	liveTrxCount := 0

	var initialLib uint32
	if s.reader != nil {
		_, initialLib, _ = s.reader.GetStateProps(true)
	} else {
		_, initialLib, _ = s.baseReader.GetStateProps(true)
	}
	initialBulkMode := s.currentBlock+s.config.BulkThreshold < initialLib
	if !initialBulkMode {
		if err := processor.Flush(); err != nil {
			return fmt.Errorf("initial flush: %w", err)
		}
		logger.Printf("sync", "Already caught up at startup (block %d, LIB %d) - transitioned to live mode", s.currentBlock, initialLib)
	}

	for {
		select {
		case <-s.stopChan:
			return nil
		default:
		}

		var lib uint32
		var err error
		if s.reader != nil {
			_, lib, err = s.reader.GetStateProps(true)
		} else {
			_, lib, err = s.baseReader.GetStateProps(true)
		}
		if err != nil {
			return fmt.Errorf("getting state props: %w", err)
		}

		bulkMode := s.currentBlock+s.config.BulkThreshold < lib

		if bulkMode {
			lastLiveLogTime = time.Now()
			lastLiveLogBlock = s.currentBlock
			liveTrxCount = 0

			if sliceReader == nil {
				return fmt.Errorf("bulk sync requires SliceReader (local slice access)")
			}

			batchProc, ok := processor.(BatchTransactionProcessor)
			if !ok {
				return fmt.Errorf("processor must implement BatchTransactionProcessor for bulk sync")
			}

			endBlock, err := s.bulkSyncTransactions(sliceReader, lib, batchProc)
			if err != nil {
				return fmt.Errorf("bulk sync: %w", err)
			}
			s.currentBlock = endBlock + 1
			continue
		}

		if s.currentBlock > lib {
			if sr, ok := s.reader.(StreamingReader); !ok || !sr.IsStreaming() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		// Live sync: fetch transaction IDs for current block
		var trxIDs [][32]byte
		if s.reader != nil {
			trxIDs, err = s.reader.GetTransactionIDsRaw(s.currentBlock)
		} else {
			trxIDs, err = s.baseReader.GetTransactionIDsRaw(s.currentBlock)
		}
		if err != nil {
			return fmt.Errorf("getting transaction IDs for block %d: %w", s.currentBlock, err)
		}

		block := BlockTransactionIDs{
			BlockNum: s.currentBlock,
			TrxIDs:   trxIDs,
		}

		if err := processor.ProcessBlock(block); err != nil {
			return fmt.Errorf("processing block %d: %w", s.currentBlock, err)
		}

		liveTrxCount += len(trxIDs)

		if processor.ShouldCommit(1) {
			if err := processor.Commit(s.currentBlock, false); err != nil {
				return fmt.Errorf("committing block %d: %w", s.currentBlock, err)
			}
		}

		s.logLiveTransactionProgress(&lastLiveLogTime, &lastLiveLogBlock, &liveTrxCount, lib)

		s.currentBlock++
	}
}

// trxSliceWork represents a slice extraction work item and its results.
// Workers receive slice ranges, perform I/O and extraction, and return extracted transaction IDs.
type trxSliceWork struct {
	sliceNum   uint32
	sliceStart uint32
	sliceEnd   uint32
	blocks     []BlockTransactionIDs
	err        error
}

func (s *Syncer) bulkSyncTransactions(sr *SliceReader, endBlock uint32, processor BatchTransactionProcessor) (uint32, error) {
	workers := s.config.Workers
	if workers < 1 {
		workers = 4
	}

	timing := s.bulkSyncTiming

	logger.Printf("sync", "bulkSyncTransactions: startBlock=%d, endBlock=%d, workers=%d",
		s.currentBlock, endBlock, workers)

	const sliceSize = uint32(10000)
	startSlice := (s.currentBlock - 1) / sliceSize
	endSlice := (endBlock - 1) / sliceSize

	if timing != nil {
		timing.EndSlice.Store(endSlice)
	}

	workChan := make(chan *trxSliceWork, workers*2)
	resultChan := make(chan *trxSliceWork, workers*2)

	var wg sync.WaitGroup
	exit := false

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				workerWaitStart := time.Now()
				work, ok := <-workChan
				if !ok {
					break
				}
				if timing != nil {
					timing.WorkerWaitNs.Add(time.Since(workerWaitStart).Nanoseconds())
				}
				if exit {
					resultChan <- work
					continue
				}
				work.blocks, work.err = s.extractTransactionSlice(sr, work.sliceStart, work.sliceEnd, timing)
				resultChan <- work
			}
		}()
	}

	go func() {
		for sliceNum := startSlice; sliceNum <= endSlice && !exit; sliceNum++ {
			sliceStart := sliceNum*sliceSize + 1
			sliceEnd := sliceStart + sliceSize - 1

			if sliceEnd > endBlock {
				sliceEnd = endBlock
			}
			if sliceStart < s.currentBlock {
				sliceStart = s.currentBlock
			}
			if sliceStart > endBlock {
				break
			}

			workChan <- &trxSliceWork{
				sliceNum:   sliceNum,
				sliceStart: sliceStart,
				sliceEnd:   sliceEnd,
			}
		}
		close(workChan)
		wg.Wait()
		close(resultChan)
	}()

	pending := make(map[uint32]*trxSliceWork)
	nextSlice := startSlice

	lastLogTime := time.Now()
	lastLogBlock := s.currentBlock
	blocksProcessed := 0
	trxCount := 0

	for {
		resultWaitStart := time.Now()
		work, ok := <-resultChan
		if !ok {
			break
		}
		if timing != nil {
			timing.ResultWaitNs.Add(time.Since(resultWaitStart).Nanoseconds())
		}

		select {
		case <-s.stopChan:
			exit = true
			for range resultChan {
			}
			return s.currentBlock - 1, nil
		default:
		}

		if exit {
			continue
		}

		if work.err != nil {
			logger.Printf("sync", "bulkSyncTransactions: error at slice %d: %v", work.sliceNum, work.err)
			continue
		}

		pending[work.sliceNum] = work

		if timing != nil {
			timing.PendingSlices.Store(int32(len(pending)))
			timing.NextSlice.Store(nextSlice)
		}

		for {
			ready, ok := pending[nextSlice]
			if !ok {
				break
			}

			sliceTrxCount := 0
			for _, block := range ready.blocks {
				sliceTrxCount += len(block.TrxIDs)
			}
			trxCount += sliceTrxCount

			processBatchStart := time.Now()
			if err := processor.ProcessBatch(ready.blocks); err != nil {
				exit = true
				return s.currentBlock - 1, fmt.Errorf("processing batch at %d: %w", ready.sliceStart, err)
			}
			if timing != nil {
				timing.ProcessBatchNs.Add(time.Since(processBatchStart).Nanoseconds())
			}

			sliceBlocks := int(ready.sliceEnd - ready.sliceStart + 1)
			blocksProcessed += sliceBlocks
			s.currentBlock = ready.sliceEnd + 1

			if timing != nil {
				timing.SliceCount.Add(1)
				timing.BlockCount.Add(int64(sliceBlocks))
				timing.TrxCount.Add(int64(sliceTrxCount))
			}

			delete(pending, nextSlice)
			nextSlice++

			if timing != nil {
				timing.PendingSlices.Store(int32(len(pending)))
				timing.NextSlice.Store(nextSlice)
			}

			if processor.ShouldCommit(blocksProcessed) {
				commitStart := time.Now()
				if err := processor.Commit(ready.sliceEnd, true); err != nil {
					exit = true
					return s.currentBlock - 1, fmt.Errorf("committing at %d: %w", ready.sliceEnd, err)
				}
				if timing != nil {
					timing.CommitNs.Add(time.Since(commitStart).Nanoseconds())
				}
				s.logTransactionProgress(&lastLogTime, &lastLogBlock, endBlock, blocksProcessed, trxCount, len(pending))
				blocksProcessed = 0
				trxCount = 0
			}
		}
	}

	if err := processor.Flush(); err != nil {
		return s.currentBlock - 1, fmt.Errorf("flushing: %w", err)
	}

	return s.currentBlock - 1, nil
}

// extractTransactionSlice reads a slice and extracts transaction IDs.
// This combines I/O (GetSliceBatch) and CPU work (ExtractTransactionIDsOnly) in one call,
// allowing workers to parallelize both. If timing is non-nil, records split timing.
func (s *Syncer) extractTransactionSlice(sr *SliceReader, startBlock, endBlock uint32, timing *BulkSyncTiming) ([]BlockTransactionIDs, error) {
	sliceGetStart := time.Now()
	batch, err := sr.GetSliceBatch(startBlock, endBlock)
	if err != nil {
		return nil, fmt.Errorf("get slice batch %d-%d: %w", startBlock, endBlock, err)
	}
	defer batch.Close()
	if timing != nil {
		timing.SliceGetNs.Add(time.Since(sliceGetStart).Nanoseconds())
	}

	extractStart := time.Now()
	trxIDsMap, err := batch.ExtractTransactionIDsOnly(8)
	if err != nil {
		return nil, fmt.Errorf("extract transaction IDs %d-%d: %w", startBlock, endBlock, err)
	}
	if timing != nil {
		timing.SliceExtractNs.Add(time.Since(extractStart).Nanoseconds())
	}

	blocks := make([]BlockTransactionIDs, 0, len(trxIDsMap))
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if ids, found := trxIDsMap[blockNum]; found {
			blocks = append(blocks, BlockTransactionIDs{
				BlockNum: blockNum,
				TrxIDs:   ids,
			})
		}
	}

	return blocks, nil
}

func (s *Syncer) logTransactionProgress(lastLogTime *time.Time, lastLogBlock *uint32, endBlock uint32, blocksProcessed, trxCount, pendingSlices int) {
	now := time.Now()
	if now.Sub(*lastLogTime) < s.config.LogInterval {
		return
	}

	elapsed := now.Sub(*lastLogTime).Seconds()
	currentBlock := s.currentBlock - 1
	blocksDelta := currentBlock - *lastLogBlock
	bps := float64(blocksDelta) / elapsed
	tps := float64(trxCount) / elapsed
	pctComplete := float64(currentBlock) / float64(endBlock) * 100.0

	var dbSizeMB int64
	if s.dbSizeFunc != nil {
		dbSizeMB = s.dbSizeFunc() / 1024 / 1024
	}

	if s.progressCb != nil {
		s.progressCb(SyncProgress{
			CurrentBlock:  currentBlock,
			EndBlock:      endBlock,
			BlocksPerSec:  bps,
			ActionsPerSec: tps,
			DBSizeMB:      dbSizeMB,
			PendingSlices: pendingSlices,
			Mode:          "BULK",
		})
	}

	logger.Printf("sync", "Block: %10d / %10d (%.1f%%) | %s tps | %s bps | DB: %d MB",
		currentBlock, endBlock, pctComplete, logger.FormatNumber(tps), logger.FormatNumber(bps), dbSizeMB)

	*lastLogTime = now
	*lastLogBlock = currentBlock
}

func (s *Syncer) logLiveTransactionProgress(lastLogTime *time.Time, lastLogBlock *uint32, trxCount *int, lib uint32) {
	now := time.Now()
	if now.Sub(*lastLogTime) < s.config.LogInterval {
		return
	}

	currentBlock := s.currentBlock

	var dbSizeMB int64
	if s.dbSizeFunc != nil {
		dbSizeMB = s.dbSizeFunc() / 1024 / 1024
	}

	elapsed := now.Sub(*lastLogTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	blocksDelta := currentBlock - *lastLogBlock
	bps := float64(blocksDelta) / elapsed
	tps := float64(*trxCount) / elapsed

	if s.progressCb != nil {
		s.progressCb(SyncProgress{
			CurrentBlock:  currentBlock,
			EndBlock:      lib,
			BlocksPerSec:  bps,
			ActionsPerSec: tps,
			DBSizeMB:      dbSizeMB,
			PendingSlices: 0,
			Mode:          "LIVE",
		})
	}

	logger.Printf("sync", "Block: %10d / %10d | %s tps | %s bps | DB: %d MB",
		currentBlock, lib, logger.FormatNumber(tps), logger.FormatNumber(bps), dbSizeMB)

	*lastLogTime = now
	*lastLogBlock = currentBlock
	*trxCount = 0
}
