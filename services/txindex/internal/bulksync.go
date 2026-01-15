package internal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

type BulkIndexer struct {
	idx         *TrxIndex
	reader      corereader.Reader
	sliceReader *corereader.SliceReader
	config      *Config
	bufferLimit int
	tempDir     string
	runCount    int
	lastBlock   uint32
	logInterval time.Duration

	activeBuffer  []WALEntry
	flushBuffer   []WALEntry
	activeBuffNum int
	flushBuffNum  int
	flushMu       sync.Mutex
	flushCond     *sync.Cond
	flushPending  bool
	flushPhase    string
	flushErr      error
	flushDone     chan struct{}

	timing *BulkSyncTiming
}

func NewBulkIndexer(idx *TrxIndex, reader corereader.Reader, config *Config) *BulkIndexer {
	entriesPerMB := (1024 * 1024) / WALEntrySize
	bufferLimit := config.BulkBufferMB * entriesPerMB

	var sliceReader *corereader.SliceReader
	if unwrapper, ok := reader.(interface {
		GetBaseReader() corereader.BaseReader
	}); ok {
		if sr, ok := unwrapper.GetBaseReader().(*corereader.SliceReader); ok {
			sliceReader = sr
		}
	}

	timing := NewBulkSyncTiming()

	bi := &BulkIndexer{
		idx:           idx,
		reader:        reader,
		sliceReader:   sliceReader,
		config:        config,
		bufferLimit:   bufferLimit,
		tempDir:       filepath.Join(idx.basePath, "temp"),
		activeBuffer:  make([]WALEntry, 0, bufferLimit),
		flushBuffer:   make([]WALEntry, 0, bufferLimit),
		activeBuffNum: 1,
		flushDone:     make(chan struct{}),
		timing:        timing,
		logInterval:   config.GetLogInterval(),
	}
	bi.flushCond = sync.NewCond(&bi.flushMu)

	meta := idx.GetMeta()
	if meta.BulkRunCount > 0 {
		bi.runCount = meta.BulkRunCount
		bi.lastBlock = meta.BulkLastBlock
		logger.Printf("sync", "Resuming bulk sync: %d existing runs, last block %d", bi.runCount, bi.lastBlock)
	}

	return bi
}

func (b *BulkIndexer) startFlushWorker() {
	go func() {
		for {
			b.flushMu.Lock()
			for !b.flushPending {
				b.flushCond.Wait()
			}

			if len(b.flushBuffer) == 0 {
				b.flushPending = false
				b.flushMu.Unlock()
				close(b.flushDone)
				return
			}

			bufferToFlush := b.flushBuffer
			b.flushMu.Unlock()

			err := b.doFlush(bufferToFlush)

			b.flushMu.Lock()
			b.flushErr = err
			b.flushPending = false
			b.flushBuffer = b.flushBuffer[:0]
			b.flushCond.Broadcast()
			b.flushMu.Unlock()
		}
	}()
}

func (b *BulkIndexer) stopFlushWorker() {
	b.flushMu.Lock()
	b.flushPending = true
	b.flushCond.Signal()
	b.flushMu.Unlock()
	<-b.flushDone
}

func (b *BulkIndexer) swapAndFlush() error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	for b.flushPending {
		b.flushCond.Wait()
	}

	if b.flushErr != nil {
		return b.flushErr
	}

	b.activeBuffer, b.flushBuffer = b.flushBuffer, b.activeBuffer
	b.flushBuffNum = b.activeBuffNum
	b.activeBuffNum++
	b.flushPending = true
	b.flushCond.Signal()

	return nil
}

func (b *BulkIndexer) waitForFlush() error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	for b.flushPending {
		b.flushCond.Wait()
	}

	return b.flushErr
}

func (b *BulkIndexer) doFlush(buffer []WALEntry) error {
	if len(buffer) == 0 {
		return nil
	}

	b.flushMu.Lock()
	b.flushPhase = "sorting"
	b.flushMu.Unlock()

	parallelSortWALEntries(buffer)

	b.flushMu.Lock()
	b.flushPhase = "writing"
	b.flushMu.Unlock()

	runPath := filepath.Join(b.tempDir, fmt.Sprintf("run_%06d.tmp", b.runCount))
	f, err := os.Create(runPath)
	if err != nil {
		return fmt.Errorf("create run file: %w", err)
	}

	w := bufio.NewWriterSize(f, 4*1024*1024)
	var entryBuf [WALEntrySize]byte
	for i := range buffer {
		copy(entryBuf[0:5], buffer[i].Prefix5[:])
		binary.LittleEndian.PutUint32(entryBuf[5:9], buffer[i].BlockNum)
		if _, err := w.Write(entryBuf[:]); err != nil {
			f.Close()
			return fmt.Errorf("write entry: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close run: %w", err)
	}

	b.runCount++

	b.idx.SetBulkProgress(b.runCount, b.lastBlock)
	if err := b.idx.SaveMeta(); err != nil {
		logger.Printf("sync", "Warning: failed to save bulk progress: %v", err)
	}

	b.flushMu.Lock()
	bufNum := b.flushBuffNum
	b.flushMu.Unlock()

	logger.Printf("sync", "Flushed buf %d with %s entries (checkpoint: block %s)", bufNum, logger.FormatCount(int64(len(buffer))), logger.FormatCount(int64(b.lastBlock)))
	return nil
}

func (b *BulkIndexer) ProcessRange(startBlock, endBlock uint32, exit *bool) error {
	if b.runCount > 0 {
		logger.Printf("sync", "BULK SYNC: Resuming from block %d to %d (%d runs exist)", startBlock, endBlock, b.runCount)
	} else {
		logger.Printf("sync", "BULK SYNC: Processing blocks %d to %d", startBlock, endBlock)
	}

	if startBlock > endBlock {
		logger.Printf("sync", "BULK SYNC: Already caught up, skipping to merge")
		return nil
	}

	if b.config.Debug && b.timing != nil {
		b.timing.StartReporter(10 * time.Second)
		defer b.timing.Stop()
	}

	b.startFlushWorker()
	defer b.stopFlushWorker()

	const sliceSize = uint32(10000)
	workerCount := b.config.Workers
	if workerCount < 1 {
		workerCount = 4
	}

	type sliceWork struct {
		sliceNum   uint32
		sliceStart uint32
		sliceEnd   uint32
		trxEntries []WALEntry
		err        error
	}

	workChan := make(chan *sliceWork, workerCount*2)
	resultChan := make(chan *sliceWork, workerCount*2)

	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tWait := time.Now()
				work, ok := <-workChan
				if !ok {
					break
				}
				if b.timing != nil {
					b.timing.RecordWorkerWait(time.Since(tWait))
				}
				if *exit {
					resultChan <- work
					continue
				}
				work.trxEntries, work.err = b.processSlice(work.sliceStart, work.sliceEnd)
				resultChan <- work
			}
		}()
	}

	go func() {
		startSlice := (startBlock - 1) / sliceSize
		endSlice := (endBlock - 1) / sliceSize

		for sliceNum := startSlice; sliceNum <= endSlice && !*exit; sliceNum++ {
			sliceStart := sliceNum*sliceSize + 1
			sliceEnd := sliceStart + sliceSize - 1

			if sliceEnd > endBlock {
				sliceEnd = endBlock
			}
			if sliceStart < startBlock {
				sliceStart = startBlock
			}

			workChan <- &sliceWork{
				sliceNum:   sliceNum,
				sliceStart: sliceStart,
				sliceEnd:   sliceEnd,
			}
		}
		close(workChan)
		wg.Wait()
		close(resultChan)
	}()

	pending := make(map[uint32]*sliceWork)
	nextSlice := (startBlock - 1) / sliceSize

	startTime := time.Now()
	lastLogTime := startTime
	lastLogBlock := startBlock - 1
	var processedTrx int64
	var totalTrx int64

	for {
		tWait := time.Now()
		work, ok := <-resultChan
		if !ok {
			break
		}
		if b.timing != nil {
			b.timing.RecordResultWait(time.Since(tWait))
		}

		if *exit {
			continue
		}

		if work.err != nil {
			logger.Printf("sync", "BULK SYNC: Error at slice %d: %v", work.sliceNum, work.err)
			continue
		}

		pending[work.sliceNum] = work

		endSlice := (endBlock - 1) / sliceSize
		if b.timing != nil {
			b.timing.SetPendingState(len(pending), nextSlice, endSlice)
		}

		for {
			ready, ok := pending[nextSlice]
			if !ok {
				break
			}

			tBufAdd := time.Now()
			for _, entry := range ready.trxEntries {
				b.activeBuffer = append(b.activeBuffer, entry)
				if len(b.activeBuffer) >= b.bufferLimit {
					tFlush := time.Now()
					if err := b.swapAndFlush(); err != nil {
						return fmt.Errorf("flush run: %w", err)
					}
					if b.timing != nil {
						b.timing.RecordFlushWait(time.Since(tFlush))
					}
				}
			}
			if b.timing != nil {
				b.timing.RecordBufferAdd(time.Since(tBufAdd))
			}

			atomic.AddInt64(&processedTrx, int64(len(ready.trxEntries)))
			atomic.AddInt64(&totalTrx, int64(len(ready.trxEntries)))
			b.lastBlock = ready.sliceEnd

			delete(pending, nextSlice)
			nextSlice++
		}

		now := time.Now()
		if now.Sub(lastLogTime) >= b.logInterval {
			elapsed := now.Sub(lastLogTime).Seconds()
			txs := atomic.LoadInt64(&processedTrx)
			bps := float64(b.lastBlock-lastLogBlock) / elapsed
			tps := float64(txs) / elapsed
			pct := float64(b.lastBlock) / float64(endBlock) * 100

			b.flushMu.Lock()
			flushing := b.flushPending
			flushPhase := b.flushPhase
			flushBuffNum := b.flushBuffNum
			activeBuffNum := b.activeBuffNum
			b.flushMu.Unlock()

			status := fmt.Sprintf("Buf %d: %s/%s",
				activeBuffNum,
				logger.FormatCount(int64(len(b.activeBuffer))),
				logger.FormatCount(int64(b.bufferLimit)))
			if flushing && flushPhase != "" {
				status += fmt.Sprintf(" | Buf %d: %s", flushBuffNum, flushPhase)
			}

			logger.Printf("sync", "Block: %s / %s (%.1f%%) | %s BPS | %s TPS | %s",
				logger.FormatCount(int64(b.lastBlock)), logger.FormatCount(int64(endBlock)), pct,
				logger.FormatRate(bps), logger.FormatRate(tps),
				status)

			lastLogTime = now
			lastLogBlock = b.lastBlock
			atomic.StoreInt64(&processedTrx, 0)
		}
	}

	if len(b.activeBuffer) > 0 {
		if err := b.swapAndFlush(); err != nil {
			return fmt.Errorf("flush final run: %w", err)
		}
	}

	if err := b.waitForFlush(); err != nil {
		return fmt.Errorf("wait for final flush: %w", err)
	}

	elapsed := time.Since(startTime)
	blocksProcessed := b.lastBlock - startBlock + 1
	totalTxCount := atomic.LoadInt64(&totalTrx)
	logger.Printf("sync", "Block scan complete: %s blocks, %s transactions in %.1fs (%s BPS)",
		logger.FormatCount(int64(blocksProcessed)), logger.FormatCount(totalTxCount), elapsed.Seconds(),
		logger.FormatRate(float64(blocksProcessed)/elapsed.Seconds()))

	b.idx.SetLastIndexedBlock(b.lastBlock)
	return nil
}

func (b *BulkIndexer) processSlice(startBlock, endBlock uint32) ([]WALEntry, error) {
	if b.sliceReader == nil {
		return b.processSliceSlow(startBlock, endBlock)
	}

	t0 := time.Now()
	batch, err := b.sliceReader.GetSliceBatch(startBlock, endBlock)
	if err != nil {
		return nil, fmt.Errorf("get slice batch %d-%d: %w", startBlock, endBlock, err)
	}
	defer batch.Close()
	sliceGetTime := time.Since(t0)

	t1 := time.Now()
	numWorkers := 8

	trxIDsMap, err := batch.ExtractTransactionIDsOnly(numWorkers)
	if err != nil {
		return nil, fmt.Errorf("extract transaction IDs %d-%d: %w", startBlock, endBlock, err)
	}
	preExtTime := time.Since(t1)

	blockCount := int(endBlock - startBlock + 1)

	// Convert transaction IDs to WAL entries
	totalEntries := 0
	for _, trxIDs := range trxIDsMap {
		totalEntries += len(trxIDs)
	}

	entries := make([]WALEntry, 0, totalEntries)
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		trxIDs, found := trxIDsMap[blockNum]
		if !found {
			continue
		}
		for _, trxID := range trxIDs {
			var prefix5 [5]byte
			copy(prefix5[:], trxID[:5])
			entries = append(entries, WALEntry{
				Prefix5:  prefix5,
				BlockNum: blockNum,
			})
		}
	}

	if b.timing != nil {
		b.timing.RecordSliceGet(sliceGetTime)
		b.timing.RecordPreExtract(preExtTime)
		b.timing.RecordSlice(blockCount, len(entries))
	}

	return entries, nil
}

func (b *BulkIndexer) processSliceSlow(startBlock, endBlock uint32) ([]WALEntry, error) {
	var entries []WALEntry

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		trxIDs, _, _, err := b.reader.GetTransactionIDsOnly(blockNum, false)
		if err != nil {
			continue
		}

		for _, trxIDHex := range trxIDs {
			prefix5, err := TrxIDToPrefix5(trxIDHex)
			if err != nil {
				continue
			}
			entries = append(entries, WALEntry{
				Prefix5:  prefix5,
				BlockNum: blockNum,
			})
		}
	}

	return entries, nil
}

func (b *BulkIndexer) Finalize() error {
	if b.runCount == 0 {
		logger.Printf("sync", "No runs to merge")
		return nil
	}

	logger.Printf("sync", "Starting k-way merge of %d runs", b.runCount)

	runPaths := make([]string, b.runCount)
	for i := range b.runCount {
		runPaths[i] = filepath.Join(b.tempDir, fmt.Sprintf("run_%06d.tmp", i))
	}

	merger, err := NewMerger(runPaths)
	if err != nil {
		return fmt.Errorf("create merger: %w", err)
	}
	defer merger.Close()

	partitionBuffers := make(map[uint16][]partitionEntry)

	entriesWritten := 0
	totalEntries := merger.TotalEntries()
	lastLogTime := time.Now()

	for merger.Len() > 0 {
		entry := merger.Pop()

		partitionID := Prefix5ToPartitionID(entry.Prefix5)
		prefix3 := Prefix5ToPrefix3(entry.Prefix5)

		partitionBuffers[partitionID] = append(partitionBuffers[partitionID], partitionEntry{
			prefix3:  prefix3,
			blockNum: entry.BlockNum,
		})
		entriesWritten++

		now := time.Now()
		if now.Sub(lastLogTime) >= b.logInterval {
			pct := float64(entriesWritten) / float64(totalEntries) * 100
			logger.Printf("sync", "Merge: %s / %s entries (%.1f%%) | %d partitions",
				logger.FormatCount(int64(entriesWritten)), logger.FormatCount(int64(totalEntries)), pct, len(partitionBuffers))
			lastLogTime = now
		}
	}

	logger.Printf("sync", "Writing %d partitions to disk with %d workers...", len(partitionBuffers), b.config.Workers)

	type writeWork struct {
		partitionID uint16
		entries     []partitionEntry
	}

	workChan := make(chan writeWork, b.config.Workers*2)
	var writeErr atomic.Value
	var partitionsWritten atomic.Int64
	var wgWrite sync.WaitGroup

	go func() {
		lastLogTime := time.Now()
		total := int64(len(partitionBuffers))
		for {
			written := partitionsWritten.Load()
			if written >= total {
				break
			}
			now := time.Now()
			if now.Sub(lastLogTime) >= b.logInterval {
				pct := float64(written) / float64(total) * 100
				logger.Printf("sync", "Partitions: %d / %d (%.1f%%)", written, total, pct)
				lastLogTime = now
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	for range b.config.Workers {
		wgWrite.Add(1)
		go func() {
			defer wgWrite.Done()
			for work := range workChan {
				if writeErr.Load() != nil {
					continue
				}

				path := PartitionIDToPath(b.idx.basePath, work.partitionID)
				dir := PartitionIDToDirPath(b.idx.basePath, work.partitionID)
				if err := os.MkdirAll(dir, 0755); err != nil {
					writeErr.Store(fmt.Errorf("mkdir partition dir %04x: %w", work.partitionID, err))
					continue
				}

				writer := NewPartitionWriter(path)
				for _, e := range work.entries {
					writer.AddEntry(e.prefix3, e.blockNum)
				}
				if err := writer.Finalize(); err != nil {
					writeErr.Store(fmt.Errorf("finalize partition %04x: %w", work.partitionID, err))
					continue
				}

				partitionsWritten.Add(1)
			}
		}()
	}

	for partitionID, entries := range partitionBuffers {
		workChan <- writeWork{partitionID: partitionID, entries: entries}
	}
	close(workChan)
	wgWrite.Wait()

	if err := writeErr.Load(); err != nil {
		return err.(error)
	}

	for i := range b.runCount {
		runPath := filepath.Join(b.tempDir, fmt.Sprintf("run_%06d.tmp", i))
		os.Remove(runPath)
	}

	b.idx.ClearBulkProgress()
	b.idx.SetLastMergedBlock(b.lastBlock)
	b.idx.SetMode("live")
	b.idx.SaveMeta()

	logger.Printf("sync", "Merge complete: %s entries across %d partitions", logger.FormatCount(int64(entriesWritten)), len(partitionBuffers))
	return nil
}

type partitionEntry struct {
	prefix3  [3]byte
	blockNum uint32
}

func parallelSortWALEntries(data []WALEntry) {
	n := len(data)
	if n < 100000 {
		slices.SortFunc(data, func(a, b WALEntry) int {
			return ComparePrefix5(a.Prefix5, b.Prefix5)
		})
		return
	}

	numCPU := runtime.NumCPU()
	if numCPU > 16 {
		numCPU = 16
	}

	chunkSize := (n + numCPU - 1) / numCPU
	var wg sync.WaitGroup

	for i := 0; i < numCPU; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= end {
			break
		}

		wg.Add(1)
		go func(chunk []WALEntry) {
			defer wg.Done()
			slices.SortFunc(chunk, func(a, b WALEntry) int {
				return ComparePrefix5(a.Prefix5, b.Prefix5)
			})
		}(data[start:end])
	}
	wg.Wait()

	mergeChunks(data, chunkSize, numCPU)
}

func mergeChunks(data []WALEntry, chunkSize, numChunks int) {
	n := len(data)
	temp := make([]WALEntry, n)

	for size := chunkSize; size < n; size *= 2 {
		for left := 0; left < n; left += 2 * size {
			mid := left + size
			right := left + 2*size
			if mid > n {
				mid = n
			}
			if right > n {
				right = n
			}
			mergeInto(data[left:mid], data[mid:right], temp[left:right])
		}
		copy(data, temp)
	}
}

func mergeInto(left, right, dest []WALEntry) {
	i, j, k := 0, 0, 0
	for i < len(left) && j < len(right) {
		if ComparePrefix5(left[i].Prefix5, right[j].Prefix5) <= 0 {
			dest[k] = left[i]
			i++
		} else {
			dest[k] = right[j]
			j++
		}
		k++
	}
	for i < len(left) {
		dest[k] = left[i]
		i++
		k++
	}
	for j < len(right) {
		dest[k] = right[j]
		j++
		k++
	}
}
