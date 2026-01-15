package internal

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type BulkSyncTiming struct {
	sliceGetNs       atomic.Int64
	slicePreExtNs    atomic.Int64
	sliceBufferAddNs atomic.Int64
	sliceCount       atomic.Int64
	blockCount       atomic.Int64
	trxCount         atomic.Int64

	workerWaitNs  atomic.Int64
	resultWaitNs  atomic.Int64
	flushWaitNs   atomic.Int64
	mainLoopOther atomic.Int64

	pendingSlices atomic.Int32
	nextSlice     atomic.Uint32
	lastSlice     atomic.Uint32

	lastHeapBytes uint64
	lastGCCount   uint32

	stopChan chan struct{}
}

func NewBulkSyncTiming() *BulkSyncTiming {
	return &BulkSyncTiming{
		stopChan: make(chan struct{}),
	}
}

func (t *BulkSyncTiming) RecordSliceGet(d time.Duration) {
	t.sliceGetNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) RecordPreExtract(d time.Duration) {
	t.slicePreExtNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) RecordBufferAdd(d time.Duration) {
	t.sliceBufferAddNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) RecordSlice(blockCount, trxCount int) {
	t.sliceCount.Add(1)
	t.blockCount.Add(int64(blockCount))
	t.trxCount.Add(int64(trxCount))
}

func (t *BulkSyncTiming) RecordWorkerWait(d time.Duration) {
	t.workerWaitNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) RecordResultWait(d time.Duration) {
	t.resultWaitNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) RecordFlushWait(d time.Duration) {
	t.flushWaitNs.Add(d.Nanoseconds())
}

func (t *BulkSyncTiming) SetPendingState(pending int, next, last uint32) {
	t.pendingSlices.Store(int32(pending))
	t.nextSlice.Store(next)
	t.lastSlice.Store(last)
}

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

func (t *BulkSyncTiming) Stop() {
	close(t.stopChan)
}

func (t *BulkSyncTiming) PrintStats() {
	slices := t.sliceCount.Load()
	if slices == 0 {
		return
	}

	blocks := t.blockCount.Load()
	trxs := t.trxCount.Load()

	sliceGetNs := t.sliceGetNs.Load()
	preExtNs := t.slicePreExtNs.Load()
	bufferAddNs := t.sliceBufferAddNs.Load()

	workerWaitNs := t.workerWaitNs.Load()
	resultWaitNs := t.resultWaitNs.Load()
	flushWaitNs := t.flushWaitNs.Load()

	sliceWorkNs := sliceGetNs + preExtNs + bufferAddNs
	totalNs := sliceWorkNs + workerWaitNs + resultWaitNs + flushWaitNs

	if totalNs == 0 {
		return
	}

	pctSliceGet := float64(sliceGetNs) / float64(totalNs) * 100
	pctPreExt := float64(preExtNs) / float64(totalNs) * 100
	pctBufferAdd := float64(bufferAddNs) / float64(totalNs) * 100
	pctWorkerWait := float64(workerWaitNs) / float64(totalNs) * 100
	pctResultWait := float64(resultWaitNs) / float64(totalNs) * 100
	pctFlushWait := float64(flushWaitNs) / float64(totalNs) * 100

	avgSliceGetMs := float64(sliceGetNs) / float64(slices) / 1e6
	avgPreExtMs := float64(preExtNs) / float64(slices) / 1e6

	var trxPerBlock float64
	if blocks > 0 {
		trxPerBlock = float64(trxs) / float64(blocks)
	}

	logger.Printf("debug-timing",
		"SliceGet=%.1f%% (%.1fms/slice) | PreExt=%.1f%% (%.1fms/slice) | %.1f trx/blk",
		pctSliceGet, avgSliceGetMs,
		pctPreExt, avgPreExtMs,
		trxPerBlock)

	pendingCount := t.pendingSlices.Load()
	nextSliceNum := t.nextSlice.Load()
	lastSliceNum := t.lastSlice.Load()

	logger.Printf("debug-timing",
		"MainLoop: WorkerWait=%.1f%% | ResultWait=%.1f%% | FlushWait=%.1f%% | BufferAdd=%.1f%% | Pending=%d (next=%d last=%d)",
		pctWorkerWait, pctResultWait, pctFlushWait, pctBufferAdd,
		pendingCount, nextSliceNum, lastSliceNum)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapMB := float64(m.HeapAlloc) / (1024 * 1024)
	deltaMB := float64(int64(m.HeapAlloc)-int64(t.lastHeapBytes)) / (1024 * 1024)
	gcDelta := m.NumGC - t.lastGCCount
	t.lastHeapBytes = m.HeapAlloc
	t.lastGCCount = m.NumGC

	logger.Printf("debug-timing",
		"Memory: Heap=%.1f MB (Δ%+.1f MB) | GC=%d | Goroutines=%d",
		heapMB, deltaMB, gcDelta, runtime.NumGoroutine())
}

func (t *BulkSyncTiming) Reset() {
	t.sliceGetNs.Store(0)
	t.slicePreExtNs.Store(0)
	t.sliceBufferAddNs.Store(0)
	t.sliceCount.Store(0)
	t.blockCount.Store(0)
	t.trxCount.Store(0)
	t.workerWaitNs.Store(0)
	t.resultWaitNs.Store(0)
	t.flushWaitNs.Store(0)
	t.mainLoopOther.Store(0)
}
