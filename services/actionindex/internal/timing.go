package internal

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

type SyncTiming struct {
	// Block processing
	processBlockNs  atomic.Int64
	processBlockCnt atomic.Int64
	actionsCnt      atomic.Int64

	// ProcessBlock breakdown
	filterNs     atomic.Int64
	batchBuildNs atomic.Int64
	blockTimeNs  atomic.Int64

	// Filter phase breakdown (shared with corereader)
	FilterTiming *corereader.FilterTiming

	// Per-action index adds
	indexAddNs atomic.Int64

	// Flush operations
	flushNs   atomic.Int64
	flushCnt  atomic.Int64
	flushKeys atomic.Int64

	// Commit operations
	commitSyncNs    atomic.Int64
	commitNoSyncNs  atomic.Int64
	commitSyncCnt   atomic.Int64
	commitNoSyncCnt atomic.Int64

	// Memory tracking
	lastHeapBytes uint64
	lastGCCount   uint32

	store *Store

	// Diagnostics provider (optional)
	diagProvider DiagnosticsProvider

	// Control
	stopChan chan struct{}
}

func NewSyncTiming() *SyncTiming {
	return &SyncTiming{
		FilterTiming: &corereader.FilterTiming{},
		stopChan:     make(chan struct{}),
	}
}

func (t *SyncTiming) RecordProcessBlock(duration time.Duration, actionCount int) {
	t.processBlockNs.Add(duration.Nanoseconds())
	t.processBlockCnt.Add(1)
	t.actionsCnt.Add(int64(actionCount))
}

func (t *SyncTiming) RecordFilter(duration time.Duration) {
	t.filterNs.Add(duration.Nanoseconds())
}

func (t *SyncTiming) RecordBatchBuild(duration time.Duration) {
	t.batchBuildNs.Add(duration.Nanoseconds())
}

func (t *SyncTiming) RecordBlockTime(duration time.Duration) {
	t.blockTimeNs.Add(duration.Nanoseconds())
}

func (t *SyncTiming) RecordIndexAdd(duration time.Duration) {
	t.indexAddNs.Add(duration.Nanoseconds())
}

func (t *SyncTiming) RecordFlush(duration time.Duration, keyCount int) {
	t.flushNs.Add(duration.Nanoseconds())
	t.flushCnt.Add(1)
	t.flushKeys.Add(int64(keyCount))
}

func (t *SyncTiming) RecordCommit(duration time.Duration, withSync bool) {
	if withSync {
		t.commitSyncNs.Add(duration.Nanoseconds())
		t.commitSyncCnt.Add(1)
	} else {
		t.commitNoSyncNs.Add(duration.Nanoseconds())
		t.commitNoSyncCnt.Add(1)
	}
}

func (t *SyncTiming) SetStore(store *Store) {
	t.store = store
}

func (t *SyncTiming) SetDiagnosticsProvider(dp DiagnosticsProvider) {
	t.diagProvider = dp
}

func (t *SyncTiming) StartReporter(interval time.Duration) {
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

func (t *SyncTiming) Stop() {
	close(t.stopChan)
}

func (t *SyncTiming) PrintStats() {
	blocks := t.processBlockCnt.Load()
	if blocks == 0 {
		return
	}

	actions := t.actionsCnt.Load()
	flushes := t.flushCnt.Load()

	processNs := t.processBlockNs.Load()
	indexAddNs := t.indexAddNs.Load()
	flushNs := t.flushNs.Load()
	commitSyncNs := t.commitSyncNs.Load()
	commitNoSyncNs := t.commitNoSyncNs.Load()

	totalNs := processNs + flushNs + commitSyncNs + commitNoSyncNs
	if totalNs == 0 {
		return
	}

	pctProcess := float64(processNs) / float64(totalNs) * 100
	pctIndexAdd := float64(indexAddNs) / float64(totalNs) * 100
	pctFlush := float64(flushNs) / float64(totalNs) * 100
	pctCommit := float64(commitSyncNs+commitNoSyncNs) / float64(totalNs) * 100

	avgProcessUs := float64(processNs) / float64(blocks) / 1000
	avgIndexAddUs := float64(indexAddNs) / float64(blocks) / 1000
	var avgFlushMs float64
	if flushes > 0 {
		avgFlushMs = float64(flushNs) / float64(flushes) / 1e6
	}

	logger.Printf("debug-timing",
		"Process=%.1f%% (%.1fus/blk) | IndexAdd=%.1f%% (%.1fus/blk, %.0f act) | Flush=%.1f%% (%.1fms) | Commit=%.1f%%",
		pctProcess, avgProcessUs,
		pctIndexAdd, avgIndexAddUs, float64(actions)/float64(blocks),
		pctFlush, avgFlushMs,
		pctCommit)

	// ProcessBlock breakdown
	filterNs := t.filterNs.Load()
	batchBuildNs := t.batchBuildNs.Load()
	blockTimeNs := t.blockTimeNs.Load()
	if processNs > 0 {
		pctFilter := float64(filterNs) / float64(processNs) * 100
		pctBatchBuild := float64(batchBuildNs) / float64(processNs) * 100
		pctBlockTime := float64(blockTimeNs) / float64(processNs) * 100
		pctIndexInProcess := float64(indexAddNs) / float64(processNs) * 100
		other := 100.0 - pctFilter - pctBatchBuild - pctBlockTime - pctIndexInProcess
		logger.Printf("debug-timing",
			"  ProcessBreakdown: Filter=%.1f%% BatchBuild=%.1f%% IndexAdd=%.1f%% BlockTime=%.1f%% Other=%.1f%%",
			pctFilter, pctBatchBuild, pctIndexInProcess, pctBlockTime, other)

		// Filter phase breakdown
		if t.FilterTiming != nil && t.FilterTiming.CallCount.Load() > 0 {
			metaPct, gsPct, ordPct, famPct, notifPct, resetPct := t.FilterTiming.GetBreakdown()
			logger.Printf("debug-timing",
				"    FilterBreakdown: Meta=%.1f%% GsCanon=%.1f%% OrdRoot=%.1f%% FamSlots=%.1f%% Notifs=%.1f%% Reset=%.1f%%",
				metaPct, gsPct, ordPct, famPct, notifPct, resetPct)
			recvHits := t.FilterTiming.ReceiverHits.Load()
			authHits := t.FilterTiming.AuthorizerHits.Load()
			skipped := t.FilterTiming.Skipped.Load()
			total := recvHits + authHits + skipped
			if total > 0 {
				logger.Printf("debug-timing",
					"    NotifPaths: Receiver=%d (%.1f%%) Authorizer=%d (%.1f%%) Skipped=%d (%.1f%%)",
					recvHits, float64(recvHits)/float64(total)*100,
					authHits, float64(authHits)/float64(total)*100,
					skipped, float64(skipped)/float64(total)*100)
			}
		}
	}

	// Memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapMB := float64(m.HeapAlloc) / (1024 * 1024)
	deltaMB := float64(int64(m.HeapAlloc)-int64(t.lastHeapBytes)) / (1024 * 1024)
	gcDelta := m.NumGC - t.lastGCCount
	t.lastHeapBytes = m.HeapAlloc
	t.lastGCCount = m.NumGC

	logger.Printf("debug-timing",
		"Heap=%.1f MB (d%+.1f MB) | GC=%d | Goroutines=%d",
		heapMB, deltaMB, gcDelta, runtime.NumGoroutine())

	// Pebble metrics
	if t.store != nil {
		t.store.LogMetrics()
	}

	// ChunkWriter diagnostics
	if t.diagProvider != nil {
		d := t.diagProvider.Diagnostics()
		if d.AllActionsMapSize > 0 || d.ContractActionMapSize > 0 {
			logger.Printf("debug-chunks",
				"Maps: all=%d ca=%d cw=%d | EstMem=%.0f MB",
				d.AllActionsMapSize, d.ContractActionMapSize, d.ContractWildcardMapSize,
				d.EstimatedMemoryMB)
		}
	}
}

func (t *SyncTiming) Reset() {
	t.processBlockNs.Store(0)
	t.processBlockCnt.Store(0)
	t.actionsCnt.Store(0)
	t.filterNs.Store(0)
	t.batchBuildNs.Store(0)
	t.blockTimeNs.Store(0)
	if t.FilterTiming != nil {
		t.FilterTiming.Reset()
	}
	t.indexAddNs.Store(0)
	t.flushNs.Store(0)
	t.flushCnt.Store(0)
	t.flushKeys.Store(0)
	t.commitSyncNs.Store(0)
	t.commitNoSyncNs.Store(0)
	t.commitSyncCnt.Store(0)
	t.commitNoSyncCnt.Store(0)
}
