package internal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
)

type ActionEntry struct {
	Account   uint64
	Contract  uint64
	Action    uint64
	GlobalSeq uint64
	BlockTime uint32
}

type Indexes struct {
	mu sync.RWMutex
	db *pebble.DB

	timeMapper  *TimeMapper
	chunkWriter *ChunkWriter
	chunkReader *ChunkReader

	walIndex     *WALIndex
	walWriter    *WALWriter
	walCompactor *WALCompactor
	walReader    *WALReader

	bulkBuffer *Buffer

	bulkMode bool
	dataDir  string
	timing   *SyncTiming

	cachedLibNum  atomic.Uint32
	cachedHeadNum atomic.Uint32
}

func NewIndexes(db *pebble.DB, dataDir string) (*Indexes, error) {
	timeMapper := NewTimeMapper()

	if err := timeMapper.LoadFromDB(db); err != nil {
		logger.Printf("startup", "Warning: failed to load time mapper: %v", err)
	}

	minHour, maxHour := timeMapper.HourRange()
	logger.Printf("startup", "TimeMapper loaded: %d entries, hour range %d-%d", timeMapper.Len(), minHour, maxHour)

	walIndex := NewWALIndex()
	if err := walIndex.LoadFromDB(db); err != nil {
		logger.Printf("startup", "Warning: failed to load WAL index: %v", err)
	}

	chunkWriter := NewChunkWriter(db, timeMapper)
	walWriter := NewWALWriter(db, walIndex)
	walReader := NewWALReader(walIndex)
	chunkReader := NewChunkReader(db, walReader)
	walCompactor := NewWALCompactor(db, walIndex, chunkWriter)

	idx := &Indexes{
		db:           db,
		timeMapper:   timeMapper,
		chunkWriter:  chunkWriter,
		chunkReader:  chunkReader,
		walIndex:     walIndex,
		walWriter:    walWriter,
		walCompactor: walCompactor,
		walReader:    walReader,
		bulkBuffer:   NewBuffer(db),
		bulkMode:     true,
		dataDir:      dataDir,
	}

	propKey := makePropertiesKey()
	if val, closer, err := db.Get(propKey); err == nil {
		if libNum, headNum, ok := parsePropertiesValue(val); ok {
			idx.cachedLibNum.Store(libNum)
			idx.cachedHeadNum.Store(headNum)
		}
		closer.Close()
	}

	return idx, nil
}

func (idx *Indexes) SetBulkMode(bulkMode bool) {
	idx.mu.Lock()
	wasBulk := idx.bulkMode
	idx.bulkMode = bulkMode
	idx.mu.Unlock()

	if wasBulk && !bulkMode {
		idx.flushBulkBuffer()
		idx.walCompactor.Start(5 * time.Second)
	} else if !wasBulk && bulkMode {
		idx.walCompactor.Stop()
	}
}

func (idx *Indexes) SetTiming(timing *SyncTiming) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.timing = timing
}

func (idx *Indexes) flushBulkBuffer() error {
	if idx.bulkBuffer == nil {
		return nil
	}
	return idx.bulkBuffer.WaitForMerge()
}

func (idx *Indexes) IsBulkMode() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.bulkMode
}

func (idx *Indexes) Add(account, contract, action, globalSeq uint64, blockTime uint32) {
	idx.mu.RLock()
	bulkMode := idx.bulkMode
	idx.mu.RUnlock()

	if bulkMode {
		idx.bulkBuffer.Add([]ActionEntry{{
			Account:   account,
			Contract:  contract,
			Action:    action,
			GlobalSeq: globalSeq,
		}})
	} else {
		idx.walWriter.Add(globalSeq, account, contract, action)
	}
}

func (idx *Indexes) AddBatch(actions []ActionEntry) {
	if len(actions) == 0 {
		return
	}

	idx.mu.RLock()
	bulkMode := idx.bulkMode
	idx.mu.RUnlock()

	if bulkMode {
		idx.bulkBuffer.Add(actions)
	} else {
		for i := range actions {
			a := &actions[i]
			idx.walWriter.Add(a.GlobalSeq, a.Account, a.Contract, a.Action)
		}
	}
}

func (idx *Indexes) RecordBlockTime(blockTime uint32, minSeq, maxSeq uint64) {
	idx.timeMapper.Record(blockTime, minSeq, maxSeq)
}

func (idx *Indexes) Commit(libNum, headNum uint32) error {
	return idx.CommitWithSync(libNum, headNum, true)
}

func (idx *Indexes) CommitNoSync(libNum, headNum uint32) error {
	return idx.CommitWithSync(libNum, headNum, false)
}

func (idx *Indexes) CommitWithSync(libNum, headNum uint32, sync bool) error {
	idx.mu.RLock()
	bulkMode := idx.bulkMode
	idx.mu.RUnlock()

	if bulkMode {
		stats, err := idx.bulkBuffer.SwapAndMergeSync()
		if err != nil {
			return fmt.Errorf("bulk buffer merge: %w", err)
		}

		if idx.timing != nil && stats.EntriesProcessed > 0 {
			stats.LogStats()
		}

		batch := idx.db.NewBatch()
		propKey := makePropertiesKey()
		propVal := makePropertiesValue(libNum, headNum)
		batch.Set(propKey, propVal, nil)

		if idx.timeMapper.DirtyCount() > 0 {
			idx.timeMapper.FlushToBatch(batch)
		}

		var opts pebble.WriteOptions
		if sync {
			opts.Sync = true
		}
		if err := batch.Commit(&opts); err != nil {
			batch.Close()
			return err
		}
		batch.Close()

		idx.cachedLibNum.Store(libNum)
		idx.cachedHeadNum.Store(headNum)
		return nil
	}

	batch := idx.db.NewBatch()

	if err := idx.walWriter.FlushToBatch(batch); err != nil {
		batch.Close()
		return err
	}

	propKey := makePropertiesKey()
	propVal := makePropertiesValue(libNum, headNum)
	batch.Set(propKey, propVal, nil)

	if idx.timeMapper.DirtyCount() > 0 {
		idx.timeMapper.FlushToBatch(batch)
	}

	var opts pebble.WriteOptions
	if sync {
		opts.Sync = true
	}
	if err := batch.Commit(&opts); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	idx.walWriter.CommitPending()

	idx.cachedLibNum.Store(libNum)
	idx.cachedHeadNum.Store(headNum)
	return nil
}

func (idx *Indexes) CommitWithTiming(libNum, headNum uint32, sync bool, timing *SyncTiming) error {
	var start time.Time
	if timing != nil {
		start = time.Now()
	}

	err := idx.CommitWithSync(libNum, headNum, sync)

	if timing != nil {
		timing.RecordCommit(time.Since(start), sync)
	}

	return err
}

func (idx *Indexes) GetProperties() (libNum, headNum uint32, err error) {
	return idx.cachedLibNum.Load(), idx.cachedHeadNum.Load(), nil
}

func (idx *Indexes) MaxSeq() uint64 {
	return idx.timeMapper.MaxSeq()
}

func (idx *Indexes) GetPropertiesFromDB() (libNum, headNum uint32, err error) {
	propKey := makePropertiesKey()
	val, closer, err := idx.db.Get(propKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer closer.Close()

	libNum, headNum, ok := parsePropertiesValue(val)
	if !ok {
		return 0, 0, fmt.Errorf("corrupt properties value")
	}
	return libNum, headNum, nil
}

func (idx *Indexes) LoadActions(account uint64, contract, action string, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error) {
	hasContract := contract != ""
	hasAction := action != ""

	var start time.Time
	if trace.Enabled() {
		start = time.Now()
	}

	var seqs []uint64
	var err error

	if hasContract && hasAction {
		contractName := chain.StringToName(contract)
		actionName := chain.StringToName(action)
		if descending {
			seqs, err = idx.chunkReader.GetContractActionLastN(account, contractName, actionName, limit)
		} else {
			seqs, err = idx.chunkReader.GetContractActionFirstN(account, contractName, actionName, limit)
		}
		if trace.Enabled() {
			trace.AddStepWithCount("ContractAction", "chunk-read", time.Since(start), len(seqs),
				fmt.Sprintf("%s::%s", contract, action))
		}
	} else if hasContract {
		contractName := chain.StringToName(contract)
		if descending {
			seqs, err = idx.chunkReader.GetContractWildcardLastN(account, contractName, limit)
		} else {
			seqs, err = idx.chunkReader.GetContractWildcardFirstN(account, contractName, limit)
		}
		if trace.Enabled() {
			trace.AddStepWithCount("ContractWildcard", "chunk-read", time.Since(start), len(seqs),
				fmt.Sprintf("%s::*", contract))
		}
	} else {
		if descending {
			seqs, err = idx.chunkReader.GetLastN(account, limit)
		} else {
			seqs, err = idx.chunkReader.GetFirstN(account, limit)
		}
		if trace.Enabled() {
			trace.AddStepWithCount("AllActions", "chunk-read", time.Since(start), len(seqs), "")
		}
	}

	if err != nil {
		return nil, err
	}

	// GetLastN returns sequences in ascending order, reverse for descending
	if descending && len(seqs) > 1 {
		for i, j := 0, len(seqs)-1; i < j; i, j = i+1, j-1 {
			seqs[i], seqs[j] = seqs[j], seqs[i]
		}
	}

	return seqs, nil
}

func (idx *Indexes) LoadActionsFromCursor(account uint64, contract, action string, cursorSeq uint64, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error) {
	return idx.LoadActionsFromCursorWithDateRange(account, contract, action, cursorSeq, nil, limit, descending, trace)
}

func (idx *Indexes) LoadActionsFromCursorWithDateRange(account uint64, contract, action string, cursorSeq uint64, dateRange *DateRange, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error) {
	hasContract := contract != ""
	hasAction := action != ""

	var start time.Time
	if trace.Enabled() {
		start = time.Now()
	}

	var minSeq, maxSeq uint64
	if dateRange != nil {
		startHour := dateHourToHourNum(dateRange.StartDateHour)
		endHour := dateHourToHourNum(dateRange.EndDateHour)
		minSeq, maxSeq = idx.timeMapper.GetSeqRangeForHours(startHour, endHour)

		if trace.Enabled() {
			minHour, maxHour := idx.timeMapper.HourRange()
			trace.AddStepWithCount("TimeMap", "lookup", time.Since(start), idx.timeMapper.Len(),
				fmt.Sprintf("queryHours=[%d,%d] dataHours=[%d,%d] seqRange=[%d,%d]",
					startHour, endHour, minHour, maxHour, minSeq, maxSeq))
			start = time.Now()
		}

		if minSeq == 0 && maxSeq == 0 {
			return []uint64{}, nil
		}
	}

	var seqs []uint64
	var err error

	if hasContract && hasAction {
		contractName := chain.StringToName(contract)
		actionName := chain.StringToName(action)
		seqs, err = idx.chunkReader.GetContractActionFromCursorWithSeqRange(account, contractName, actionName, cursorSeq, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			detail := fmt.Sprintf("%s::%s cursor=%d", contract, action, cursorSeq)
			if dateRange != nil {
				detail += fmt.Sprintf(" seqRange=[%d,%d]", minSeq, maxSeq)
			}
			trace.AddStepWithCount("ContractAction", "cursor-read", time.Since(start), len(seqs), detail)
		}
	} else if hasContract {
		contractName := chain.StringToName(contract)
		seqs, err = idx.chunkReader.GetContractWildcardFromCursorWithSeqRange(account, contractName, cursorSeq, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			detail := fmt.Sprintf("%s::* cursor=%d", contract, cursorSeq)
			if dateRange != nil {
				detail += fmt.Sprintf(" seqRange=[%d,%d]", minSeq, maxSeq)
			}
			trace.AddStepWithCount("ContractWildcard", "cursor-read", time.Since(start), len(seqs), detail)
		}
	} else {
		seqs, err = idx.chunkReader.GetFromCursorWithSeqRange(account, cursorSeq, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			detail := fmt.Sprintf("cursor=%d", cursorSeq)
			if dateRange != nil {
				detail += fmt.Sprintf(" seqRange=[%d,%d]", minSeq, maxSeq)
			}
			trace.AddStepWithCount("AllActions", "cursor-read", time.Since(start), len(seqs), detail)
		}
	}

	return seqs, err
}

func (idx *Indexes) LoadActionsWithDateRange(account uint64, contract, action string, dateRange *DateRange, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error) {
	hasContract := contract != ""
	hasAction := action != ""

	var start time.Time
	if trace.Enabled() {
		start = time.Now()
	}

	startHour := dateHourToHourNum(dateRange.StartDateHour)
	endHour := dateHourToHourNum(dateRange.EndDateHour)

	minSeq, maxSeq := idx.timeMapper.GetSeqRangeForHours(startHour, endHour)

	if trace.Enabled() {
		minHour, maxHour := idx.timeMapper.HourRange()
		trace.AddStepWithCount("TimeMap", "lookup", time.Since(start), idx.timeMapper.Len(),
			fmt.Sprintf("queryHours=[%d,%d] dataHours=[%d,%d] seqRange=[%d,%d]",
				startHour, endHour, minHour, maxHour, minSeq, maxSeq))
	}

	if minSeq == 0 && maxSeq == 0 {
		return []uint64{}, nil
	}

	var seqs []uint64
	var err error

	if hasContract && hasAction {
		contractName := chain.StringToName(contract)
		actionName := chain.StringToName(action)
		seqs, err = idx.chunkReader.GetContractActionWithSeqRange(account, contractName, actionName, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			trace.AddStepWithCount("ContractAction+DateRange", "chunk-read", time.Since(start), len(seqs),
				fmt.Sprintf("%s::%s seqRange=[%d,%d]", contract, action, minSeq, maxSeq))
		}
	} else if hasContract {
		contractName := chain.StringToName(contract)
		seqs, err = idx.chunkReader.GetContractWildcardWithSeqRange(account, contractName, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			trace.AddStepWithCount("ContractWildcard+DateRange", "chunk-read", time.Since(start), len(seqs),
				fmt.Sprintf("%s::* seqRange=[%d,%d]", contract, minSeq, maxSeq))
		}
	} else {
		seqs, err = idx.chunkReader.GetWithSeqRange(account, minSeq, maxSeq, limit, descending)
		if trace.Enabled() {
			trace.AddStepWithCount("DateRange", "chunk-read", time.Since(start), len(seqs),
				fmt.Sprintf("query=[%d,%d]", minSeq, maxSeq))
		}
	}

	return seqs, err
}

func (idx *Indexes) Close() error {
	idx.walCompactor.Stop()

	if err := idx.flushBulkBuffer(); err != nil {
		return err
	}

	if idx.bulkBuffer != nil {
		if err := idx.bulkBuffer.Close(); err != nil {
			return err
		}
	}

	if err := idx.chunkWriter.FlushAllPartials(); err != nil {
		return err
	}
	if err := idx.chunkWriter.Sync(); err != nil {
		return err
	}

	return nil
}

func (idx *Indexes) Stats() (chunks, sequences uint64) {
	stats := idx.chunkWriter.Stats()
	return stats.ChunksWritten, stats.SequencesWritten
}

func (idx *Indexes) Diagnostics() ChunkWriterDiagnostics {
	return idx.chunkWriter.Diagnostics()
}

func (idx *Indexes) HasAccount(account uint64) bool {
	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)
	iter, err := idx.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return false
	}
	defer iter.Close()
	return iter.First()
}

func dateHourToHourNum(dateHour []byte) uint32 {
	if len(dateHour) < 3 {
		return 0
	}
	encoded := uint32(dateHour[0])<<16 | uint32(dateHour[1])<<8 | uint32(dateHour[2])
	days := encoded >> 5
	hour := encoded & 0x1F

	epochHours := uint32(antelopeEpoch.Unix() / 3600)
	return epochHours + days*24 + hour
}
