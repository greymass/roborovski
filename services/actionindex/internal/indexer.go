package internal

import "github.com/greymass/roborovski/libraries/querytrace"

type ActionIndexer interface {
	Add(account, contract, action, globalSeq uint64, blockTime uint32)
	AddBatch(actions []ActionEntry)
	RecordBlockTime(blockTime uint32, minSeq, maxSeq uint64)
	Commit(libNum, headNum uint32) error
	CommitNoSync(libNum, headNum uint32) error
	CommitWithTiming(libNum, headNum uint32, sync bool, timing *SyncTiming) error
	GetProperties() (libNum, headNum uint32, err error)
	MaxSeq() uint64
	SetBulkMode(bulkMode bool)
	IsBulkMode() bool
	SetTiming(timing *SyncTiming)
	LoadActions(account uint64, contract, action string, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error)
	LoadActionsFromCursor(account uint64, contract, action string, cursorSeq uint64, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error)
	LoadActionsFromCursorWithDateRange(account uint64, contract, action string, cursorSeq uint64, dateRange *DateRange, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error)
	LoadActionsWithDateRange(account uint64, contract, action string, dateRange *DateRange, limit int, descending bool, trace *querytrace.Tracer) ([]uint64, error)
	Close() error
}

type DiagnosticsProvider interface {
	Diagnostics() ChunkWriterDiagnostics
}
