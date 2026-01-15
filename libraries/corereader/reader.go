package corereader

import (
	"time"

	"github.com/greymass/roborovski/libraries/chain"
)

// FetchTimings contains detailed timing breakdown for GetActionsByGlobalSeqs
type FetchTimings struct {
	Total             time.Duration
	Phase1            time.Duration // Slice/block lookup (total)
	Phase1a           time.Duration // 1a: Find slices via globRangeIndex
	Phase1b           time.Duration // 1b: Parallel slice reader initialization
	Phase1c           time.Duration // 1c: Binary search block lookups
	Phase1DiskLookups int           // Number of disk-based slice lookups (vs range index)
	Phase2            time.Duration // Block reading and parsing
	DecompressTime    time.Duration
	DecompressCount   int
	ParseTime         time.Duration
	ParseCount        int
	MatchTime         time.Duration
	CacheHits         int
	CacheMisses       int
	TotalBytes        int64
	BlockCount        int
	SliceCount        int
}

type Reader interface {
	GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error)
	GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error)
	GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error)

	GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error)
	GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error)

	GetNextBatch(maxBlocks int, filterFunc ActionFilterFunc) ([]RawBlock, error)
	SetCurrentBlock(block uint32)
	CurrentBlock() uint32

	GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error)
	GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error)

	GetStateProps(bypassCache bool) (head uint32, lib uint32, err error)
	FindFirstAvailableBlock(afterBlock uint32) (uint32, bool)

	GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int)

	GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error)
	GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error)
	GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error)

	Close() error
}

type BaseReader interface {
	GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error)
	GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error)
	GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error)
	GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error)
	GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error)
	GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error)
	GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error)
	GetStateProps(bypassCache bool) (head uint32, lib uint32, err error)
	FindFirstAvailableBlock(afterBlock uint32) (uint32, bool)
	GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int)
	GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error)
	GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error)
	GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error)
	Close() error
}

var _ BaseReader = (*SliceReader)(nil)
var _ BaseReader = (*HTTPReader)(nil)
var _ BaseReader = (*StreamReader)(nil)

type StreamingReader interface {
	IsStreaming() bool
}
