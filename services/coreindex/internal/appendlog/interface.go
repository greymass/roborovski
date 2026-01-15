package appendlog

// BlockEntry represents a block to be appended in batch operations
type BlockEntry struct {
	BlockNum      uint32
	Data          []byte
	GlobMin       uint64
	GlobMax       uint64
	PreCompressed bool // If true, Data is already compressed (skip compression in store)
}

// StoreInterface defines the common interface for both Store and SliceStore
// This allows the main application to use either storage backend transparently
//
// Note: All write operations skip glob index building for performance.
// Glob indices are built in the background or during recovery via BuildGlobIndex().
type StoreInterface interface {
	// Write operations
	// liveMode: if true, immediately write indexes to disk after each block (for live sync)
	AppendBlock(blockNum uint32, data []byte, globMin, globMax uint64, liveMode bool) error // Write single block
	AppendBlockBatch(blocks []BlockEntry) error                                             // Write multiple blocks with single fsync

	// Read operations
	GetBlock(blockNum uint32) ([]byte, error)
	GetBlockByGlob(glob uint64) (uint32, error)

	// Metadata
	GetLIB() uint32
	GetHead() uint32
	GetCacheStats() (hits, misses uint64)
	GetCacheSize() int

	// Index operations
	BuildGlobIndex() (int, error) // Build glob index from block metadata (used during recovery)

	// Validation operations (for reactive validation during sync)
	ValidateAndRepairLastSlices(numSlices int) error // Validate and repair last N slices

	// Persistence
	Flush() error
	Sync() error
	Close() error
}

var _ StoreInterface = (*SliceStore)(nil)
