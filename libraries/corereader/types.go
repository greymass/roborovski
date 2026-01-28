package corereader

// ActionFilterFunc filters actions by contract and action name.
// Return true to EXCLUDE the action, false to include it.
type ActionFilterFunc func(contract, action uint64) bool

// ActionMetadata contains minimal action info for filtering decisions.
type ActionMetadata struct {
	GlobalSeq uint64
	Contract  uint64
	Action    uint64
}

// CanonicalAction represents a raw parsed action from block data.
// Contains ordinals, auth indexes, and other raw fields from the trace.
type CanonicalAction struct {
	ActionOrdinal      uint32
	CreatorAO          uint32
	ReceiverUint64     uint64
	DataIndex          uint32
	AuthAccountIndexes []uint32
	GlobalSeqUint64    uint64
	TrxIndex           uint32
	ContractUint64     uint64
	ActionUint64       uint64
}

// RawBlock contains unfiltered block data as read from storage.
// This is the input to the canonical filtering process.
type RawBlock struct {
	BlockNum      uint32
	BlockTime     uint32
	Notifications map[uint64][]uint64
	ActionMeta    []ActionMetadata
	Actions       []CanonicalAction
	NamesInBlock  []uint64

	rawData     []byte
	dataOffsets []uint32
	dataLengths []uint32
}

func (r *RawBlock) GetActionData(dataIndex uint32) []byte {
	if r.rawData == nil || int(dataIndex) >= len(r.dataOffsets) {
		return nil
	}
	off := r.dataOffsets[dataIndex]
	length := r.dataLengths[dataIndex]
	return r.rawData[off : off+length]
}

func (r *RawBlock) HasActionData() bool {
	return r.rawData != nil
}

// Action represents a canonically-deduplicated action for a specific account.
// This is the output of canonical filtering - one Action per (account, globalSeq) pair.
type Action struct {
	Account      uint64
	Contract     uint64
	Action       uint64
	GlobalSeq    uint64
	TrxIndex     uint32
	IsAuthorizer bool
}

// ContractExecution represents a contract execution (receiver == contract).
// These are tracked separately as they represent the primary action execution.
type ContractExecution struct {
	Contract  uint64
	Action    uint64
	GlobalSeq uint64
	TrxIndex  uint32
}

// Block holds a parsed and filtered block with its actions and executions.
// This is the result of filtering a RawBlock.
type Block struct {
	BlockNum   uint32
	BlockTime  uint32
	Actions    []Action
	Executions []ContractExecution
	MinSeq     uint64
	MaxSeq     uint64

	rawBlock *RawBlock
}

func (b *Block) GetActionDataBySeq(globalSeq uint64) []byte {
	if b.rawBlock == nil || !b.rawBlock.HasActionData() {
		return nil
	}
	for i := range b.rawBlock.Actions {
		if b.rawBlock.Actions[i].GlobalSeqUint64 == globalSeq {
			return b.rawBlock.GetActionData(b.rawBlock.Actions[i].DataIndex)
		}
	}
	return nil
}

func (b *Block) HasActionData() bool {
	return b.rawBlock != nil && b.rawBlock.HasActionData()
}

func (b *Block) SetRawBlock(raw *RawBlock) {
	b.rawBlock = raw
}

// Processor handles single-block processing during sync.
type Processor interface {
	ProcessBlock(block Block) error
	ShouldCommit(blocksProcessed int) bool
	Commit(currentBlock uint32, bulkMode bool) error
	Flush() error
}

// BatchProcessor extends Processor for bulk block processing.
type BatchProcessor interface {
	Processor
	ProcessBatch(blocks []Block) error
}

// BlockTransactionIDs holds transaction IDs for a specific block.
// Used by TransactionProcessor for services that only need transaction IDs.
type BlockTransactionIDs struct {
	BlockNum uint32
	TrxIDs   [][32]byte
}

// TransactionProcessor handles transaction ID extraction during sync.
// This is a lighter-weight alternative to Processor for services that
// only need transaction IDs (not full action data).
type TransactionProcessor interface {
	ProcessBlock(block BlockTransactionIDs) error
	ShouldCommit(blocksProcessed int) bool
	Commit(currentBlock uint32, bulkMode bool) error
	Flush() error
}

// BatchTransactionProcessor extends TransactionProcessor for bulk processing.
type BatchTransactionProcessor interface {
	TransactionProcessor
	ProcessBatch(blocks []BlockTransactionIDs) error
}
