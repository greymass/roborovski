package internal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type IndexType byte

const (
	IndexTypeAllActions IndexType = iota
	IndexTypeContractAction
	IndexTypeContractWildcard
)

type IndexKey struct {
	Type     IndexType
	Account  uint64
	Contract uint64
	Action   uint64
}

type ContractActionKey struct {
	Account  uint64
	Contract uint64
	Action   uint64
}

type ContractWildcardKey struct {
	Account  uint64
	Contract uint64
}

type partialEntry struct {
	partial *PartialChunk
}

type ChunkWriter struct {
	mu       sync.Mutex
	db       *pebble.DB
	metadata *ChunkMetadata
	timeMap  *TimeMapper

	allActions       map[uint64]*partialEntry
	contractAction   map[ContractActionKey]*partialEntry
	contractWildcard map[ContractWildcardKey]*partialEntry

	pendingBatch *pebble.Batch
	batchSize    int
	maxBatchSize int

	stats ChunkWriterStats
}

type ChunkWriterStats struct {
	ChunksWritten    uint64
	SequencesWritten uint64
	BatchesCommitted uint64
}

type ChunkWriterDiagnostics struct {
	AllActionsMapSize       int
	ContractActionMapSize   int
	ContractWildcardMapSize int

	// Distribution of partial chunk fill levels
	// Buckets: 0-9, 10-99, 100-999, 1000-9999, 10000 (full)
	AllActionsFillBuckets       [5]int
	ContractActionFillBuckets   [5]int
	ContractWildcardFillBuckets [5]int

	// Estimated memory usage
	EstimatedMemoryMB float64
}

func NewChunkWriter(db *pebble.DB, metadata *ChunkMetadata, timeMap *TimeMapper) *ChunkWriter {
	return &ChunkWriter{
		db:               db,
		metadata:         metadata,
		timeMap:          timeMap,
		allActions:       make(map[uint64]*partialEntry),
		contractAction:   make(map[ContractActionKey]*partialEntry),
		contractWildcard: make(map[ContractWildcardKey]*partialEntry),
		maxBatchSize:     10000,
	}
}

func (w *ChunkWriter) AddAllActions(account uint64, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := w.allActions[account]
	if entry == nil {
		chunkID := uint32(w.metadata.GetAllActionsChunkCount(account))
		entry = &partialEntry{
			partial: NewPartialChunk(chunkID),
		}
		w.allActions[account] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushAllActionsChunk(account, entry.partial)
		entry.partial.Reset(entry.partial.ChunkID + 1)
	}
}

func (w *ChunkWriter) AddContractAction(account, contract, action uint64, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := ContractActionKey{Account: account, Contract: contract, Action: action}
	entry := w.contractAction[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractAction[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractActionChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) AddContractWildcard(account, contract uint64, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := ContractWildcardKey{Account: account, Contract: contract}
	entry := w.contractWildcard[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractWildcard[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractWildcardChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) AddBulk(account, contract, action, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.addAllActionsLocked(account, seq)
	w.addContractActionLocked(account, contract, action, seq)
	w.addContractWildcardLocked(account, contract, seq)
}

func (w *ChunkWriter) AddBulkNoLock(account, contract, action, seq uint64) {
	w.addAllActionsNoLock(account, seq)
	w.addContractActionNoLock(account, contract, action, seq)
	w.addContractWildcardNoLock(account, contract, seq)
}

// AddBatchSorted is an optimized batch add that uses last-entry caching
// to skip redundant map lookups when consecutive actions involve the same account.
// Note: Sorting was tested but the O(n log n) overhead exceeded the cache benefits.
func (w *ChunkWriter) AddBatchSorted(actions []ActionEntry) {
	if len(actions) == 0 {
		return
	}

	// Last-entry cache for each index type - no sorting to preserve natural locality
	var lastAccount uint64
	var lastAllEntry *partialEntry

	var lastCAKey ContractActionKey
	var lastCAEntry *partialEntry

	var lastCWKey ContractWildcardKey
	var lastCWEntry *partialEntry

	for i := range actions {
		a := &actions[i]

		// === AllActions index ===
		if a.Account != lastAccount || lastAllEntry == nil {
			// Cache miss - need map lookup
			entry := w.allActions[a.Account]
			if entry == nil {
				chunkID := uint32(w.metadata.GetAllActionsChunkCountNoLock(a.Account))
				entry = &partialEntry{
					partial: NewPartialChunk(chunkID),
				}
				w.allActions[a.Account] = entry
			}
			lastAccount = a.Account
			lastAllEntry = entry
		}
		lastAllEntry.partial.Add(a.GlobalSeq)
		if lastAllEntry.partial.IsFull() {
			w.flushAllActionsChunk(a.Account, lastAllEntry.partial)
			lastAllEntry.partial.Reset(lastAllEntry.partial.ChunkID + 1)
		}

		// === ContractAction index ===
		caKey := ContractActionKey{Account: a.Account, Contract: a.Contract, Action: a.Action}
		if caKey != lastCAKey || lastCAEntry == nil {
			entry := w.contractAction[caKey]
			if entry == nil {
				entry = &partialEntry{
					partial: NewPartialChunk(0),
				}
				w.contractAction[caKey] = entry
			}
			lastCAKey = caKey
			lastCAEntry = entry
		}
		lastCAEntry.partial.Add(a.GlobalSeq)
		if lastCAEntry.partial.IsFull() {
			w.flushContractActionChunk(lastCAKey, lastCAEntry.partial)
			lastCAEntry.partial.Reset(0)
		}

		// === ContractWildcard index ===
		cwKey := ContractWildcardKey{Account: a.Account, Contract: a.Contract}
		if cwKey != lastCWKey || lastCWEntry == nil {
			entry := w.contractWildcard[cwKey]
			if entry == nil {
				entry = &partialEntry{
					partial: NewPartialChunk(0),
				}
				w.contractWildcard[cwKey] = entry
			}
			lastCWKey = cwKey
			lastCWEntry = entry
		}
		lastCWEntry.partial.Add(a.GlobalSeq)
		if lastCWEntry.partial.IsFull() {
			w.flushContractWildcardChunk(lastCWKey, lastCWEntry.partial)
			lastCWEntry.partial.Reset(0)
		}
	}
}

func (w *ChunkWriter) addAllActionsLocked(account uint64, seq uint64) {
	entry := w.allActions[account]
	if entry == nil {
		chunkID := uint32(w.metadata.GetAllActionsChunkCount(account))
		entry = &partialEntry{
			partial: NewPartialChunk(chunkID),
		}
		w.allActions[account] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushAllActionsChunk(account, entry.partial)
		entry.partial.Reset(entry.partial.ChunkID + 1)
	}
}

func (w *ChunkWriter) addAllActionsNoLock(account uint64, seq uint64) {
	entry := w.allActions[account]
	if entry == nil {
		chunkID := uint32(w.metadata.GetAllActionsChunkCountNoLock(account))
		entry = &partialEntry{
			partial: NewPartialChunk(chunkID),
		}
		w.allActions[account] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushAllActionsChunk(account, entry.partial)
		entry.partial.Reset(entry.partial.ChunkID + 1)
	}
}

func (w *ChunkWriter) addContractActionLocked(account, contract, action uint64, seq uint64) {
	key := ContractActionKey{Account: account, Contract: contract, Action: action}
	entry := w.contractAction[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractAction[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractActionChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) addContractActionNoLock(account, contract, action uint64, seq uint64) {
	key := ContractActionKey{Account: account, Contract: contract, Action: action}
	entry := w.contractAction[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractAction[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractActionChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) addContractWildcardLocked(account, contract uint64, seq uint64) {
	key := ContractWildcardKey{Account: account, Contract: contract}
	entry := w.contractWildcard[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractWildcard[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractWildcardChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) addContractWildcardNoLock(account, contract uint64, seq uint64) {
	key := ContractWildcardKey{Account: account, Contract: contract}
	entry := w.contractWildcard[key]
	if entry == nil {
		entry = &partialEntry{
			partial: NewPartialChunk(0),
		}
		w.contractWildcard[key] = entry
	}

	entry.partial.Add(seq)

	if entry.partial.IsFull() {
		w.flushContractWildcardChunk(key, entry.partial)
		entry.partial.Reset(0)
	}
}

func (w *ChunkWriter) flushAllActionsChunk(account uint64, p *PartialChunk) {
	if p.Len() == 0 {
		return
	}

	legacyEncoded, err := p.Encode()
	if err != nil {
		return
	}

	leanEncoded, err := EncodeLeanChunk(p.BaseSeq, p.Seqs)
	if err != nil {
		return
	}

	legacyKey := makeLegacyAccountActionsKey(account, p.ChunkID)
	newKey := makeAccountActionsKey(account, p.BaseSeq)

	w.ensureBatch()
	w.pendingBatch.Set(legacyKey, legacyEncoded, nil)
	w.pendingBatch.Set(newKey, leanEncoded, nil)
	w.batchSize += 2

	w.metadata.AddAllActionsChunk(account, p.BaseSeq)
	w.stats.ChunksWritten++
	w.stats.SequencesWritten += uint64(p.Len())

	w.maybeCommitBatch()
}

func (w *ChunkWriter) flushContractActionChunk(k ContractActionKey, p *PartialChunk) {
	if p.Len() == 0 {
		return
	}

	encoded, err := EncodeLeanChunk(p.BaseSeq, p.Seqs)
	if err != nil {
		return
	}

	key := makeContractActionKey(k.Account, k.Contract, k.Action, p.BaseSeq)
	w.ensureBatch()
	w.pendingBatch.Set(key, encoded, nil)
	w.batchSize++

	w.stats.ChunksWritten++
	w.stats.SequencesWritten += uint64(p.Len())

	w.maybeCommitBatch()
}

func (w *ChunkWriter) flushContractWildcardChunk(k ContractWildcardKey, p *PartialChunk) {
	if p.Len() == 0 {
		return
	}

	encoded, err := EncodeLeanChunk(p.BaseSeq, p.Seqs)
	if err != nil {
		return
	}

	key := makeContractWildcardKey(k.Account, k.Contract, p.BaseSeq)
	w.ensureBatch()
	w.pendingBatch.Set(key, encoded, nil)
	w.batchSize++

	w.stats.ChunksWritten++
	w.stats.SequencesWritten += uint64(p.Len())

	w.maybeCommitBatch()
}

func (w *ChunkWriter) ensureBatch() {
	if w.pendingBatch == nil {
		w.pendingBatch = w.db.NewBatch()
	}
}

func (w *ChunkWriter) maybeCommitBatch() {
	if w.batchSize >= w.maxBatchSize {
		w.commitBatchLocked()
	}
}

func (w *ChunkWriter) commitBatchLocked() {
	if w.pendingBatch == nil || w.batchSize == 0 {
		return
	}

	w.pendingBatch.Commit(pebble.NoSync)
	w.pendingBatch.Close()
	w.pendingBatch = nil
	w.batchSize = 0
	w.stats.BatchesCommitted++
}

func (w *ChunkWriter) FlushAllPartials() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for account, entry := range w.allActions {
		if entry.partial.Len() > 0 {
			w.flushAllActionsChunk(account, entry.partial)
			entry.partial.Reset(entry.partial.ChunkID + 1)
		}
	}

	for key, entry := range w.contractAction {
		if entry.partial.Len() > 0 {
			w.flushContractActionChunk(key, entry.partial)
			entry.partial.Reset(entry.partial.ChunkID + 1)
		}
	}

	for key, entry := range w.contractWildcard {
		if entry.partial.Len() > 0 {
			w.flushContractWildcardChunk(key, entry.partial)
			entry.partial.Reset(entry.partial.ChunkID + 1)
		}
	}

	w.commitBatchLocked()

	return nil
}

func (w *ChunkWriter) FlushStalePartials(maxAge time.Duration) error {
	return w.FlushAllPartials()
}

func (w *ChunkWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.commitBatchLocked()

	return w.db.Flush()
}

func (w *ChunkWriter) CommitWithProperties(libNum, headNum uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.ensureBatch()

	propKey := []byte{PrefixProperties}
	propVal := make([]byte, 8)
	putUint32(propVal[0:4], libNum)
	putUint32(propVal[4:8], headNum)
	w.pendingBatch.Set(propKey, propVal, nil)

	if w.timeMap != nil && w.timeMap.DirtyCount() > 0 {
		w.timeMap.FlushToBatch(w.pendingBatch)
	}

	if err := w.pendingBatch.Commit(pebble.Sync); err != nil {
		return err
	}

	w.pendingBatch.Close()
	w.pendingBatch = nil
	w.batchSize = 0
	w.stats.BatchesCommitted++

	return nil
}

func (w *ChunkWriter) Stats() ChunkWriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *ChunkWriter) PendingCounts() (allActions, contractAction, contractWildcard int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range w.allActions {
		allActions += entry.partial.Len()
	}
	for _, entry := range w.contractAction {
		contractAction += entry.partial.Len()
	}
	for _, entry := range w.contractWildcard {
		contractWildcard += entry.partial.Len()
	}
	return
}

func (w *ChunkWriter) Diagnostics() (d ChunkWriterDiagnostics) {
	defer func() {
		if r := recover(); r != nil {
			d = ChunkWriterDiagnostics{}
		}
	}()

	d.AllActionsMapSize = len(w.allActions)
	d.ContractActionMapSize = len(w.contractAction)
	d.ContractWildcardMapSize = len(w.contractWildcard)
	totalEntries := d.AllActionsMapSize + d.ContractActionMapSize + d.ContractWildcardMapSize
	d.EstimatedMemoryMB = float64(totalEntries*InitialChunkCapacity*8) / (1024 * 1024)

	return d
}

func (w *ChunkWriter) ClearPartials() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.allActions = make(map[uint64]*partialEntry)
	w.contractAction = make(map[ContractActionKey]*partialEntry)
	w.contractWildcard = make(map[ContractWildcardKey]*partialEntry)
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
