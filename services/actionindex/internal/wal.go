package internal

import (
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/logger"
)

// walEntryKey uniquely identifies a WAL entry by account and globalSeq.
// The same action (globalSeq) can be indexed for multiple accounts
// (e.g., sender and receiver of a transfer).
type walEntryKey struct {
	Account   uint64
	GlobalSeq uint64
}

// WALIndex maintains an in-memory index of WAL entries for O(1) lookups.
// This avoids expensive Pebble iterator seeks (~90ms) on every query.
// The index is populated at startup and kept in sync by WALWriter/WALCompactor.
type WALIndex struct {
	mu sync.RWMutex

	// Index by account for AllActions queries
	byAccount map[uint64][]uint64

	// Index by (account, contract, action) for ContractAction queries
	byContractAction map[ContractActionKey][]uint64

	// Index by (account, contract) for ContractWildcard queries
	byContractWildcard map[ContractWildcardKey][]uint64

	// Full entry data needed for compaction, keyed by (account, globalSeq)
	entries map[walEntryKey]WALEntry
}

func NewWALIndex() *WALIndex {
	return &WALIndex{
		byAccount:          make(map[uint64][]uint64),
		byContractAction:   make(map[ContractActionKey][]uint64),
		byContractWildcard: make(map[ContractWildcardKey][]uint64),
		entries:            make(map[walEntryKey]WALEntry),
	}
}

// LoadFromDB populates the index by scanning WAL entries from Pebble.
// Called once at startup.
func (idx *WALIndex) LoadFromDB(db *pebble.DB) error {
	t0 := time.Now()
	prefix := []byte{PrefixWAL}
	upperBound := []byte{PrefixWAL + 1}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	var count int
	for iter.First(); iter.Valid(); iter.Next() {
		globalSeq, keyAccount, ok := parseWALKey(iter.Key())
		if !ok {
			continue
		}

		account, contract, action, ok := parseWALValue(iter.Value())
		if !ok {
			continue
		}

		// Use account from key if available (new format), otherwise from value (legacy)
		if keyAccount != 0 {
			account = keyAccount
		}

		idx.addLocked(WALEntry{
			GlobalSeq: globalSeq,
			Account:   account,
			Contract:  contract,
			Action:    action,
		})
		count++
	}

	logger.Printf("startup", "WALIndex loaded %d entries in %v", count, time.Since(t0))
	return iter.Error()
}

// Add inserts a new WAL entry into all indexes.
func (idx *WALIndex) Add(e WALEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.addLocked(e)
}

func (idx *WALIndex) addLocked(e WALEntry) {
	key := walEntryKey{Account: e.Account, GlobalSeq: e.GlobalSeq}
	if _, exists := idx.entries[key]; exists {
		return // Already indexed for this account
	}

	idx.entries[key] = e
	idx.byAccount[e.Account] = append(idx.byAccount[e.Account], e.GlobalSeq)

	caKey := ContractActionKey{Account: e.Account, Contract: e.Contract, Action: e.Action}
	idx.byContractAction[caKey] = append(idx.byContractAction[caKey], e.GlobalSeq)

	cwKey := ContractWildcardKey{Account: e.Account, Contract: e.Contract}
	idx.byContractWildcard[cwKey] = append(idx.byContractWildcard[cwKey], e.GlobalSeq)
}

// RemoveEntries deletes WAL entries by their keys from all indexes.
func (idx *WALIndex) RemoveEntries(entries []WALEntry) {
	if len(entries) == 0 {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Build a map of seqs to remove per account for efficient filtering
	toRemoveByAccount := make(map[uint64]map[uint64]struct{})

	// Collect affected keys for each index
	affectedAccounts := make(map[uint64]struct{})
	affectedCA := make(map[ContractActionKey]struct{})
	affectedCW := make(map[ContractWildcardKey]struct{})

	for _, e := range entries {
		key := walEntryKey{Account: e.Account, GlobalSeq: e.GlobalSeq}
		if _, exists := idx.entries[key]; !exists {
			continue
		}
		delete(idx.entries, key)

		if toRemoveByAccount[e.Account] == nil {
			toRemoveByAccount[e.Account] = make(map[uint64]struct{})
		}
		toRemoveByAccount[e.Account][e.GlobalSeq] = struct{}{}

		affectedAccounts[e.Account] = struct{}{}
		affectedCA[ContractActionKey{Account: e.Account, Contract: e.Contract, Action: e.Action}] = struct{}{}
		affectedCW[ContractWildcardKey{Account: e.Account, Contract: e.Contract}] = struct{}{}
	}

	// Rebuild affected slices by filtering out removed seqs
	for account := range affectedAccounts {
		toRemove := toRemoveByAccount[account]
		idx.byAccount[account] = filterSeqs(idx.byAccount[account], toRemove)
		if len(idx.byAccount[account]) == 0 {
			delete(idx.byAccount, account)
		}
	}

	for key := range affectedCA {
		toRemove := toRemoveByAccount[key.Account]
		idx.byContractAction[key] = filterSeqs(idx.byContractAction[key], toRemove)
		if len(idx.byContractAction[key]) == 0 {
			delete(idx.byContractAction, key)
		}
	}

	for key := range affectedCW {
		toRemove := toRemoveByAccount[key.Account]
		idx.byContractWildcard[key] = filterSeqs(idx.byContractWildcard[key], toRemove)
		if len(idx.byContractWildcard[key]) == 0 {
			delete(idx.byContractWildcard, key)
		}
	}
}

// filterSeqs returns a new slice with entries in toRemove filtered out.
func filterSeqs(seqs []uint64, toRemove map[uint64]struct{}) []uint64 {
	result := seqs[:0] // Reuse backing array
	for _, s := range seqs {
		if _, remove := toRemove[s]; !remove {
			result = append(result, s)
		}
	}
	return result
}

// GetEntriesForAccount returns globalSeqs for an account (sorted).
func (idx *WALIndex) GetEntriesForAccount(account uint64) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	seqs := idx.byAccount[account]
	if len(seqs) == 0 {
		return nil
	}

	result := make([]uint64, len(seqs))
	copy(result, seqs)
	return result
}

// GetEntriesForContractAction returns globalSeqs for account+contract+action.
func (idx *WALIndex) GetEntriesForContractAction(account, contract, action uint64) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := ContractActionKey{Account: account, Contract: contract, Action: action}
	seqs := idx.byContractAction[key]
	if len(seqs) == 0 {
		return nil
	}

	result := make([]uint64, len(seqs))
	copy(result, seqs)
	return result
}

// GetEntriesForContractWildcard returns globalSeqs for account+contract.
func (idx *WALIndex) GetEntriesForContractWildcard(account, contract uint64) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := ContractWildcardKey{Account: account, Contract: contract}
	seqs := idx.byContractWildcard[key]
	if len(seqs) == 0 {
		return nil
	}

	result := make([]uint64, len(seqs))
	copy(result, seqs)
	return result
}

// AllEntries returns all WAL entries (for compaction).
func (idx *WALIndex) AllEntries() []WALEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]WALEntry, 0, len(idx.entries))
	for _, e := range idx.entries {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].GlobalSeq < result[j].GlobalSeq })
	return result
}

// Count returns the number of WAL entries.
func (idx *WALIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Stats returns min/max globalSeq in the WAL.
func (idx *WALIndex) Stats() (count int, minSeq, maxSeq uint64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	count = len(idx.entries)
	for key := range idx.entries {
		if minSeq == 0 || key.GlobalSeq < minSeq {
			minSeq = key.GlobalSeq
		}
		if key.GlobalSeq > maxSeq {
			maxSeq = key.GlobalSeq
		}
	}
	return
}

type WALEntry struct {
	GlobalSeq uint64
	Account   uint64
	Contract  uint64
	Action    uint64
}

type WALWriter struct {
	mu    sync.Mutex
	db    *pebble.DB
	index *WALIndex
	batch *pebble.Batch
	count int

	// Pending entries to add to index after successful commit
	pending []WALEntry

	maxBatchSize int
	stats        WALWriterStats
}

type WALWriterStats struct {
	EntriesWritten   uint64
	BatchesCommitted uint64
}

func NewWALWriter(db *pebble.DB, index *WALIndex) *WALWriter {
	return &WALWriter{
		db:           db,
		index:        index,
		maxBatchSize: 1000,
	}
}

func (w *WALWriter) Add(globalSeq, account, contract, action uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.batch == nil {
		w.batch = w.db.NewBatch()
	}

	key := makeWALKey(globalSeq, account)
	val := makeWALValue(account, contract, action)
	w.batch.Set(key, val, nil)
	w.count++
	w.stats.EntriesWritten++

	// Buffer entry for index update after successful commit
	w.pending = append(w.pending, WALEntry{
		GlobalSeq: globalSeq,
		Account:   account,
		Contract:  contract,
		Action:    action,
	})

	if w.count >= w.maxBatchSize {
		w.commitLocked()
	}
}

func (w *WALWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commitLocked()
}

func (w *WALWriter) FlushToBatch(batch *pebble.Batch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pending) == 0 {
		return nil
	}

	for _, e := range w.pending {
		key := makeWALKey(e.GlobalSeq, e.Account)
		val := makeWALValue(e.Account, e.Contract, e.Action)
		batch.Set(key, val, nil)
	}

	return nil
}

func (w *WALWriter) CommitPending() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, e := range w.pending {
		w.index.Add(e)
	}
	w.pending = w.pending[:0]

	if w.batch != nil {
		w.batch.Close()
		w.batch = nil
	}
	w.count = 0
	w.stats.BatchesCommitted++
}

func (w *WALWriter) commitLocked() error {
	if w.batch == nil || w.count == 0 {
		return nil
	}

	if err := w.batch.Commit(pebble.Sync); err != nil {
		// On failure, discard pending entries (they're not in Pebble)
		w.pending = w.pending[:0]
		return err
	}

	// Only update index AFTER successful Pebble commit
	for _, e := range w.pending {
		w.index.Add(e)
	}
	w.pending = w.pending[:0]

	w.batch.Close()
	w.batch = nil
	w.count = 0
	w.stats.BatchesCommitted++
	return nil
}

func (w *WALWriter) Stats() WALWriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

type WALCompactor struct {
	mu       sync.Mutex
	db       *pebble.DB
	index    *WALIndex
	metadata *ChunkMetadata
	writer   *ChunkWriter

	minSeq uint64
	maxSeq uint64

	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	stats WALCompactorStats
}

type WALCompactorStats struct {
	CompactionRuns   uint64
	EntriesCompacted uint64
	ChunksCreated    uint64
}

func NewWALCompactor(db *pebble.DB, index *WALIndex, metadata *ChunkMetadata, writer *ChunkWriter) *WALCompactor {
	return &WALCompactor{
		db:       db,
		index:    index,
		metadata: metadata,
		writer:   writer,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (c *WALCompactor) Start(interval time.Duration) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.mu.Unlock()

	go c.run(interval)
}

func (c *WALCompactor) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	close(c.stopCh)
	<-c.doneCh

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
}

func (c *WALCompactor) run(interval time.Duration) {
	defer close(c.doneCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			c.Compact()
			return
		case <-ticker.C:
			c.Compact()
		}
	}
}

func (c *WALCompactor) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries := c.index.AllEntries()
	if len(entries) == 0 {
		return nil
	}

	c.stats.CompactionRuns++

	allActionsGroups := make(map[uint64][]uint64)
	contractActionGroups := make(map[ContractActionKey][]uint64)
	contractWildcardGroups := make(map[ContractWildcardKey][]uint64)

	for _, e := range entries {
		allActionsGroups[e.Account] = append(allActionsGroups[e.Account], e.GlobalSeq)

		caKey := ContractActionKey{Account: e.Account, Contract: e.Contract, Action: e.Action}
		contractActionGroups[caKey] = append(contractActionGroups[caKey], e.GlobalSeq)

		cwKey := ContractWildcardKey{Account: e.Account, Contract: e.Contract}
		contractWildcardGroups[cwKey] = append(contractWildcardGroups[cwKey], e.GlobalSeq)
	}

	for account, seqs := range allActionsGroups {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, seq := range seqs {
			c.writer.AddAllActions(account, seq)
		}
	}

	for key, seqs := range contractActionGroups {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, seq := range seqs {
			c.writer.AddContractAction(key.Account, key.Contract, key.Action, seq)
		}
	}

	for key, seqs := range contractWildcardGroups {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, seq := range seqs {
			c.writer.AddContractWildcard(key.Account, key.Contract, seq)
		}
	}

	if err := c.writer.FlushAllPartials(); err != nil {
		return err
	}

	batch := c.db.NewBatch()
	for _, e := range entries {
		batch.Delete(makeWALKey(e.GlobalSeq, e.Account), nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	c.index.RemoveEntries(entries)

	c.stats.EntriesCompacted += uint64(len(entries))

	if len(entries) > 0 {
		c.minSeq = entries[0].GlobalSeq
		c.maxSeq = entries[len(entries)-1].GlobalSeq
	}

	return nil
}

func (c *WALCompactor) Stats() WALCompactorStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *WALCompactor) WALCount() int {
	return c.index.Count()
}

type WALReader struct {
	index *WALIndex
}

func NewWALReader(index *WALIndex) *WALReader {
	return &WALReader{index: index}
}

func (r *WALReader) GetEntriesForAccount(account uint64) ([]uint64, error) {
	return r.index.GetEntriesForAccount(account), nil
}

func (r *WALReader) GetEntriesForContractAction(account, contract, action uint64) ([]uint64, error) {
	return r.index.GetEntriesForContractAction(account, contract, action), nil
}

func (r *WALReader) GetEntriesForContractWildcard(account, contract uint64) ([]uint64, error) {
	return r.index.GetEntriesForContractWildcard(account, contract), nil
}
