package internal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func TestWALWriterBasic(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	walIndex := NewWALIndex()
	w := NewWALWriter(db, walIndex)

	w.Add(100, 1000, 5000, 6000)
	w.Add(101, 1000, 5000, 6001)
	w.Add(102, 2000, 7000, 8000)

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	stats := w.Stats()
	if stats.EntriesWritten != 3 {
		t.Errorf("entries written = %d, want 3", stats.EntriesWritten)
	}

	reader := NewWALReader(walIndex)
	seqs, err := reader.GetEntriesForAccount(1000)
	if err != nil {
		t.Fatalf("GetEntriesForAccount failed: %v", err)
	}

	if len(seqs) != 2 {
		t.Errorf("got %d entries for account 1000, want 2", len(seqs))
	}
}

func TestWALWriterAutoBatch(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	walIndex := NewWALIndex()
	w := NewWALWriter(db, walIndex)
	w.maxBatchSize = 100

	for i := uint64(0); i < 250; i++ {
		w.Add(i, 1000, 5000, 6000)
	}

	stats := w.Stats()
	if stats.BatchesCommitted != 2 {
		t.Errorf("batches committed = %d, want 2 (auto-commit at 100)", stats.BatchesCommitted)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	stats = w.Stats()
	if stats.BatchesCommitted != 3 {
		t.Errorf("batches committed after flush = %d, want 3", stats.BatchesCommitted)
	}
}

func TestWALCompactorBasic(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	chunkWriter := NewChunkWriter(db, metadata, nil)
	walIndex := NewWALIndex()

	walWriter := NewWALWriter(db, walIndex)
	for i := uint64(0); i < 100; i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	for i := uint64(100); i < 150; i++ {
		walWriter.Add(i, 2000, 7000, 8000)
	}
	if err := walWriter.Flush(); err != nil {
		t.Fatalf("WAL flush failed: %v", err)
	}

	compactor := NewWALCompactor(db, walIndex, metadata, chunkWriter)

	count := compactor.WALCount()
	if count != 150 {
		t.Errorf("WAL count before compact = %d, want 150", count)
	}

	if err := compactor.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	count = compactor.WALCount()
	if count != 0 {
		t.Errorf("WAL count after compact = %d, want 0", count)
	}

	stats := compactor.Stats()
	if stats.CompactionRuns != 1 {
		t.Errorf("compaction runs = %d, want 1", stats.CompactionRuns)
	}
	if stats.EntriesCompacted != 150 {
		t.Errorf("entries compacted = %d, want 150", stats.EntriesCompacted)
	}

	if metadata.GetAllActionsChunkCount(1000) != 1 {
		t.Errorf("account 1000 chunk count = %d, want 1", metadata.GetAllActionsChunkCount(1000))
	}
	if metadata.GetAllActionsChunkCount(2000) != 1 {
		t.Errorf("account 2000 chunk count = %d, want 1", metadata.GetAllActionsChunkCount(2000))
	}
}

func TestWALCompactorMultipleRuns(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	chunkWriter := NewChunkWriter(db, metadata, nil)
	walIndex := NewWALIndex()
	compactor := NewWALCompactor(db, walIndex, metadata, chunkWriter)

	walWriter := NewWALWriter(db, walIndex)
	for i := uint64(0); i < 50; i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	walWriter.Flush()
	compactor.Compact()

	for i := uint64(50); i < 100; i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	walWriter.Flush()
	compactor.Compact()

	stats := compactor.Stats()
	if stats.CompactionRuns != 2 {
		t.Errorf("compaction runs = %d, want 2", stats.CompactionRuns)
	}
	if stats.EntriesCompacted != 100 {
		t.Errorf("entries compacted = %d, want 100", stats.EntriesCompacted)
	}
}

func TestWALCompactorStartStop(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	chunkWriter := NewChunkWriter(db, metadata, nil)
	walIndex := NewWALIndex()
	compactor := NewWALCompactor(db, walIndex, metadata, chunkWriter)

	walWriter := NewWALWriter(db, walIndex)
	for i := uint64(0); i < 100; i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	walWriter.Flush()

	compactor.Start(10 * time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	compactor.Stop()

	count := compactor.WALCount()
	if count != 0 {
		t.Errorf("WAL should be empty after compactor stopped, got %d", count)
	}

	stats := compactor.Stats()
	if stats.CompactionRuns == 0 {
		t.Error("expected at least one compaction run")
	}
}

func TestWALReaderFilters(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	walIndex := NewWALIndex()
	w := NewWALWriter(db, walIndex)

	w.Add(100, 1000, 5000, 6000)
	w.Add(101, 1000, 5000, 6001)
	w.Add(102, 1000, 7000, 8000)
	w.Add(103, 2000, 5000, 6000)
	w.Add(104, 1000, 5000, 6000)

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	reader := NewWALReader(walIndex)

	seqs, err := reader.GetEntriesForAccount(1000)
	if err != nil {
		t.Fatalf("GetEntriesForAccount failed: %v", err)
	}
	if len(seqs) != 4 {
		t.Errorf("account filter: got %d, want 4", len(seqs))
	}

	seqs, err = reader.GetEntriesForContractAction(1000, 5000, 6000)
	if err != nil {
		t.Fatalf("GetEntriesForContractAction failed: %v", err)
	}
	if len(seqs) != 2 {
		t.Errorf("contract+action filter: got %d, want 2", len(seqs))
	}

	seqs, err = reader.GetEntriesForContractWildcard(1000, 5000)
	if err != nil {
		t.Fatalf("GetEntriesForContractWildcard failed: %v", err)
	}
	if len(seqs) != 3 {
		t.Errorf("contract wildcard filter: got %d, want 3", len(seqs))
	}
}

func TestWALCompactorEmptyWAL(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	chunkWriter := NewChunkWriter(db, metadata, nil)
	walIndex := NewWALIndex()
	compactor := NewWALCompactor(db, walIndex, metadata, chunkWriter)

	if err := compactor.Compact(); err != nil {
		t.Fatalf("Compact on empty WAL failed: %v", err)
	}

	stats := compactor.Stats()
	if stats.CompactionRuns != 0 {
		t.Errorf("should not count empty compaction runs, got %d", stats.CompactionRuns)
	}
}

func TestWALIndexLoadFromDB(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	// Write entries directly to Pebble (simulating existing WAL)
	batch := db.NewBatch()
	batch.Set(makeWALKey(100), makeWALValue(1000, 5000, 6000), nil)
	batch.Set(makeWALKey(101), makeWALValue(1000, 5000, 6001), nil)
	batch.Set(makeWALKey(102), makeWALValue(2000, 7000, 8000), nil)
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch commit failed: %v", err)
	}
	batch.Close()

	// Load WAL index from DB
	walIndex := NewWALIndex()
	if err := walIndex.LoadFromDB(db); err != nil {
		t.Fatalf("LoadFromDB failed: %v", err)
	}

	// Verify count
	if walIndex.Count() != 3 {
		t.Errorf("WAL index count = %d, want 3", walIndex.Count())
	}

	// Verify account lookup
	seqs := walIndex.GetEntriesForAccount(1000)
	if len(seqs) != 2 {
		t.Errorf("account 1000 entries = %d, want 2", len(seqs))
	}

	seqs = walIndex.GetEntriesForAccount(2000)
	if len(seqs) != 1 {
		t.Errorf("account 2000 entries = %d, want 1", len(seqs))
	}
}

func TestWALIndexSortedOrder(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	walIndex := NewWALIndex()
	w := NewWALWriter(db, walIndex)

	// Add entries in order
	w.Add(100, 1000, 5000, 6000)
	w.Add(200, 1000, 5000, 6000)
	w.Add(300, 1000, 5000, 6000)
	w.Flush()

	seqs := walIndex.GetEntriesForAccount(1000)
	if len(seqs) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(seqs))
	}

	// Verify sorted order
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("entries not sorted: seqs[%d]=%d <= seqs[%d]=%d", i, seqs[i], i-1, seqs[i-1])
		}
	}
}

func TestWALCompactorCreatesChunks(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	chunkWriter := NewChunkWriter(db, metadata, nil)
	walIndex := NewWALIndex()

	walWriter := NewWALWriter(db, walIndex)
	for i := uint64(0); i < uint64(ChunkSize+500); i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	walWriter.Flush()

	compactor := NewWALCompactor(db, walIndex, metadata, chunkWriter)
	if err := compactor.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if metadata.GetAllActionsChunkCount(1000) != 2 {
		t.Errorf("should create 2 chunks (1 full + 1 partial), got %d",
			metadata.GetAllActionsChunkCount(1000))
	}

	key := makeLegacyAccountActionsKey(1000, 0)
	val, closer, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to read chunk 0: %v", err)
	}
	chunk, err := DecodeChunk(val)
	closer.Close()
	if err != nil {
		t.Fatalf("DecodeChunk failed: %v", err)
	}
	if len(chunk.Seqs) != ChunkSize {
		t.Errorf("chunk 0 size = %d, want %d", len(chunk.Seqs), ChunkSize)
	}
}
