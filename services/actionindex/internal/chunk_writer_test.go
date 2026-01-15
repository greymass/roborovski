package internal

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestChunkWriterBasicAdd(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	timeMap := NewTimeMapper()
	w := NewChunkWriter(db, metadata, timeMap)

	for i := uint64(0); i < 100; i++ {
		w.AddAllActions(1000, i)
	}

	a, ca, cw := w.PendingCounts()
	if a != 100 {
		t.Errorf("pending allActions = %d, want 100", a)
	}
	if ca != 0 || cw != 0 {
		t.Errorf("pending contractAction=%d, contractWildcard=%d, want 0,0", ca, cw)
	}

	stats := w.Stats()
	if stats.ChunksWritten != 0 {
		t.Errorf("chunks written = %d, want 0 (not full yet)", stats.ChunksWritten)
	}
}

func TestChunkWriterFlushOnFull(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for i := uint64(0); i < ChunkSize; i++ {
		w.AddAllActions(1000, i*10)
	}

	stats := w.Stats()
	if stats.ChunksWritten != 1 {
		t.Errorf("chunks written = %d, want 1", stats.ChunksWritten)
	}
	if stats.SequencesWritten != ChunkSize {
		t.Errorf("sequences written = %d, want %d", stats.SequencesWritten, ChunkSize)
	}

	if metadata.GetAllActionsChunkCount(1000) != 1 {
		t.Errorf("metadata chunk count = %d, want 1", metadata.GetAllActionsChunkCount(1000))
	}
}

func TestChunkWriterMultipleChunks(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for i := uint64(0); i < ChunkSize*3+500; i++ {
		w.AddAllActions(1000, i)
	}

	stats := w.Stats()
	if stats.ChunksWritten != 3 {
		t.Errorf("chunks written = %d, want 3", stats.ChunksWritten)
	}

	a, _, _ := w.PendingCounts()
	if a != 500 {
		t.Errorf("pending = %d, want 500", a)
	}

	if metadata.GetAllActionsChunkCount(1000) != 3 {
		t.Errorf("metadata chunk count = %d, want 3", metadata.GetAllActionsChunkCount(1000))
	}
}

func TestChunkWriterFlushAllPartials(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for i := uint64(0); i < 100; i++ {
		w.AddAllActions(1000, i)
	}
	for i := uint64(0); i < 50; i++ {
		w.AddContractAction(1000, 5000, 6000, i)
	}
	for i := uint64(0); i < 25; i++ {
		w.AddContractWildcard(2000, 7000, i)
	}

	if err := w.FlushAllPartials(); err != nil {
		t.Fatalf("FlushAllPartials failed: %v", err)
	}

	stats := w.Stats()
	if stats.ChunksWritten != 3 {
		t.Errorf("chunks written = %d, want 3", stats.ChunksWritten)
	}

	a, ca, cw := w.PendingCounts()
	if a != 0 || ca != 0 || cw != 0 {
		t.Errorf("pending after flush: a=%d, ca=%d, cw=%d, want all 0", a, ca, cw)
	}

	if metadata.GetAllActionsChunkCount(1000) != 1 {
		t.Errorf("allActions chunk count = %d, want 1", metadata.GetAllActionsChunkCount(1000))
	}
}

func TestChunkWriterFlushStalePartials(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for i := uint64(0); i < 100; i++ {
		w.AddAllActions(1000, i)
	}

	if err := w.FlushStalePartials(0); err != nil {
		t.Fatalf("FlushStalePartials failed: %v", err)
	}

	a, _, _ := w.PendingCounts()
	if a != 0 {
		t.Errorf("should flush all partials: pending = %d, want 0", a)
	}
}

func TestChunkWriterReadBackFromDB(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	seqs := make([]uint64, ChunkSize)
	for i := range seqs {
		seqs[i] = uint64(i * 10)
		w.AddAllActions(1000, seqs[i])
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	key := makeLegacyAccountActionsKey(1000, 0)
	val, closer, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to read chunk from db: %v", err)
	}

	chunk, err := DecodeChunk(val)
	closer.Close()
	if err != nil {
		t.Fatalf("DecodeChunk failed: %v", err)
	}

	if len(chunk.Seqs) != ChunkSize {
		t.Errorf("chunk size = %d, want %d", len(chunk.Seqs), ChunkSize)
	}

	for i := 0; i < 10; i++ {
		if chunk.Seqs[i] != seqs[i] {
			t.Errorf("seq[%d] = %d, want %d", i, chunk.Seqs[i], seqs[i])
		}
	}
}

func TestChunkWriterCommitWithProperties(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	timeMap := NewTimeMapper()
	w := NewChunkWriter(db, metadata, timeMap)

	for i := uint64(0); i < 100; i++ {
		w.AddAllActions(1000, i)
	}

	timeMap.Record(3600, 1, 100)

	if err := w.FlushAllPartials(); err != nil {
		t.Fatalf("FlushAllPartials failed: %v", err)
	}

	if err := w.CommitWithProperties(1000, 2000); err != nil {
		t.Fatalf("CommitWithProperties failed: %v", err)
	}

	propKey := []byte{PrefixProperties}
	val, closer, err := db.Get(propKey)
	if err != nil {
		t.Fatalf("failed to read properties: %v", err)
	}
	defer closer.Close()

	if len(val) != 8 {
		t.Fatalf("properties length = %d, want 8", len(val))
	}

	libNum, headNum, ok := parsePropertiesValue(val)
	if !ok {
		t.Fatalf("parsePropertiesValue failed")
	}

	if libNum != 1000 {
		t.Errorf("libNum = %d, want 1000", libNum)
	}
	if headNum != 2000 {
		t.Errorf("headNum = %d, want 2000", headNum)
	}
}

func TestChunkWriterClearPartials(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for i := uint64(0); i < 100; i++ {
		w.AddAllActions(1000, i)
		w.AddContractAction(1000, 5000, 6000, i)
	}

	a, ca, _ := w.PendingCounts()
	if a != 100 || ca != 100 {
		t.Errorf("before clear: a=%d, ca=%d", a, ca)
	}

	w.ClearPartials()

	a, ca, cw := w.PendingCounts()
	if a != 0 || ca != 0 || cw != 0 {
		t.Errorf("after clear: a=%d, ca=%d, cw=%d, want all 0", a, ca, cw)
	}
}

func TestChunkWriterMultipleAccounts(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	metadata := NewChunkMetadata()
	w := NewChunkWriter(db, metadata, nil)

	for acc := uint64(1); acc <= 10; acc++ {
		for i := uint64(0); i < ChunkSize+100; i++ {
			w.AddAllActions(acc, i)
		}
	}

	stats := w.Stats()
	if stats.ChunksWritten != 10 {
		t.Errorf("chunks written = %d, want 10 (one per account)", stats.ChunksWritten)
	}

	for acc := uint64(1); acc <= 10; acc++ {
		count := metadata.GetAllActionsChunkCount(acc)
		if count != 1 {
			t.Errorf("account %d chunk count = %d, want 1", acc, count)
		}
	}
}
