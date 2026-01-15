package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestChunkMetadataBasicOperations(t *testing.T) {
	m := NewChunkMetadata()

	m.AddAllActionsChunk(1000, 100)
	m.AddAllActionsChunk(1000, 200)
	m.AddAllActionsChunk(1000, 300)
	m.AddAllActionsChunk(2000, 500)

	if got := m.GetAllActionsChunkCount(1000); got != 3 {
		t.Errorf("GetAllActionsChunkCount(1000) = %d, want 3", got)
	}
	if got := m.GetAllActionsChunkCount(2000); got != 1 {
		t.Errorf("GetAllActionsChunkCount(2000) = %d, want 1", got)
	}
	if got := m.GetAllActionsChunkCount(9999); got != 0 {
		t.Errorf("GetAllActionsChunkCount(9999) = %d, want 0", got)
	}

	if got := m.Stats(); got != 2 {
		t.Errorf("Stats = %d, want 2 (unique accounts)", got)
	}
}

func TestFindAllActionsChunk(t *testing.T) {
	m := NewChunkMetadata()

	m.AddAllActionsChunk(1000, 100)
	m.AddAllActionsChunk(1000, 200)
	m.AddAllActionsChunk(1000, 300)

	tests := []struct {
		name      string
		targetSeq uint64
		wantID    uint32
		wantFound bool
	}{
		{"before first chunk", 50, 0, true},
		{"exactly at first chunk", 100, 0, true},
		{"between first and second", 150, 0, true},
		{"exactly at second chunk", 200, 1, true},
		{"between second and third", 250, 1, true},
		{"exactly at third chunk", 300, 2, true},
		{"after last chunk", 500, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkID, found := m.FindAllActionsChunk(1000, tt.targetSeq)
			if found != tt.wantFound {
				t.Errorf("found = %v, want %v", found, tt.wantFound)
			}
			if chunkID != tt.wantID {
				t.Errorf("chunkID = %d, want %d", chunkID, tt.wantID)
			}
		})
	}

	_, found := m.FindAllActionsChunk(9999, 100)
	if found {
		t.Error("expected not found for unknown account")
	}
}

func TestChunkMetadataFilePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.bin")

	original := NewChunkMetadata()

	for acc := uint64(1); acc <= 100; acc++ {
		for i := uint64(0); i < 5; i++ {
			original.AddAllActionsChunk(acc, i*10000)
		}
	}

	if err := original.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loaded, err := LoadChunkMetadataFromFile(path)
	if err != nil {
		t.Fatalf("LoadChunkMetadataFromFile failed: %v", err)
	}

	if original.Stats() != loaded.Stats() {
		t.Errorf("Stats mismatch: original=%d loaded=%d", original.Stats(), loaded.Stats())
	}

	for acc := uint64(1); acc <= 100; acc++ {
		origCount := original.GetAllActionsChunkCount(acc)
		loadCount := loaded.GetAllActionsChunkCount(acc)
		if origCount != loadCount {
			t.Errorf("AllActions count mismatch for account %d: %d vs %d", acc, origCount, loadCount)
		}
	}

	origID, origFound := original.FindAllActionsChunk(50, 25000)
	loadID, loadFound := loaded.FindAllActionsChunk(50, 25000)
	if origFound != loadFound || origID != loadID {
		t.Errorf("FindAllActionsChunk mismatch: orig=(%d,%v) load=(%d,%v)",
			origID, origFound, loadID, loadFound)
	}
}

func TestChunkMetadataFileNotFound(t *testing.T) {
	_, err := LoadChunkMetadataFromFile("/nonexistent/path/metadata.bin")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestChunkMetadataCorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "corrupt.bin")

	if err := os.WriteFile(path, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadChunkMetadataFromFile(path)
	if err != ErrMetadataCorrupt {
		t.Errorf("expected ErrMetadataCorrupt, got %v", err)
	}
}

func TestChunkMetadataAtomicSave(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.bin")

	m := NewChunkMetadata()
	m.AddAllActionsChunk(1000, 100)

	if err := m.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should be cleaned up after save")
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("final file should exist")
	}
}

func TestRebuildChunkMetadataFromDB(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	batch := db.NewBatch()

	for i := uint32(0); i < 5; i++ {
		seqs := make([]uint64, 100)
		for j := range seqs {
			seqs[j] = uint64(i)*10000 + uint64(j)
		}
		key := makeLegacyAccountActionsKey(1000, i)
		value, err := EncodeChunk(seqs)
		if err != nil {
			t.Fatalf("EncodeChunk failed: %v", err)
		}
		batch.Set(key, value, nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch commit failed: %v", err)
	}

	m, err := RebuildChunkMetadataFromDB(db)
	if err != nil {
		t.Fatalf("RebuildChunkMetadataFromDB failed: %v", err)
	}

	if got := m.GetAllActionsChunkCount(1000); got != 5 {
		t.Errorf("AllActions chunk count = %d, want 5", got)
	}

	chunkID, found := m.FindAllActionsChunk(1000, 25000)
	if !found {
		t.Fatal("expected to find chunk")
	}
	if chunkID != 2 {
		t.Errorf("chunkID = %d, want 2", chunkID)
	}
}

func TestRebuildChunkMetadataEmptyDB(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tmpDir, "emptydb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	m, err := RebuildChunkMetadataFromDB(db)
	if err != nil {
		t.Fatalf("RebuildChunkMetadataFromDB failed: %v", err)
	}

	if got := m.Stats(); got != 0 {
		t.Errorf("Stats = %d, want 0", got)
	}
}

func TestRebuildChunkMetadataOutOfOrder(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer db.Close()

	batch := db.NewBatch()

	chunksToWrite := []struct {
		chunkID uint32
		baseSeq uint64
	}{
		{0, 300000},
		{1, 400000},
		{2, 100000},
		{3, 200000},
	}

	for _, chunk := range chunksToWrite {
		seqs := make([]uint64, 100)
		for j := range seqs {
			seqs[j] = chunk.baseSeq + uint64(j)
		}
		key := makeLegacyAccountActionsKey(1000, chunk.chunkID)
		value, err := EncodeChunk(seqs)
		if err != nil {
			t.Fatalf("EncodeChunk failed: %v", err)
		}
		batch.Set(key, value, nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch commit failed: %v", err)
	}

	m, err := RebuildChunkMetadataFromDB(db)
	if err != nil {
		t.Fatalf("RebuildChunkMetadataFromDB failed: %v", err)
	}

	minBase, maxBase := m.GetAllActionsSeqRange(1000)
	if minBase != 100000 {
		t.Errorf("minBase = %d, want 100000 (should be sorted)", minBase)
	}
	if maxBase != 400000 {
		t.Errorf("maxBase = %d, want 400000 (should be sorted)", maxBase)
	}

	tests := []struct {
		name      string
		targetSeq uint64
		wantIdx   uint32
	}{
		{"in first sorted chunk", 150000, 0},
		{"in second sorted chunk", 250000, 1},
		{"in third sorted chunk", 350000, 2},
		{"in last sorted chunk", 450000, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, found := m.FindAllActionsChunk(1000, tt.targetSeq)
			if !found {
				t.Fatal("expected to find chunk")
			}
			if idx != tt.wantIdx {
				t.Errorf("index = %d, want %d", idx, tt.wantIdx)
			}
		})
	}
}

func TestLoadChunkMetadataOutOfOrderFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.bin")

	original := NewChunkMetadata()
	original.allActions[1000] = []uint64{300000, 400000, 100000, 200000}

	if err := original.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loaded, err := LoadChunkMetadataFromFile(path)
	if err != nil {
		t.Fatalf("LoadChunkMetadataFromFile failed: %v", err)
	}

	minBase, maxBase := loaded.GetAllActionsSeqRange(1000)
	if minBase != 100000 {
		t.Errorf("minBase = %d, want 100000 (should be sorted after load)", minBase)
	}
	if maxBase != 400000 {
		t.Errorf("maxBase = %d, want 400000 (should be sorted after load)", maxBase)
	}
}
