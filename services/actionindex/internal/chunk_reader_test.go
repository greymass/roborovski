package internal

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

func setupTestDB(t *testing.T) (*pebble.DB, *ChunkMetadata, *ChunkWriter, func()) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "testdb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}

	metadata := NewChunkMetadata()
	writer := NewChunkWriter(db, metadata, nil)

	cleanup := func() {
		db.Close()
	}

	return db, metadata, writer, cleanup
}

func TestChunkReaderGetLastN(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetLastN(1000, 20)
	if err != nil {
		t.Fatalf("GetLastN failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}

	if seqs[0] != 800 {
		t.Errorf("first seq = %d, want 800", seqs[0])
	}
	if seqs[19] != 990 {
		t.Errorf("last seq = %d, want 990", seqs[19])
	}
}

func TestChunkReaderGetLastNMultipleChunks(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < uint64(ChunkSize*2+500); i++ {
		writer.AddAllActions(1000, i)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetLastN(1000, 1000)
	if err != nil {
		t.Fatalf("GetLastN failed: %v", err)
	}

	if len(seqs) != 1000 {
		t.Errorf("got %d seqs, want 1000", len(seqs))
	}

	expectedLast := uint64(ChunkSize*2 + 500 - 1)
	if seqs[len(seqs)-1] != expectedLast {
		t.Errorf("last seq = %d, want %d", seqs[len(seqs)-1], expectedLast)
	}
}

func TestChunkReaderGetFirstN(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetFirstN(1000, 20)
	if err != nil {
		t.Fatalf("GetFirstN failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}

	if seqs[0] != 0 {
		t.Errorf("first seq = %d, want 0", seqs[0])
	}
	if seqs[19] != 190 {
		t.Errorf("last seq = %d, want 190", seqs[19])
	}
}

func TestChunkReaderGetRange(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetRange(1000, 10, 20)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}

	if seqs[0] != 10 {
		t.Errorf("first seq = %d, want 10", seqs[0])
	}
	if seqs[19] != 29 {
		t.Errorf("last seq = %d, want 29", seqs[19])
	}
}

func TestChunkReaderGetRangeAcrossChunks(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < uint64(ChunkSize+500); i++ {
		writer.AddAllActions(1000, i)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetRange(1000, ChunkSize-5, 20)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}

	if seqs[0] != uint64(ChunkSize-5) {
		t.Errorf("first seq = %d, want %d", seqs[0], ChunkSize-5)
	}
}

func TestChunkReaderGetFromCursor(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetFromCursor(1000, 500, 10, true)
	if err != nil {
		t.Fatalf("GetFromCursor desc failed: %v", err)
	}

	if len(seqs) != 10 {
		t.Errorf("got %d seqs, want 10", len(seqs))
	}
	if seqs[0] != 490 {
		t.Errorf("first seq = %d, want 490 (before cursor)", seqs[0])
	}

	seqs, err = reader.GetFromCursor(1000, 500, 10, false)
	if err != nil {
		t.Fatalf("GetFromCursor asc failed: %v", err)
	}

	if len(seqs) != 10 {
		t.Errorf("got %d seqs, want 10", len(seqs))
	}
	if seqs[0] != 510 {
		t.Errorf("first seq = %d, want 510 (after cursor)", seqs[0])
	}
}

func TestChunkReaderContractAction(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 50; i++ {
		writer.AddContractAction(1000, 5000, 6000, i*10)
	}
	for i := uint64(0); i < 30; i++ {
		writer.AddContractAction(1000, 5000, 6001, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetContractActionLastN(1000, 5000, 6000, 20)
	if err != nil {
		t.Fatalf("GetContractActionLastN failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}
}

func TestChunkReaderContractWildcard(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 50; i++ {
		writer.AddContractWildcard(1000, 5000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetContractWildcardLastN(1000, 5000, 20)
	if err != nil {
		t.Fatalf("GetContractWildcardLastN failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}
}

func TestChunkReaderWithWAL(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 50; i++ {
		writer.AddAllActions(1000, i)
	}
	writer.FlushAllPartials()
	writer.Sync()

	walIndex := NewWALIndex()
	walWriter := NewWALWriter(db, walIndex)
	for i := uint64(50); i < 60; i++ {
		walWriter.Add(i, 1000, 5000, 6000)
	}
	walWriter.Flush()

	walReader := NewWALReader(walIndex)
	reader := NewChunkReader(db, metadata, walReader)

	seqs, err := reader.GetLastN(1000, 20)
	if err != nil {
		t.Fatalf("GetLastN with WAL failed: %v", err)
	}

	if len(seqs) != 20 {
		t.Errorf("got %d seqs, want 20", len(seqs))
	}

	if seqs[len(seqs)-1] != 59 {
		t.Errorf("last seq = %d, want 59 (from WAL)", seqs[len(seqs)-1])
	}
}

func TestChunkReaderGetWithSeqRange(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetWithSeqRange(1000, 200, 500, 50, false)
	if err != nil {
		t.Fatalf("GetWithSeqRange failed: %v", err)
	}

	for _, seq := range seqs {
		if seq < 200 || seq > 500 {
			t.Errorf("seq %d out of range [200, 500]", seq)
		}
	}

	if seqs[0] != 200 {
		t.Errorf("first seq = %d, want 200", seqs[0])
	}
}

func TestChunkReaderGetWithSeqRangeDesc(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < 100; i++ {
		writer.AddAllActions(1000, i*10)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetWithSeqRange(1000, 200, 500, 50, true)
	if err != nil {
		t.Fatalf("GetWithSeqRange desc failed: %v", err)
	}

	if seqs[0] != 500 {
		t.Errorf("first seq (desc) = %d, want 500", seqs[0])
	}

	for i := 1; i < len(seqs); i++ {
		if seqs[i] > seqs[i-1] {
			t.Errorf("seqs not in descending order: %d > %d", seqs[i], seqs[i-1])
		}
	}
}

func TestChunkReaderGetTotalCount(t *testing.T) {
	db, metadata, writer, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(0); i < uint64(ChunkSize+500); i++ {
		writer.AddAllActions(1000, i)
	}
	writer.FlushAllPartials()
	writer.Sync()

	reader := NewChunkReader(db, metadata, nil)

	count, err := reader.GetTotalCount(1000)
	if err != nil {
		t.Fatalf("GetTotalCount failed: %v", err)
	}

	expected := ChunkSize + 500
	if count != expected {
		t.Errorf("count = %d, want %d", count, expected)
	}
}

func TestChunkReaderEmptyAccount(t *testing.T) {
	db, metadata, _, cleanup := setupTestDB(t)
	defer cleanup()

	reader := NewChunkReader(db, metadata, nil)

	seqs, err := reader.GetLastN(9999, 20)
	if err != nil {
		t.Fatalf("GetLastN failed: %v", err)
	}

	if len(seqs) != 0 {
		t.Errorf("got %d seqs for empty account, want 0", len(seqs))
	}
}

func TestIncrementPrefix(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x10, 0x00}, []byte{0x10, 0x01}},
		{[]byte{0x10, 0xFF}, []byte{0x11, 0x00}},
		{[]byte{0xFF, 0xFF}, []byte{0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		result := incrementPrefix(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("incrementPrefix(%v) length = %d, want %d", tt.input, len(result), len(tt.expected))
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("incrementPrefix(%v) = %v, want %v", tt.input, result, tt.expected)
				break
			}
		}
	}
}

func TestFindSeqInChunk(t *testing.T) {
	seqs := []uint64{100, 200, 300, 400, 500}

	tests := []struct {
		target   uint64
		expected int
	}{
		{100, 0},
		{200, 1},
		{500, 4},
		{150, 0},
		{250, 1},
		{50, 0},
		{600, 4},
	}

	for _, tt := range tests {
		result := findSeqInChunk(seqs, tt.target)
		if result != tt.expected {
			t.Errorf("findSeqInChunk(seqs, %d) = %d, want %d", tt.target, result, tt.expected)
		}
	}
}

func TestMergeAndDedupe(t *testing.T) {
	a := []uint64{1, 3, 5, 7}
	b := []uint64{2, 4, 5, 6}

	result := mergeAndDedupe(a, b)

	expected := []uint64{1, 2, 3, 4, 5, 6, 7}
	if len(result) != len(expected) {
		t.Errorf("merged length = %d, want %d", len(result), len(expected))
	}

	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, result[i], expected[i])
		}
	}
}
