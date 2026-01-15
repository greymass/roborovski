package appendlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSliceStore_Basic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		Debug:           true,
		BlocksPerSlice:  100, // Small slice for testing
		MaxCachedSlices: 2,
		BlockCacheSize:  50,
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write some blocks (start at 2 since block 1 doesn't exist in trace files)
	for i := uint32(2); i <= 11; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	// Verify LIB/HEAD
	if store.GetLIB() != 11 {
		t.Errorf("expected LIB=11, got %d", store.GetLIB())
	}
	if store.GetHead() != 11 {
		t.Errorf("expected HEAD=11, got %d", store.GetHead())
	}

	// Read blocks back
	for i := uint32(2); i <= 11; i++ {
		data, err := store.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func TestSliceStore_SliceRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Use very small slices for testing rotation
	opts := SliceStoreOptions{
		Debug:           true,
		BlocksPerSlice:  10, // 10 blocks per slice
		MaxCachedSlices: 3,
		BlockCacheSize:  20,
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write 25 blocks - should create 3 slices (2-11, 12-21, 22-31)
	for i := uint32(2); i <= 26; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	// Verify we have 3 slices
	sliceInfos := store.GetSliceInfos()
	if len(sliceInfos) != 3 {
		t.Errorf("expected 3 slices, got %d", len(sliceInfos))
	}

	// Verify slice boundaries
	// Slice 0: blocks 1-10 (but starts at 2)
	// Slice 1: blocks 11-20
	// Slice 2: blocks 21-30 (active, only has 21-26)
	if sliceInfos[0].SliceNum != 0 || sliceInfos[0].StartBlock != 1 {
		t.Errorf("slice 0 wrong: %+v", sliceInfos[0])
	}
	if sliceInfos[1].SliceNum != 1 || sliceInfos[1].StartBlock != 11 {
		t.Errorf("slice 1 wrong: %+v", sliceInfos[1])
	}
	if sliceInfos[2].SliceNum != 2 || sliceInfos[2].StartBlock != 21 {
		t.Errorf("slice 2 wrong: %+v", sliceInfos[2])
	}

	// Read blocks from all slices
	for i := uint32(2); i <= 26; i++ {
		data, err := store.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func TestSliceStore_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		Debug:           false,
		BlocksPerSlice:  100,
		MaxCachedSlices: 2,
		BlockCacheSize:  50,
	}

	// Create and write (start at block 2 since block 1 doesn't exist)
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(2); i <= 21; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	// Close
	store.Close()

	// Reopen
	store2, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// Verify state restored
	if store2.GetLIB() != 21 {
		t.Errorf("expected LIB=21 after reopen, got %d", store2.GetLIB())
	}

	// Verify data readable
	for i := uint32(2); i <= 21; i++ {
		data, err := store2.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d after reopen: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d after reopen: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func TestSliceStore_BatchAppend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		Debug:           false,
		BlocksPerSlice:  50,
		MaxCachedSlices: 2,
		BlockCacheSize:  100,
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create batch spanning multiple slices (start at block 2)
	blocks := make([]BlockEntry, 75)
	for i := range blocks {
		blockNum := uint32(i + 2) // Start at block 2
		blocks[i] = BlockEntry{
			BlockNum: blockNum,
			Data:     []byte{byte(blockNum), byte(blockNum), byte(blockNum)},
			GlobMin:  uint64(i*10 + 1),
			GlobMax:  uint64(i*10 + 10),
		}
	}

	err = store.AppendBlockBatch(blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to disk so data is readable
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}

	// Verify LIB (blocks 2-76)
	if store.GetLIB() != 76 {
		t.Errorf("expected LIB=76, got %d", store.GetLIB())
	}

	// Should have 2 slices (1-50, 51-100)
	sliceInfos := store.GetSliceInfos()
	if len(sliceInfos) != 2 {
		t.Errorf("expected 2 slices, got %d", len(sliceInfos))
	}

	// Verify data
	for i := uint32(2); i <= 76; i++ {
		data, err := store.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func TestSliceStore_Cache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		Debug:           false,
		BlocksPerSlice:  100,
		MaxCachedSlices: 2,
		BlockCacheSize:  5, // Small cache for testing
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write 10 blocks (start at block 2)
	for i := uint32(2); i <= 11; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Cache should have 5 blocks (7-11, oldest evicted)
	cacheSize := store.GetCacheSize()
	if cacheSize != 5 {
		t.Errorf("expected cache size 5, got %d", cacheSize)
	}

	// Reading block 11 should hit cache
	_, err = store.GetBlock(11)
	if err != nil {
		t.Fatal(err)
	}
	hits, misses := store.GetCacheStats()
	if hits != 1 {
		t.Errorf("expected 1 cache hit, got %d", hits)
	}

	// Reading block 2 should miss cache (evicted)
	store.Flush() // Ensure data is on disk
	_, err = store.GetBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	_, misses = store.GetCacheStats()
	if misses != 1 {
		t.Errorf("expected 1 cache miss, got %d", misses)
	}
}

func TestSliceManager_BlockToSliceNum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-manager-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewSliceManager(tmpDir, 100, 2, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Test block to slice mapping
	tests := []struct {
		blockNum uint32
		expected uint32
	}{
		{1, 0},
		{50, 0},
		{100, 0},
		{101, 1},
		{200, 1},
		{201, 2},
		{1000, 9},
	}

	for _, tc := range tests {
		result := sm.BlockToSliceNum(tc.blockNum)
		if result != tc.expected {
			t.Errorf("BlockToSliceNum(%d) = %d, expected %d", tc.blockNum, result, tc.expected)
		}
	}
}

func TestSliceManager_SliceNumToBlockRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-manager-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewSliceManager(tmpDir, 100, 2, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Test slice to block range mapping
	tests := []struct {
		sliceNum      uint32
		expectedStart uint32
		expectedEnd   uint32
	}{
		{0, 1, 100},
		{1, 101, 200},
		{2, 201, 300},
		{9, 901, 1000},
	}

	for _, tc := range tests {
		start, end := sm.SliceNumToBlockRange(tc.sliceNum)
		if start != tc.expectedStart || end != tc.expectedEnd {
			t.Errorf("SliceNumToBlockRange(%d) = (%d, %d), expected (%d, %d)",
				tc.sliceNum, start, end, tc.expectedStart, tc.expectedEnd)
		}
	}
}

func TestSlice_AppendAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	slice := &Slice{
		SliceNum:   0,
		StartBlock: 1,
		basePath:   tmpDir,
	}

	// Load for writing
	err = slice.Load(true)
	if err != nil {
		t.Fatal(err)
	}
	defer slice.Close()

	// Write blocks (start at 2 to match production - block 1 doesn't exist in trace files)
	for i := uint32(2); i <= 6; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		_, err = slice.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	// Sync to disk
	err = slice.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Read blocks
	for i := uint32(2); i <= 6; i++ {
		data, err := slice.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func TestSlice_GlobIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	slice := &Slice{
		SliceNum:   0,
		StartBlock: 1,
		basePath:   tmpDir,
	}

	err = slice.Load(true)
	if err != nil {
		t.Fatal(err)
	}
	defer slice.Close()

	// Write blocks with specific glob ranges
	_, err = slice.AppendBlock(1, []byte{1}, 100, 110, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = slice.AppendBlock(2, []byte{2}, 111, 120, false)
	if err != nil {
		t.Fatal(err)
	}

	// Build glob index (since AppendBlock skips it for performance)
	if _, err := slice.BuildGlobIndex(); err != nil {
		t.Fatal(err)
	}

	// Test glob lookup
	blockNum, err := slice.GetBlockByGlob(105)
	if err != nil {
		t.Fatal(err)
	}
	if blockNum != 1 {
		t.Errorf("glob 105 should be in block 1, got block %d", blockNum)
	}

	blockNum, err = slice.GetBlockByGlob(115)
	if err != nil {
		t.Fatal(err)
	}
	if blockNum != 2 {
		t.Errorf("glob 115 should be in block 2, got block %d", blockNum)
	}
}

// Test compression support
func TestSliceStore_Compression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-compression-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		Debug:           false,
		BlocksPerSlice:  100,
		MaxCachedSlices: 2,
		BlockCacheSize:  50,
		EnableZstd:      true,
		ZstdLevel:       3,
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write blocks with repetitive data (should compress well) - start at block 2
	for i := uint32(2); i <= 11; i++ {
		// Create data with repetition (compresses well)
		data := make([]byte, 1000)
		for j := range data {
			data[j] = byte(i)
		}
		err = store.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	// Verify LIB/HEAD
	if store.GetLIB() != 11 {
		t.Errorf("expected LIB=11, got %d", store.GetLIB())
	}

	// Read blocks back and verify they decompress correctly
	for i := uint32(2); i <= 11; i++ {
		data, err := store.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		// Verify length after decompression
		if len(data) != 1000 {
			t.Errorf("block %d: expected length 1000, got %d", i, len(data))
		}
		// Verify data content
		for j := range data {
			if data[j] != byte(i) {
				t.Errorf("block %d: expected data[%d]=%d, got %d", i, j, i, data[j])
				break
			}
		}
	}
}

// Test backward compatibility (reading uncompressed data with compression enabled)
func TestSliceStore_BackwardCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-compat-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Write blocks WITHOUT compression (start at block 2)
	optsNoZstd := SliceStoreOptions{
		BlocksPerSlice: 100,
		EnableZstd:     false,
	}
	store1, err := NewSliceStore(tmpDir, optsNoZstd)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(2); i <= 6; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store1.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatal(err)
		}
	}
	store1.Close()

	// Reopen WITH compression enabled
	optsZstd := SliceStoreOptions{
		BlocksPerSlice: 100,
		EnableZstd:     true,
		ZstdLevel:      3,
	}
	store2, err := NewSliceStore(tmpDir, optsZstd)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// Should be able to read old uncompressed blocks
	for i := uint32(2); i <= 6; i++ {
		data, err := store2.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read uncompressed block %d with compression enabled: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}

	// Write new blocks WITH compression
	for i := uint32(7); i <= 11; i++ {
		data := []byte{byte(i), byte(i), byte(i)}
		err = store2.AppendBlock(i, data, uint64(i*10), uint64(i*10+5), false)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Should be able to read both old and new blocks
	for i := uint32(2); i <= 11; i++ {
		data, err := store2.GetBlock(i)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("block %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

// Verify directories are created correctly
func TestSliceStore_DirectoryStructure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "slice-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := SliceStoreOptions{
		BlocksPerSlice: 10,
	}

	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough blocks to create 2 slices (start at block 2)
	for i := uint32(2); i <= 16; i++ {
		err = store.AppendBlock(i, []byte{byte(i)}, uint64(i), uint64(i), false)
		if err != nil {
			t.Fatal(err)
		}
	}
	store.Close()

	// Verify directory structure
	// New naming format: history_STARTBLOCK-ENDBLOCK (10-digit zero-padded)
	slice0Path := filepath.Join(tmpDir, "history_0000000001-0000000010")
	slice1Path := filepath.Join(tmpDir, "history_0000000011-0000000020")
	slicesJsonPath := filepath.Join(tmpDir, "slices.json")
	stateJsonPath := filepath.Join(tmpDir, "state.json")

	if _, err := os.Stat(slice0Path); os.IsNotExist(err) {
		t.Error("history_0000000001-0000000010 directory not created")
	}
	if _, err := os.Stat(slice1Path); os.IsNotExist(err) {
		t.Error("history_0000000011-0000000020 directory not created")
	}
	if _, err := os.Stat(slicesJsonPath); os.IsNotExist(err) {
		t.Error("slices.json not created")
	}
	if _, err := os.Stat(stateJsonPath); os.IsNotExist(err) {
		t.Error("state.json not created")
	}

	// Verify slice contents
	slice0DataLog := filepath.Join(slice0Path, "data.log")
	slice0BlocksIdx := filepath.Join(slice0Path, "blocks.index")

	if _, err := os.Stat(slice0DataLog); os.IsNotExist(err) {
		t.Error("history_0000000001-0000000010/data.log not created")
	}
	if _, err := os.Stat(slice0BlocksIdx); os.IsNotExist(err) {
		t.Error("history_0000000001-0000000010/blocks.index not created")
	}
	// Note: globalseq.index is no longer created (removed Jan 2025 for performance)
}
