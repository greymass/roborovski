package appendlog

import (
	"os"
	"testing"
)

func TestWriteThroughCache(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SliceStore with cache enabled
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 10,
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test data (start at block 2 - block 1 doesn't exist in trace files)
	testData := []byte("test block data for caching")

	// Write a block
	err = store.AppendBlock(2, testData, 1, 10, false)
	if err != nil {
		t.Fatal(err)
	}

	// Verify cache size
	cacheSize := store.GetCacheSize()
	if cacheSize != 1 {
		t.Errorf("expected cache size 1, got %d", cacheSize)
	}

	// Read block - should hit cache
	data, err := store.GetBlock(2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify data
	if string(data) != string(testData) {
		t.Errorf("expected %q, got %q", testData, data)
	}

	// Verify cache hit
	hits, misses := store.GetCacheStats()
	if hits != 1 {
		t.Errorf("expected 1 cache hit, got %d", hits)
	}
	if misses != 0 {
		t.Errorf("expected 0 cache misses, got %d", misses)
	}
}

func TestCacheDataIsolation(t *testing.T) {
	// Test that modifying returned data doesn't affect cache
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SliceStore with cache enabled
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 10,
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write a block (start at block 2 - block 1 doesn't exist in trace files)
	testData := []byte{1, 2, 3, 4, 5}
	err = store.AppendBlock(2, testData, 1, 5, false)
	if err != nil {
		t.Fatal(err)
	}

	// Read and modify
	data1, err := store.GetBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	data1[0] = 99 // Modify the returned data

	// Read again - should get original data
	data2, err := store.GetBlock(2)
	if err != nil {
		t.Fatal(err)
	}

	if data2[0] != 1 {
		t.Errorf("cache was corrupted: expected data2[0]=1, got %d", data2[0])
	}
}

// ============== Batch Append Tests ==============

func TestAppendBlockBatch_Basic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SliceStore with cache enabled
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 10,
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create batch of blocks (start at block 2 - block 1 doesn't exist in trace files)
	blocks := []BlockEntry{
		{BlockNum: 2, Data: []byte{2, 2, 2}, GlobMin: 1, GlobMax: 5},
		{BlockNum: 3, Data: []byte{3, 3, 3}, GlobMin: 6, GlobMax: 10},
		{BlockNum: 4, Data: []byte{4, 4, 4}, GlobMin: 11, GlobMax: 15},
	}

	// Append batch
	err = store.AppendBlockBatch(blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to disk so data is readable
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}

	// Verify LIB/HEAD updated to last block
	if store.GetLIB() != 4 {
		t.Errorf("expected LIB=4, got %d", store.GetLIB())
	}
	if store.GetHead() != 4 {
		t.Errorf("expected HEAD=4, got %d", store.GetHead())
	}

	// Verify all blocks are cached and readable
	for _, block := range blocks {
		data, err := store.GetBlock(block.BlockNum)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", block.BlockNum, err)
		}
		if data[0] != block.Data[0] {
			t.Errorf("block %d: expected data[0]=%d, got %d", block.BlockNum, block.Data[0], data[0])
		}
	}

	// Note: Batch writes skip caching for performance (write-only optimization)
	// Cache is a write-through cache, not a read cache - it's only populated on writes
	cacheSize := store.GetCacheSize()
	if cacheSize != 0 {
		t.Errorf("expected cache size 0 (batch writes skip caching), got %d", cacheSize)
	}
}

func TestAppendBlockBatch_Empty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SliceStore
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 10,
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Empty batch should succeed without error
	err = store.AppendBlockBatch([]BlockEntry{})
	if err != nil {
		t.Errorf("empty batch should not error: %v", err)
	}
}

func TestAppendBlockBatch_LargeBatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SliceStore
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 50, // Large enough for 100 blocks
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create large batch of 100 blocks (start at block 2)
	blocks := make([]BlockEntry, 100)
	for i := range blocks {
		blockNum := uint32(i + 2) // Start at block 2
		blocks[i] = BlockEntry{
			BlockNum: blockNum,
			Data:     []byte{byte(blockNum), byte(blockNum), byte(blockNum)},
			GlobMin:  uint64(i*10 + 1),
			GlobMax:  uint64(i*10 + 10),
		}
	}

	// Append batch
	err = store.AppendBlockBatch(blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to disk so data is readable
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}

	// Verify last block (2 + 99 = 101)
	if store.GetLIB() != 101 {
		t.Errorf("expected LIB=101, got %d", store.GetLIB())
	}

	// Spot check some blocks
	for _, checkBlock := range []uint32{2, 50, 101} {
		data, err := store.GetBlock(checkBlock)
		if err != nil {
			t.Fatalf("failed to read block %d: %v", checkBlock, err)
		}
		if data[0] != byte(checkBlock) {
			t.Errorf("block %d: expected data[0]=%d, got %d", checkBlock, checkBlock, data[0])
		}
	}
}

func TestAppendBlockBatch_PersistsToDisk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "appendlog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Write batch - use properly formatted block data (starts with blockNum as uint32)
	opts := SliceStoreOptions{
		BlocksPerSlice: 100,
		BlockCacheSize: 10,
		Debug:          false,
	}
	store, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Create block data with proper format: [blockNum:u32][rest of data...]
	makeBlockData := func(blockNum uint32) []byte {
		data := make([]byte, 20)
		// First 4 bytes = blockNum (little endian) - required by crash recovery
		data[0] = byte(blockNum)
		data[1] = byte(blockNum >> 8)
		data[2] = byte(blockNum >> 16)
		data[3] = byte(blockNum >> 24)
		// Rest is test data
		for i := 4; i < len(data); i++ {
			data[i] = byte(blockNum)
		}
		return data
	}

	// Start at block 2 - block 1 doesn't exist in trace files
	blocks := []BlockEntry{
		{BlockNum: 2, Data: makeBlockData(2), GlobMin: 1, GlobMax: 5},
		{BlockNum: 3, Data: makeBlockData(3), GlobMin: 6, GlobMax: 10},
	}

	err = store.AppendBlockBatch(blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Close store
	store.Close()

	// Reopen and verify data persisted
	store2, err := NewSliceStore(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// LIB should be restored from state.json
	if store2.GetLIB() != 3 {
		t.Errorf("expected LIB=3 after reopen, got %d", store2.GetLIB())
	}

	// Data should be readable from disk (cache is empty after reopen)
	data, err := store2.GetBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	// First byte should be block number (2)
	if data[0] != 2 {
		t.Errorf("expected data[0]=2 after reopen, got %d", data[0])
	}
}
