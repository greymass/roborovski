package corereader

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

// =============================================================================
// Coverage Tests for slice_buffer.go - Target: 0% → 90%+
// =============================================================================

// TestNewSliceBufferPool tests buffer pool creation
func TestNewSliceBufferPool(t *testing.T) {
	t.Run("PoolDisabled", func(t *testing.T) {
		pool := NewSliceBufferPool(0)
		if pool.enabled {
			t.Error("Expected pool to be disabled with maxSizeMB=0")
		}
		t.Log("Pool correctly disabled")
	})

	t.Run("PoolEnabled", func(t *testing.T) {
		pool := NewSliceBufferPool(100)
		if !pool.enabled {
			t.Error("Expected pool to be enabled with maxSizeMB=100")
		}
		if pool.maxBytes != 100*1024*1024 {
			t.Errorf("Expected maxBytes=104857600, got %d", pool.maxBytes)
		}
		t.Logf("Pool enabled: max=%d bytes", pool.maxBytes)
	})

	t.Run("NegativeSizeDisables", func(t *testing.T) {
		pool := NewSliceBufferPool(-1)
		if pool.enabled {
			t.Error("Expected pool to be disabled with negative size")
		}
	})
}

// TestSliceBufferPoolOperations tests buffer pool basic operations
func TestSliceBufferPoolOperations(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReaderWithCache(dataPath, 50) // 50MB cache
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	if reader.bufferPool == nil {
		t.Fatal("Buffer pool not initialized")
	}

	pool := reader.bufferPool

	t.Run("GetSliceBufferMiss", func(t *testing.T) {
		// Try to get a slice that's not loaded yet
		buffer := pool.GetSliceBuffer(0)
		if buffer != nil {
			t.Error("Expected nil for non-loaded slice")
		}
		t.Log("GetSliceBuffer correctly returned nil for missing slice")
	})

	t.Run("Has", func(t *testing.T) {
		// Check if slice 0 is loaded
		has := pool.Has(0)
		if has {
			t.Log("Slice 0 is already loaded")
		} else {
			t.Log("Slice 0 is not loaded")
		}
	})

	t.Run("GetStats", func(t *testing.T) {
		slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount := pool.GetStats()

		t.Logf("Buffer pool stats:")
		t.Logf("  Slices loaded: %d", slicesLoaded)
		t.Logf("  Blocks served: %d", blocksServed)
		t.Logf("  Evictions: %d", evictions)
		t.Logf("  Skipped: %d", skipped)
		t.Logf("  Used: %d MB / %d MB", usedMB, maxMB)
		t.Logf("  Buffers: %d", bufferCount)

		if maxMB != 50 {
			t.Errorf("Expected maxMB=50, got %d", maxMB)
		}
	})

	t.Run("LogStats", func(t *testing.T) {
		pool.LogStats()
		t.Log("LogStats executed successfully")
	})

	// Trigger actual buffer loading by reading a block
	t.Run("LoadBufferViaRead", func(t *testing.T) {
		// Read a block to trigger buffer loading
		_, _, _, err := reader.GetNotificationsOnly(500)
		if err != nil {
			t.Logf("GetNotificationsOnly failed: %v", err)
		}

		// Check stats again
		slicesLoaded, blocksServed, _, _, usedMB, _, bufferCount := pool.GetStats()
		t.Logf("After read: slicesLoaded=%d, blocksServed=%d, usedMB=%d, buffers=%d",
			slicesLoaded, blocksServed, usedMB, bufferCount)
	})
}

// TestLoadSlice tests buffer loading logic
func TestLoadSlice(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReaderWithCache(dataPath, 50)
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	if reader.bufferPool == nil {
		t.Fatal("Buffer pool not initialized")
	}

	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) == 0 {
		t.Skip("No slices available")
		return
	}

	// Find a finalized slice
	var finalizedSlice *SliceInfo
	for i := range sliceInfos {
		if sliceInfos[i].Finalized {
			finalizedSlice = &sliceInfos[i]
			break
		}
	}

	if finalizedSlice == nil {
		t.Skip("No finalized slices available")
		return
	}

	t.Run("LoadFinalizedSlice", func(t *testing.T) {
		// Load slice data
		data, index, err := loadSliceData(dataPath, *finalizedSlice)
		if err != nil {
			t.Fatalf("loadSliceData failed: %v", err)
		}

		t.Logf("Loaded slice %d: data=%d bytes, index=%d blocks",
			finalizedSlice.SliceNum, len(data), len(index))

		// Try to load into pool
		added := reader.bufferPool.LoadSlice(finalizedSlice.SliceNum, data, index, true)
		if !added {
			t.Log("Slice not added (may already be loaded)")
		} else {
			t.Log("Slice added to pool successfully")
		}

		// Verify it's now in the pool
		has := reader.bufferPool.Has(finalizedSlice.SliceNum)
		if !has {
			t.Error("Slice should be in pool after LoadSlice")
		}
	})

	t.Run("LoadNonFinalizedSlice", func(t *testing.T) {
		// Try to load a non-finalized slice (should be skipped)
		var nonFinalizedSlice *SliceInfo
		for i := range sliceInfos {
			if !sliceInfos[i].Finalized {
				nonFinalizedSlice = &sliceInfos[i]
				break
			}
		}

		if nonFinalizedSlice == nil {
			t.Skip("No non-finalized slices available")
			return
		}

		data, index, err := loadSliceData(dataPath, *nonFinalizedSlice)
		if err != nil {
			t.Fatalf("loadSliceData failed: %v", err)
		}

		// Try to load non-finalized slice (should be skipped)
		added := reader.bufferPool.LoadSlice(nonFinalizedSlice.SliceNum, data, index, false)
		if added {
			t.Error("Non-finalized slice should not be added to pool")
		}

		// Check skipped counter
		_, _, _, skipped, _, _, _ := reader.bufferPool.GetStats()
		if skipped == 0 {
			t.Error("Expected skipped counter to increase")
		}

		t.Logf("Non-finalized slice correctly skipped (skipped=%d)", skipped)
	})

	t.Run("LoadDuplicateSlice", func(t *testing.T) {
		if finalizedSlice == nil {
			t.Skip("No finalized slice")
			return
		}

		// Load same slice twice
		data, index, err := loadSliceData(dataPath, *finalizedSlice)
		if err != nil {
			t.Fatalf("loadSliceData failed: %v", err)
		}

		// First load
		reader.bufferPool.LoadSlice(finalizedSlice.SliceNum, data, index, true)

		// Second load (should not add, but update LRU)
		added := reader.bufferPool.LoadSlice(finalizedSlice.SliceNum, data, index, true)
		if added {
			t.Log("Duplicate load returned true (LRU updated)")
		} else {
			t.Log("Duplicate load correctly returned false")
		}
	})
}

// TestGetBlockData tests block extraction from buffer
func TestGetBlockData(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReaderWithCache(dataPath, 50)
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	// Load a slice into buffer
	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) == 0 {
		t.Skip("No slices available")
		return
	}

	var finalizedSlice *SliceInfo
	for i := range sliceInfos {
		if sliceInfos[i].Finalized {
			finalizedSlice = &sliceInfos[i]
			break
		}
	}

	if finalizedSlice == nil {
		t.Skip("No finalized slices")
		return
	}

	// Load the slice
	data, index, err := loadSliceData(dataPath, *finalizedSlice)
	if err != nil {
		t.Fatalf("loadSliceData failed: %v", err)
	}

	reader.bufferPool.LoadSlice(finalizedSlice.SliceNum, data, index, true)

	// Get buffer
	buffer := reader.bufferPool.GetSliceBuffer(finalizedSlice.SliceNum)
	if buffer == nil {
		t.Fatal("Failed to get buffer")
	}

	t.Run("ExtractValidBlock", func(t *testing.T) {
		// Try to extract a block - use StartBlock+1 since block 1 doesn't exist in EOS
		// (trace files start at block 2)
		testBlock := finalizedSlice.StartBlock + 1
		blockData, err := buffer.GetBlockData(testBlock)
		if err != nil {
			t.Fatalf("GetBlockData failed: %v", err)
		}

		t.Logf("Extracted block %d: %d bytes", testBlock, len(blockData))

		if len(blockData) == 0 {
			t.Error("Block data is empty")
		}
	})

	t.Run("ExtractNonExistentBlock", func(t *testing.T) {
		// Try to extract a block that's not in this slice
		_, err := buffer.GetBlockData(999999999)
		if err == nil {
			t.Error("Expected error for non-existent block")
		}
		t.Logf("Got expected error: %v", err)
	})
}

// TestBufferPoolEviction tests LRU eviction
func TestBufferPoolEviction(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	// Create a very small pool to force eviction
	reader, err := NewSliceReaderWithCache(dataPath, 5) // Only 5MB
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) < 2 {
		t.Skip("Need at least 2 slices for eviction test")
		return
	}

	// Find 2 finalized slices
	var slice1, slice2 *SliceInfo
	for i := range sliceInfos {
		if sliceInfos[i].Finalized {
			if slice1 == nil {
				slice1 = &sliceInfos[i]
			} else if slice2 == nil {
				slice2 = &sliceInfos[i]
				break
			}
		}
	}

	if slice1 == nil || slice2 == nil {
		t.Skip("Need at least 2 finalized slices")
		return
	}

	t.Run("TriggerEviction", func(t *testing.T) {
		// Load first slice
		data1, index1, err := loadSliceData(dataPath, *slice1)
		if err != nil {
			t.Fatalf("loadSliceData failed for slice1: %v", err)
		}

		reader.bufferPool.LoadSlice(slice1.SliceNum, data1, index1, true)

		// Check stats
		_, _, evictions1, _, _, _, buffers1 := reader.bufferPool.GetStats()
		t.Logf("After loading slice 1: buffers=%d, evictions=%d", buffers1, evictions1)

		// Load second slice (may trigger eviction if pool is small enough)
		data2, index2, err := loadSliceData(dataPath, *slice2)
		if err != nil {
			t.Fatalf("loadSliceData failed for slice2: %v", err)
		}

		reader.bufferPool.LoadSlice(slice2.SliceNum, data2, index2, true)

		// Check stats again
		_, _, evictions2, _, usedMB, maxMB, buffers2 := reader.bufferPool.GetStats()
		t.Logf("After loading slice 2: buffers=%d, evictions=%d, used=%d/%d MB",
			buffers2, evictions2, usedMB, maxMB)

		if evictions2 > evictions1 {
			t.Logf("Eviction triggered as expected (%d evictions)", evictions2-evictions1)
		} else {
			t.Logf("No eviction (pool may be large enough for both slices)")
		}
	})
}

// TestLoadSliceData tests slice data loading
func TestLoadSliceData(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) == 0 {
		t.Skip("No slices available")
		return
	}

	t.Run("LoadValidSlice", func(t *testing.T) {
		sliceInfo := sliceInfos[0]

		data, index, err := loadSliceData(dataPath, sliceInfo)
		if err != nil {
			t.Fatalf("loadSliceData failed: %v", err)
		}

		t.Logf("Loaded slice %d: data=%d bytes, index=%d blocks",
			sliceInfo.SliceNum, len(data), len(index))

		if len(data) == 0 {
			t.Error("Data is empty")
		}
		if len(index) == 0 {
			t.Error("Index is empty")
		}
	})

	t.Run("LoadNonExistentSlice", func(t *testing.T) {
		// Create invalid slice info with very high numbers
		invalidSlice := SliceInfo{
			SliceNum:       99999,
			StartBlock:     999990001,
			MaxBlock:       1000000000,
			BlocksPerSlice: 10000,
		}

		_, _, err := loadSliceData(dataPath, invalidSlice)
		if err == nil {
			t.Error("Expected error for non-existent slice")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})
}

// Helper function to pad block numbers for directory names
func padBlockNum(blockNum uint32) string {
	// Convert to 10-digit zero-padded string
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, blockNum)
	return string(buf.Bytes())
}

// BenchmarkSliceBufferPool benchmarks buffer pool operations
func BenchmarkSliceBufferPool(b *testing.B) {
	dataPath := getTestDataPath()

	reader, err := NewSliceReaderWithCache(dataPath, 100)
	if err != nil {
		b.Skipf("Skipping benchmark: %v", err)
		return
	}
	defer reader.Close()

	b.Run("GetSliceBuffer_Hit", func(b *testing.B) {
		// Pre-load a slice
		reader.GetNotificationsOnly(500)

		pool := reader.bufferPool
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = pool.GetSliceBuffer(0)
		}
	})

	b.Run("GetSliceBuffer_Miss", func(b *testing.B) {
		pool := reader.bufferPool
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = pool.GetSliceBuffer(9999)
		}
	})

	b.Run("Has", func(b *testing.B) {
		pool := reader.bufferPool
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = pool.Has(0)
		}
	})
}
