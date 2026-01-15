package corereader

import (
	"os"
	"testing"
	"time"
)

// =============================================================================
// Advanced Coverage Tests - Target specific low-coverage functions
// Focus on: loadLocked eviction, findSliceForBlock edge cases, readBlockData paths
// =============================================================================

// =============================================================================
// slice_buffer.go: loadLocked (42% coverage) - Test eviction paths
// =============================================================================

func TestSliceBufferPoolEvictionPaths(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	// Create a VERY small pool (1MB) to force eviction
	reader, err := NewSliceReaderWithCache(dataPath, 1) // Only 1MB!
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) < 3 {
		t.Skip("Need at least 3 slices for eviction test")
		return
	}

	// Find 3 finalized slices
	var slice1, slice2, slice3 *SliceInfo
	for i := range sliceInfos {
		if sliceInfos[i].Finalized {
			if slice1 == nil {
				slice1 = &sliceInfos[i]
			} else if slice2 == nil {
				slice2 = &sliceInfos[i]
			} else if slice3 == nil {
				slice3 = &sliceInfos[i]
				break
			}
		}
	}

	if slice1 == nil || slice2 == nil || slice3 == nil {
		t.Skip("Need at least 3 finalized slices")
		return
	}

	t.Run("ForceMultipleEvictions", func(t *testing.T) {
		// Read blocks from 3 different slices to trigger multiple evictions
		blocks := []struct {
			sliceNum uint32
			blockNum uint32
		}{
			{slice1.SliceNum, slice1.StartBlock},
			{slice2.SliceNum, slice2.StartBlock},
			{slice3.SliceNum, slice3.StartBlock},
		}

		for i, b := range blocks {
			t.Logf("Reading block %d from slice %d (read %d/3)", b.blockNum, b.sliceNum, i+1)

			_, _, _, err := reader.GetNotificationsOnly(b.blockNum)
			if err != nil {
				t.Logf("GetNotificationsOnly failed: %v", err)
				continue
			}

			// Check stats after each read
			_, _, evictions, _, usedMB, maxMB, bufferCount := reader.GetBufferPoolStats()
			t.Logf("  After read %d: buffers=%d, evictions=%d, used=%d/%d MB",
				i+1, bufferCount, evictions, usedMB, maxMB)
		}

		// Final stats - should show evictions with small pool
		_, _, finalEvictions, _, finalUsed, finalMax, finalBuffers := reader.GetBufferPoolStats()
		t.Logf("Final: buffers=%d, evictions=%d, used=%d/%d MB",
			finalBuffers, finalEvictions, finalUsed, finalMax)

		if finalEvictions == 0 && finalBuffers > 1 {
			t.Log("Note: No evictions occurred (slices may be smaller than 1MB each)")
		}
	})

	t.Run("EvictionWithMunmapFailure", func(t *testing.T) {
		// This path is hard to test directly as we can't easily force munmap to fail
		// But we exercise the code by loading slices normally

		// Load multiple slices to test eviction code paths
		for i := 0; i < 3; i++ {
			testBlock := slice1.StartBlock + uint32(i*100)
			if testBlock > slice1.EndBlock {
				break
			}

			_, _, _, err := reader.GetNotificationsOnly(testBlock)
			if err != nil {
				t.Logf("Read failed: %v", err)
			}
		}

		t.Log("Eviction code paths exercised (munmap failure path is rare)")
	})
}

// =============================================================================
// slice_reader.go: findSliceForBlock (46% coverage) - Test all branches
// =============================================================================

func TestFindSliceForBlock_AllPaths(t *testing.T) {
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

	t.Run("FindInFirstSlice", func(t *testing.T) {
		// Block 1 doesn't exist in EOS (trace files start at block 2)
		firstBlock := sliceInfos[0].StartBlock + 1
		_, _, _, err := reader.GetNotificationsOnly(firstBlock)
		if err != nil {
			t.Errorf("Failed to find first block: %v", err)
		} else {
			t.Logf("Found block %d in first slice", firstBlock)
		}
	})

	t.Run("FindInMiddleSlice", func(t *testing.T) {
		if len(sliceInfos) < 3 {
			t.Skip("Need at least 3 slices")
			return
		}

		midIdx := len(sliceInfos) / 2
		midBlock := sliceInfos[midIdx].StartBlock
		_, _, _, err := reader.GetNotificationsOnly(midBlock)
		if err != nil {
			t.Errorf("Failed to find middle block: %v", err)
		} else {
			t.Logf("Found block %d in middle slice (slice %d)", midBlock, sliceInfos[midIdx].SliceNum)
		}
	})

	t.Run("FindInLastSlice", func(t *testing.T) {
		lastSlice := sliceInfos[len(sliceInfos)-1]
		lastBlock := lastSlice.StartBlock
		_, _, _, err := reader.GetNotificationsOnly(lastBlock)
		if err != nil {
			t.Errorf("Failed to find last block: %v", err)
		} else {
			t.Logf("Found block %d in last slice", lastBlock)
		}
	})

	t.Run("BlockBeforeAllSlices", func(t *testing.T) {
		// Try to find a block before the first slice
		beforeFirstBlock := sliceInfos[0].StartBlock - 1
		if beforeFirstBlock < 1 {
			beforeFirstBlock = 1
		}

		_, _, _, err := reader.GetNotificationsOnly(beforeFirstBlock)
		if err == nil {
			t.Logf("Block %d found (may exist in slice)", beforeFirstBlock)
		} else {
			t.Logf("Block %d not found (expected): %v", beforeFirstBlock, err)
		}
	})

	t.Run("BlockAfterAllSlices", func(t *testing.T) {
		// Try to find a block after the last slice
		afterLastBlock := sliceInfos[len(sliceInfos)-1].MaxBlock + 1

		_, _, _, err := reader.GetNotificationsOnly(afterLastBlock)
		if err == nil {
			t.Logf("Block %d found (may exist in active slice)", afterLastBlock)
		} else {
			t.Logf("Block %d not found (expected): %v", afterLastBlock, err)
		}
	})

	t.Run("BlockInGap", func(t *testing.T) {
		// Try to find a block in a gap between slices (if any)
		if len(sliceInfos) < 2 {
			t.Skip("Need at least 2 slices")
			return
		}

		// Check if there's a gap
		gap := sliceInfos[1].StartBlock - sliceInfos[0].EndBlock
		if gap > 1 {
			gapBlock := sliceInfos[0].EndBlock + 1
			_, _, _, err := reader.GetNotificationsOnly(gapBlock)
			if err == nil {
				t.Logf("Block %d in gap found (slices may be contiguous)", gapBlock)
			} else {
				t.Logf("Block %d in gap not found: %v", gapBlock, err)
			}
		} else {
			t.Log("No gaps between slices")
		}
	})
}

// =============================================================================
// slice_reader.go: findSliceForGlob (40% coverage) - Test fallback paths
// =============================================================================

func TestFindSliceForGlob_FallbackPaths(t *testing.T) {
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
	if len(sliceInfos) < 2 {
		t.Skip("Need at least 2 slices")
		return
	}

	t.Run("FindGlobInActiveSlice", func(t *testing.T) {
		// Try to find a glob in the last slice (active)
		lastSlice := sliceInfos[len(sliceInfos)-1]
		if lastSlice.GlobMin == 0 {
			t.Skip("Last slice has no glob info")
			return
		}

		testGlob := lastSlice.GlobMin + 10
		actions, _, err := reader.GetActionsByGlobalSeqs([]uint64{testGlob})
		if err != nil {
			t.Logf("Glob %d in active slice not found: %v", testGlob, err)
		} else {
			t.Logf("Found %d actions for glob %d in active slice", len(actions), testGlob)
		}
	})

	t.Run("FindGlobInPenultimateSlice", func(t *testing.T) {
		if len(sliceInfos) < 2 {
			t.Skip("Need at least 2 slices")
			return
		}

		// Second-to-last slice (also considered active)
		penultimateSlice := sliceInfos[len(sliceInfos)-2]
		if penultimateSlice.GlobMin == 0 {
			t.Skip("Penultimate slice has no glob info")
			return
		}

		testGlob := penultimateSlice.GlobMin + 10
		actions, _, err := reader.GetActionsByGlobalSeqs([]uint64{testGlob})
		if err != nil {
			t.Logf("Glob %d in penultimate slice not found: %v", testGlob, err)
		} else {
			t.Logf("Found %d actions for glob %d in penultimate slice", len(actions), testGlob)
		}
	})

	t.Run("FindGlobInFinalizedSlice", func(t *testing.T) {
		if len(sliceInfos) < 3 {
			t.Skip("Need at least 3 slices")
			return
		}

		// Use a finalized slice (not last 2)
		finalizedSlice := sliceInfos[0]
		if finalizedSlice.GlobMin == 0 {
			t.Skip("Finalized slice has no glob info")
			return
		}

		testGlob := finalizedSlice.GlobMin + 10
		actions, _, err := reader.GetActionsByGlobalSeqs([]uint64{testGlob})
		if err != nil {
			t.Logf("Glob %d in finalized slice not found: %v", testGlob, err)
		} else {
			t.Logf("Found %d actions for glob %d in finalized slice", len(actions), testGlob)
		}
	})
}

// =============================================================================
// slice_reader.go: readBlockData (25% coverage) - Test index reload path
// =============================================================================

func TestReadBlockData_IndexReloadPath(t *testing.T) {
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

	t.Run("ReadManyBlocksToExerciseReload", func(t *testing.T) {
		// Read many blocks sequentially to exercise the index caching/reload logic
		firstSlice := sliceInfos[0]
		blocksToRead := 100
		if firstSlice.EndBlock-firstSlice.StartBlock < uint32(blocksToRead) {
			blocksToRead = int(firstSlice.EndBlock - firstSlice.StartBlock)
		}

		successCount := 0
		for i := 0; i < blocksToRead; i++ {
			blockNum := firstSlice.StartBlock + uint32(i)
			_, _, _, err := reader.GetNotificationsOnly(blockNum)
			if err != nil {
				if i < 10 {
					t.Logf("Block %d failed: %v", blockNum, err)
				}
			} else {
				successCount++
			}
		}

		t.Logf("Successfully read %d/%d blocks (index reload paths exercised)", successCount, blocksToRead)
	})

	t.Run("ReadBlocksFromMultipleSlices", func(t *testing.T) {
		// Read blocks from different slices to exercise slice reader caching
		if len(sliceInfos) < 2 {
			t.Skip("Need multiple slices")
			return
		}

		for i := 0; i < len(sliceInfos) && i < 5; i++ {
			testBlock := sliceInfos[i].StartBlock
			_, _, _, err := reader.GetNotificationsOnly(testBlock)
			if err != nil {
				t.Logf("Slice %d block %d failed: %v", i, testBlock, err)
			} else {
				t.Logf("Slice %d block %d success", i, testBlock)
			}
		}

		t.Log("Multi-slice read completed (cache paths exercised)")
	})
}

// =============================================================================
// slice_reader.go: RefreshSliceMetadata (45% coverage) - Test update paths
// =============================================================================

func TestRefreshSliceMetadata_UpdatePaths(t *testing.T) {
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

	t.Run("RefreshMultipleTimes", func(t *testing.T) {
		sliceInfos1 := reader.GetSliceInfos()
		t.Logf("Initial: %d slices", len(sliceInfos1))

		// Refresh 1
		err := reader.RefreshSliceMetadata()
		if err != nil {
			t.Logf("Refresh 1 error: %v", err)
		}
		sliceInfos2 := reader.GetSliceInfos()
		t.Logf("After refresh 1: %d slices", len(sliceInfos2))

		// Small delay
		time.Sleep(100 * time.Millisecond)

		// Refresh 2
		err = reader.RefreshSliceMetadata()
		if err != nil {
			t.Logf("Refresh 2 error: %v", err)
		}
		sliceInfos3 := reader.GetSliceInfos()
		t.Logf("After refresh 2: %d slices", len(sliceInfos3))

		// Compare results
		if len(sliceInfos2) != len(sliceInfos1) {
			t.Logf("Slice count changed: %d → %d", len(sliceInfos1), len(sliceInfos2))
		}
		if len(sliceInfos3) != len(sliceInfos2) {
			t.Logf("Slice count changed: %d → %d", len(sliceInfos2), len(sliceInfos3))
		}
	})

	t.Run("CheckLastSliceUpdates", func(t *testing.T) {
		if len(reader.GetSliceInfos()) == 0 {
			t.Skip("No slices")
			return
		}

		lastSlice1 := reader.GetSliceInfos()[len(reader.GetSliceInfos())-1]
		t.Logf("Last slice before refresh: %d-%d, endBlock=%d",
			lastSlice1.StartBlock, lastSlice1.MaxBlock, lastSlice1.EndBlock)

		err := reader.RefreshSliceMetadata()
		if err != nil {
			t.Logf("Refresh error: %v", err)
		}

		lastSlice2 := reader.GetSliceInfos()[len(reader.GetSliceInfos())-1]
		t.Logf("Last slice after refresh: %d-%d, endBlock=%d",
			lastSlice2.StartBlock, lastSlice2.MaxBlock, lastSlice2.EndBlock)

		if lastSlice2.EndBlock != lastSlice1.EndBlock {
			t.Logf("Last slice endBlock updated: %d → %d", lastSlice1.EndBlock, lastSlice2.EndBlock)
		}
	})
}

// =============================================================================
// slice_reader.go: UpdateGlobRangeIndexWithNewSlices (11% coverage)
// =============================================================================

func TestUpdateGlobRangeIndex_AllPaths(t *testing.T) {
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

	if reader.globRangeIndex == nil {
		t.Skip("No glob range index")
		return
	}

	t.Run("UpdateMultipleTimesWithCooldown", func(t *testing.T) {
		reader.SetGlobRangeUpdateCooldown(10 * time.Millisecond)

		// First update
		err := reader.UpdateGlobRangeIndexWithNewSlices()
		if err != nil {
			t.Logf("Update 1 error: %v", err)
		}

		stats1 := reader.GetGlobRangeIndexStats()
		t.Logf("After update 1: indexed=%d, highest=%d",
			stats1.IndexedSlices, stats1.HighestIndexed)

		// Immediate second update (should skip due to cooldown)
		err = reader.UpdateGlobRangeIndexWithNewSlices()
		if err != nil {
			t.Logf("Update 2 error: %v", err)
		}

		stats2 := reader.GetGlobRangeIndexStats()
		t.Logf("After update 2 (cooldown): indexed=%d, highest=%d",
			stats2.IndexedSlices, stats2.HighestIndexed)

		// Wait for cooldown
		time.Sleep(20 * time.Millisecond)

		// Third update (should execute)
		err = reader.UpdateGlobRangeIndexWithNewSlices()
		if err != nil {
			t.Logf("Update 3 error: %v", err)
		}

		stats3 := reader.GetGlobRangeIndexStats()
		t.Logf("After update 3 (post-cooldown): indexed=%d, highest=%d",
			stats3.IndexedSlices, stats3.HighestIndexed)
	})

	t.Run("CheckForNewSlices", func(t *testing.T) {
		// Refresh metadata first
		reader.RefreshSliceMetadata()

		// Then update index
		err := reader.UpdateGlobRangeIndexWithNewSlices()
		if err != nil {
			t.Logf("Update error: %v", err)
		}

		stats := reader.GetGlobRangeIndexStats()
		t.Logf("Final stats: indexed=%d, highest=%d, added=%d",
			stats.IndexedSlices, stats.HighestIndexed, stats.SlicesAddedSinceLastLog)
	})
}

// =============================================================================
// slice_buffer.go: GetBlockData error paths (78% coverage)
// =============================================================================

func TestGetBlockData_ErrorPaths(t *testing.T) {
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

	sliceInfos := reader.GetSliceInfos()
	if len(sliceInfos) == 0 {
		t.Skip("No slices")
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
		t.Skip("No finalized slices")
		return
	}

	// Load the slice into buffer
	data, index, err := loadSliceData(dataPath, *finalizedSlice)
	if err != nil {
		t.Fatalf("loadSliceData failed: %v", err)
		return
	}

	reader.bufferPool.LoadSlice(finalizedSlice.SliceNum, data, index, true)

	buffer := reader.bufferPool.GetSliceBuffer(finalizedSlice.SliceNum)
	if buffer == nil {
		t.Fatal("Failed to get buffer")
	}

	t.Run("BlockNotInSlice", func(t *testing.T) {
		// Try to get a block that's definitely not in this slice
		nonExistentBlock := finalizedSlice.MaxBlock + 10000
		_, err := buffer.GetBlockData(nonExistentBlock)
		if err == nil {
			t.Error("Expected error for non-existent block")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})

	t.Run("ExtractMultipleBlocks", func(t *testing.T) {
		// Extract several blocks to exercise decompression paths
		successCount := 0
		testBlocks := []uint32{
			finalizedSlice.StartBlock,
			finalizedSlice.StartBlock + 100,
			finalizedSlice.StartBlock + 500,
		}

		for _, blockNum := range testBlocks {
			if blockNum > finalizedSlice.EndBlock {
				continue
			}

			blockData, err := buffer.GetBlockData(blockNum)
			if err != nil {
				t.Logf("Block %d failed: %v", blockNum, err)
			} else {
				t.Logf("Block %d: %d bytes", blockNum, len(blockData))
				successCount++
			}
		}

		t.Logf("Successfully extracted %d/%d blocks", successCount, len(testBlocks))
	})
}

// =============================================================================
// Integration: Test complete read workflow with buffer pool
// =============================================================================

func TestCompleteReadWorkflowWithBufferPool(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReaderWithCache(dataPath, 100)
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	// Read blocks sequentially to build up buffer
	startBlock := uint32(500)
	endBlock := startBlock + 50

	successCount := 0
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		_, _, _, err := reader.GetNotificationsOnly(blockNum)
		if err != nil {
			if blockNum == startBlock {
				t.Logf("First block error: %v", err)
			}
		} else {
			successCount++
		}
	}

	t.Logf("Read %d/%d blocks successfully", successCount, endBlock-startBlock+1)

	// Check buffer pool stats
	slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount := reader.GetBufferPoolStats()
	t.Logf("Buffer pool: slicesLoaded=%d, blocksServed=%d, evictions=%d, skipped=%d, used=%d/%d MB, buffers=%d",
		slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount)

	// Re-read same blocks (should hit buffer)
	secondPassStart := startBlock
	secondPassEnd := startBlock + 10

	for blockNum := secondPassStart; blockNum <= secondPassEnd; blockNum++ {
		_, _, _, err := reader.GetNotificationsOnly(blockNum)
		if err != nil {
			t.Logf("Second pass block %d error: %v", blockNum, err)
		}
	}

	// Check stats again
	_, blocksServed2, _, _, _, _, _ := reader.GetBufferPoolStats()
	bufferedHits := blocksServed2 - blocksServed
	t.Logf("Second pass: %d additional blocks served from buffer", bufferedHits)
}
