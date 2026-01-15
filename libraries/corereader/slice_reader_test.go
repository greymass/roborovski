package corereader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

// getTestDataPath returns the path to embedded test data
// Can be overridden with ROBO_TEST_DATA_DIR environment variable
func getTestDataPath() string {
	if dir := os.Getenv("ROBO_TEST_DATA_DIR"); dir != "" {
		return dir
	}
	return "testdata/slices"
}

// TestSliceReaderWithRealData tests the slice reader with real coreindex data
func TestSliceReaderWithRealData(t *testing.T) {
	dataPath := getTestDataPath()

	// Check if path exists
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available - run coreindex to generate it")
		return
	}

	// Check if this is slice-based storage

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	// Get slice info
	sliceInfos := reader.GetSliceInfos()
	t.Logf("Loaded %d slices", len(sliceInfos))
	for i, info := range sliceInfos {
		t.Logf("  Slice %d: blocks %d-%d (glob %d-%d, finalized=%v)",
			info.SliceNum, info.StartBlock, info.EndBlock, info.GlobMin, info.GlobMax, info.Finalized)
		if i >= 3 {
			t.Logf("  ... and %d more slices", len(sliceInfos)-4)
			break
		}
	}

	// Test block number (should exist in test data)
	testBlock := uint32(500)

	t.Run("GetNotificationsOnly", func(t *testing.T) {
		notifs, blockID, previous, err := reader.GetNotificationsOnly(testBlock)
		if err != nil {
			t.Fatalf("GetNotificationsOnly failed: %v", err)
		}

		t.Logf("Block %d:", testBlock)
		t.Logf("  Block ID: %s...", blockID[:16])
		t.Logf("  Previous: %s", previous)
		t.Logf("  Notifications: %d accounts", len(notifs))

		if len(notifs) == 0 {
			t.Error("Expected at least one notification")
		}

		// Display first few notifications
		count := 0
		for account, globs := range notifs {
			if count >= 3 {
				break
			}
			accountStr := chain.NameToString(account)
			t.Logf("    %s -> %d actions", accountStr, len(globs))
			count++
		}
	})

	t.Run("GetTransactionIDsOnly", func(t *testing.T) {
		trxIDs, blockID, previous, err := reader.GetTransactionIDsOnly(testBlock, false)
		if err != nil {
			t.Fatalf("GetTransactionIDsOnly failed: %v", err)
		}

		t.Logf("Block %d:", testBlock)
		t.Logf("  Block ID: %s...", blockID[:16])
		t.Logf("  Previous: %s", previous)
		t.Logf("  Transaction IDs: %d", len(trxIDs))

		// Display transaction IDs
		for i, trxID := range trxIDs {
			if i >= 5 {
				t.Logf("    ... and %d more", len(trxIDs)-5)
				break
			}
			t.Logf("    [%d] %s...", i, trxID[:16])
		}
	})

	t.Run("MultipleBlocks", func(t *testing.T) {
		// Test first 10 blocks in dataset
		for blockNum := uint32(500); blockNum <= 509; blockNum++ {
			notifs, blockID, _, err := reader.GetNotificationsOnly(blockNum)
			if err != nil {
				t.Logf("Block %d: %v", blockNum, err)
				continue
			}

			t.Logf("Block %d: %s... -> %d accounts", blockNum, blockID[:16], len(notifs))
		}
	})

	t.Run("CrossSliceBoundary", func(t *testing.T) {
		// Test blocks across slice boundaries
		// Slice 0: blocks 1-10000
		// Slice 1: blocks 10001-20000
		testBlocks := []uint32{9999, 10000, 10001, 10002}

		for _, blockNum := range testBlocks {
			notifs, blockID, _, err := reader.GetNotificationsOnly(blockNum)
			if err != nil {
				t.Errorf("Block %d failed: %v", blockNum, err)
				continue
			}

			t.Logf("Block %d: %s... -> %d accounts", blockNum, blockID[:16], len(notifs))
		}
	})
}

// TestSliceReaderBatch tests batch extraction
func TestSliceReaderBatch(t *testing.T) {
	dataPath := getTestDataPath()

	// Check if path exists
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available - run coreindex to generate it")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("SingleBlock", func(t *testing.T) {
		// Fetch a single block using batch
		results, err := reader.GetRawBlockBatch(500, 500)
		if err != nil {
			t.Fatalf("GetRawBlockBatch failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
			return
		}

		// Compare with direct call
		directNotifs, _, _, err := reader.GetNotificationsOnly(500)
		if err != nil {
			t.Fatalf("GetNotificationsOnly failed: %v", err)
		}

		if results[0].BlockNum != 500 {
			t.Errorf("Expected block 500, got %d", results[0].BlockNum)
		}

		// Note: Batch operations skip blockID for performance (fmt.Sprintf is expensive)
		// So we don't compare blockID here

		if len(results[0].Notifications) != len(directNotifs) {
			t.Errorf("Notifications count mismatch: batch=%d, direct=%d", len(results[0].Notifications), len(directNotifs))
		}

		t.Logf("Single block batch: block=%d, notifs=%d accounts",
			results[0].BlockNum, len(results[0].Notifications))
	})

	t.Run("MultipleBlocks", func(t *testing.T) {
		// Fetch 10 consecutive blocks
		results, err := reader.GetRawBlockBatch(500, 509)
		if err != nil {
			t.Fatalf("GetRawBlockBatch failed: %v", err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
			return
		}

		// Verify results are in order
		for i, result := range results {
			expectedBlock := uint32(500 + i)
			if result.BlockNum != expectedBlock {
				t.Errorf("Result[%d]: expected block %d, got %d", i, expectedBlock, result.BlockNum)
			}
			t.Logf("  [%d] block=%d, notifs=%d accounts",
				i, result.BlockNum, len(result.Notifications))
		}
	})

	t.Run("CrossSliceBatch", func(t *testing.T) {
		// Fetch blocks across slice boundary (9995-10005)
		results, err := reader.GetRawBlockBatch(9995, 10005)
		if err != nil {
			t.Fatalf("GetRawBlockBatch failed: %v", err)
		}

		if len(results) != 11 {
			t.Errorf("Expected 11 results, got %d", len(results))
			return
		}

		// Verify continuity across slice boundary
		for i, result := range results {
			expectedBlock := uint32(9995 + i)
			if result.BlockNum != expectedBlock {
				t.Errorf("Result[%d]: expected block %d, got %d", i, expectedBlock, result.BlockNum)
			}

			// Log blocks around the boundary
			if result.BlockNum >= 9998 && result.BlockNum <= 10002 {
				t.Logf("  block=%d, notifs=%d accounts (crossing slice boundary)",
					result.BlockNum, len(result.Notifications))
			}
		}
	})

	t.Run("InvalidRange", func(t *testing.T) {
		// Test invalid range (end < start)
		_, err = reader.GetRawBlockBatch(600, 500)
		if err == nil {
			t.Error("Expected error for invalid range (end < start), got nil")
		} else {
			t.Logf("Got expected error for invalid range: %v", err)
		}
	})
}

// TestSliceReaderErrorHandling tests error conditions
func TestSliceReaderErrorHandling(t *testing.T) {
	t.Run("InvalidPath", func(t *testing.T) {
		_, err := NewSliceReader("/nonexistent/path")
		if err == nil {
			t.Error("Expected error for invalid path")
		}
	})

	t.Run("BlockNotFound", func(t *testing.T) {
		dataPath := getTestDataPath()

		// Check if path exists
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			t.Skip("Test data not available")
			return
		}

		reader, err := NewSliceReader(dataPath)
		if err != nil {
			t.Skip("No test data available")
			return
		}
		defer reader.Close()

		// Try to read a very high block number that doesn't exist
		_, _, _, err = reader.GetNotificationsOnly(999999999)
		if err == nil {
			t.Error("Expected error for non-existent block")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})
}

// BenchmarkSliceReaderMethods compares performance of slice reader methods
func BenchmarkSliceReaderMethods(b *testing.B) {
	dataPath := getTestDataPath()

	// Check if path exists
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		b.Skip("Test data not available - run coreindex to generate it")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		b.Skipf("Skipping benchmark - no data: %v", err)
		return
	}
	defer reader.Close()

	testBlock := uint32(1000)

	b.Run("GetNotificationsOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, err := reader.GetNotificationsOnly(testBlock)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetTransactionIDsOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, err := reader.GetTransactionIDsOnly(testBlock, false)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetRawBlockBatch_10Blocks", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := reader.GetRawBlockBatch(1000, 1009)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetRawBlockBatch_100Blocks", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := reader.GetRawBlockBatch(1000, 1099)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSliceLookup(b *testing.B) {
	sliceCount := 47784
	slices := make([]SliceInfo, sliceCount)
	for i := 0; i < sliceCount; i++ {
		slices[i] = SliceInfo{
			SliceNum:   uint32(i),
			StartBlock: uint32(i * 10000),
			MaxBlock:   uint32((i+1)*10000 - 1),
			EndBlock:   uint32((i+1)*10000 - 1),
			Finalized:  true,
		}
	}
	sm := newSharedSliceMetadata(slices)

	targetSlices := []uint32{0, 1000, 23892, 40000, 47783}

	b.Run("LinearScan", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, target := range targetSlices {
				count := sm.getSliceCount()
				for j := 0; j < count; j++ {
					s := sm.getSlice(j)
					if s.SliceNum == target {
						_ = s.Finalized
						break
					}
				}
			}
		}
	})

	b.Run("MapLookup", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, target := range targetSlices {
				s := sm.getSliceByNum(target)
				if s != nil {
					_ = s.Finalized
				}
			}
		}
	})
}

func TestNewSliceReaderWithCache_ErrorPaths(t *testing.T) {
	t.Run("InvalidPath", func(t *testing.T) {
		_, err := NewSliceReaderWithCache("/nonexistent/path/to/storage", 0)
		if err == nil {
			t.Error("Expected error for non-existent path")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("PathExistsButNoSlicesJSON", func(t *testing.T) {
		tmpDir := t.TempDir()

		_, err := NewSliceReaderWithCache(tmpDir, 0)
		if err == nil {
			t.Error("Expected error for missing slices.json")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("InvalidSlicesJSON", func(t *testing.T) {
		tmpDir := t.TempDir()
		slicesPath := filepath.Join(tmpDir, "slices.json")

		err := os.WriteFile(slicesPath, []byte("invalid json{{{"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		_, err = NewSliceReaderWithCache(tmpDir, 0)
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("WithCacheEnabled", func(t *testing.T) {
		dataPath := getTestDataPath()

		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			t.Skip("Test data not available")
			return
		}

		reader, err := NewSliceReaderWithCache(dataPath, 100)
		if err != nil {
			t.Skipf("No test data available: %v", err)
			return
		}
		defer reader.Close()

		if reader.bufferPool == nil {
			t.Fatal("Expected slice buffer pool to be initialized")
		}

		t.Logf("Created reader with 100MB cache")
	})
}

func TestGetActionsByGlobalSeqs_ErrorPaths(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("EmptyGlobSeqList", func(t *testing.T) {
		results, _, err := reader.GetActionsByGlobalSeqs([]uint64{})
		if err != nil {
			t.Logf("Error with empty list: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results for empty list, got %d", len(results))
		}
	})

	t.Run("SingleGlobSeq", func(t *testing.T) {
		results, _, err := reader.GetActionsByGlobalSeqs([]uint64{1000})
		if err != nil {
			t.Logf("Error: %v", err)
		}
		t.Logf("Single glob seq lookup returned %d results", len(results))

		if len(results) > 0 {
			t.Logf("Found action: receiver=%s", results[0].Receiver)
		}
	})

	t.Run("MultipleGlobSeqs", func(t *testing.T) {
		globSeqs := []uint64{100, 200, 300, 400, 500}
		results, _, err := reader.GetActionsByGlobalSeqs(globSeqs)
		if err != nil {
			t.Logf("Error: %v", err)
		}
		t.Logf("Multiple glob seq lookup (%d seqs) returned %d results", len(globSeqs), len(results))

		if len(results) > 0 {
			for i, result := range results {
				t.Logf("  [%d] receiver=%s, act=%s", i, result.Receiver, result.Act.Name)
				if i >= 2 {
					break
				}
			}
		}
	})

	t.Run("NonExistentGlobSeqs", func(t *testing.T) {
		results, _, err := reader.GetActionsByGlobalSeqs([]uint64{9999999999, 9999999998})
		if err != nil {
			t.Logf("Error: %v", err)
		}
		t.Logf("Non-existent glob seq lookup returned %d results", len(results))

		if len(results) > 0 {
			t.Logf("Unexpectedly found %d results for non-existent glob seqs", len(results))
		}
	})

	t.Run("MixedExistentAndNonExistent", func(t *testing.T) {
		globSeqs := []uint64{100, 9999999999, 200, 9999999998, 300}
		results, _, err := reader.GetActionsByGlobalSeqs(globSeqs)
		if err != nil {
			t.Logf("Error: %v", err)
		}
		t.Logf("Mixed glob seq lookup returned %d results (out of %d requested)",
			len(results), len(globSeqs))
	})
}

func TestGetNotificationsWithActionMetadata_ErrorPaths(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("ValidBlock", func(t *testing.T) {
		notifs, actions, blockID, previous, err := reader.GetNotificationsWithActionMetadata(500)
		if err != nil {
			t.Fatalf("GetNotificationsWithActionMetadata failed: %v", err)
		}

		t.Logf("Block 500: blockID=%s..., previous=%s, notifs=%d accounts, actions=%d",
			blockID[:16], previous, len(notifs), len(actions))

		count := 0
		for account, globIndices := range notifs {
			if count >= 2 {
				break
			}
			accountStr := chain.NameToString(account)
			t.Logf("  %s -> %d action refs", accountStr, len(globIndices))
			count++
		}

		for i, action := range actions {
			if i >= 3 {
				t.Logf("    ... and %d more actions", len(actions)-3)
				break
			}
			t.Logf("    [%d] contract=%s, action=%s, glob=%d",
				i, chain.NameToString(action.Contract), chain.NameToString(action.Action), action.GlobalSeq)
		}
	})

	t.Run("BlockNotFound", func(t *testing.T) {
		_, _, _, _, err := reader.GetNotificationsWithActionMetadata(999999999)
		if err == nil {
			t.Error("Expected error for non-existent block")
		}
		t.Logf("Got expected error: %v", err)
	})
}

func TestGetStateProps_ErrorPaths(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("GetHeadAndLib", func(t *testing.T) {
		head, lib, err := reader.GetStateProps(false)
		if err != nil {
			t.Fatalf("GetStateProps failed: %v", err)
		}

		t.Logf("State props:")
		t.Logf("  HEAD block: %d", head)
		t.Logf("  LIB block: %d", lib)

		if head < lib {
			t.Errorf("HEAD (%d) should be >= LIB (%d)", head, lib)
		}
	})
}

func TestGetTransactionIDsOnly_EdgeCases(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("WithInferredTrx_True", func(t *testing.T) {
		trxIDs, blockID, previous, err := reader.GetTransactionIDsOnly(500, true)
		if err != nil {
			t.Fatalf("GetTransactionIDsOnly(includeInferred=true) failed: %v", err)
		}

		t.Logf("Block 500 (with inferred): blockID=%s..., previous=%s, trxs=%d",
			blockID[:16], previous, len(trxIDs))
	})

	t.Run("WithInferredTrx_False", func(t *testing.T) {
		trxIDs, blockID, previous, err := reader.GetTransactionIDsOnly(500, false)
		if err != nil {
			t.Fatalf("GetTransactionIDsOnly(includeInferred=false) failed: %v", err)
		}

		t.Logf("Block 500 (without inferred): blockID=%s..., previous=%s, trxs=%d",
			blockID[:16], previous, len(trxIDs))
	})

	t.Run("CompareInferredVsNot", func(t *testing.T) {
		withInferred, _, _, err := reader.GetTransactionIDsOnly(500, true)
		if err != nil {
			t.Fatal(err)
		}

		withoutInferred, _, _, err := reader.GetTransactionIDsOnly(500, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(withInferred) < len(withoutInferred) {
			t.Errorf("Expected more trxs with inferred, got %d vs %d",
				len(withInferred), len(withoutInferred))
		}

		t.Logf("Transaction count: with_inferred=%d, without_inferred=%d",
			len(withInferred), len(withoutInferred))
	})
}

func TestGetRawBlockBatch_EdgeCases(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("EndBeforeStart", func(t *testing.T) {
		_, err := reader.GetRawBlockBatch(600, 500)
		if err == nil {
			t.Error("Expected error for end < start")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("SameStartAndEnd", func(t *testing.T) {
		results, err := reader.GetRawBlockBatch(500, 500)
		if err != nil {
			t.Fatalf("GetRawBlockBatch(500, 500) failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for same start/end, got %d", len(results))
		}

		if len(results) > 0 && results[0].BlockNum != 500 {
			t.Errorf("Expected block 500, got %d", results[0].BlockNum)
		}

		t.Logf("Single block batch: block=%d, notifs=%d",
			results[0].BlockNum, len(results[0].Notifications))
	})

	t.Run("VeryLargeRange", func(t *testing.T) {
		results, err := reader.GetRawBlockBatch(500, 1500)
		if err != nil {
			t.Logf("Large range failed (expected if data not available): %v", err)
			return
		}

		expectedCount := 1001
		if len(results) != expectedCount {
			t.Logf("Expected %d results, got %d (some blocks may not exist)",
				expectedCount, len(results))
		} else {
			t.Logf("Large range successful: got %d blocks", len(results))
		}
	})

	t.Run("PartiallyAvailableRange", func(t *testing.T) {
		results, err := reader.GetRawBlockBatch(999999, 999999)
		if err != nil {
			t.Logf("Got expected error for unavailable range: %v", err)
			return
		}

		if len(results) > 0 {
			t.Logf("Unexpectedly got %d results for high block numbers", len(results))
		}
	})
}

func TestGetRawActionsFiltered_Unimplemented(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("ReturnsUnimplementedError", func(t *testing.T) {
		_, _, _, err := reader.GetRawActionsFiltered(500, "", "")
		if err == nil {
			t.Error("Expected error for unimplemented function")
		}

		expectedMsg := "GetRawActionsFiltered not yet implemented for slice-based storage"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
		}

		t.Logf("Confirmed unimplemented: %v", err)
	})
}

func TestSliceCacheStats(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	t.Run("WithCacheEnabled", func(t *testing.T) {
		reader, err := NewSliceReaderWithCache(dataPath, 100)
		if err != nil {
			t.Skipf("No test data available: %v", err)
			return
		}
		defer reader.Close()

		slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount := reader.GetBufferPoolStats()
		t.Logf("Buffer pool stats:")
		t.Logf("  Slices loaded: %d", slicesLoaded)
		t.Logf("  Blocks served: %d", blocksServed)
		t.Logf("  Evictions: %d", evictions)
		t.Logf("  Skipped: %d", skipped)
		t.Logf("  Used: %d MB / %d MB", usedMB, maxMB)
		t.Logf("  Buffers: %d", bufferCount)

		reader.LogBufferPoolStats()
		t.Logf("Buffer pool stats logged successfully")
	})

	t.Run("WithoutCache", func(t *testing.T) {
		reader, err := NewSliceReader(dataPath)
		if err != nil {
			t.Skipf("No test data available: %v", err)
			return
		}
		defer reader.Close()

		slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount := reader.GetBufferPoolStats()
		if usedMB != 0 || maxMB != 0 || bufferCount != 0 {
			t.Errorf("Expected zero buffer pool stats without pool, got: used=%d, max=%d, buffers=%d",
				usedMB, maxMB, bufferCount)
		}
		t.Logf("No-pool stats: slicesLoaded=%d, blocksServed=%d, evictions=%d, skipped=%d, used=%d/%d MB, buffers=%d",
			slicesLoaded, blocksServed, evictions, skipped, usedMB, maxMB, bufferCount)

		reader.LogBufferPoolStats()
		t.Logf("No-pool stats logged successfully (no-op)")
	})
}

func TestSliceReaderLoading_ErrorPaths(t *testing.T) {
	dataPath := getTestDataPath()

	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Skip("Test data not available")
		return
	}

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("No test data available: %v", err)
		return
	}
	defer reader.Close()

	t.Run("LoadMultipleSlices", func(t *testing.T) {
		blocks := []uint32{500, 10500, 20500}

		for _, blockNum := range blocks {
			notifs, blockID, _, err := reader.GetNotificationsOnly(blockNum)
			if err != nil {
				t.Logf("Block %d not available: %v", blockNum, err)
				continue
			}

			t.Logf("Block %d: blockID=%s..., notifs=%d (slice loaded)",
				blockNum, blockID[:16], len(notifs))
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		done := make(chan bool, 5)

		for i := 0; i < 5; i++ {
			go func(id int) {
				defer func() { done <- true }()

				notifs, blockID, _, err := reader.GetNotificationsOnly(500)
				if err != nil {
					t.Logf("Goroutine %d failed: %v", id, err)
					return
				}

				t.Logf("Goroutine %d: blockID=%s..., notifs=%d",
					id, blockID[:16], len(notifs))
			}(i)
		}

		for i := 0; i < 5; i++ {
			<-done
		}

		t.Logf("Concurrent access test completed")
	})
}
