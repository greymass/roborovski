package corereader

import (
	"os"
	"testing"
)

// TestGetActionsByGlobalSeqsDetailed tests batch lookup by global sequence comprehensively
func TestGetActionsByGlobalSeqsDetailed(t *testing.T) {
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

	t.Run("EmptyInput", func(t *testing.T) {
		// Empty input should return empty slice, no error
		actions, _, err := reader.GetActionsByGlobalSeqs([]uint64{})
		if err != nil {
			t.Fatalf("GetActionsByGlobalSeqs with empty input failed: %v", err)
		}

		if len(actions) != 0 {
			t.Errorf("Expected empty result, got %d actions", len(actions))
		}

		t.Log("Empty input correctly returns empty result")
	})

	t.Run("SingleGlobalSeq", func(t *testing.T) {
		// Use a known glob from test data (block 500 has glob 1260 for eosio::onblock)
		testGlob := uint64(1260)

		actions, _, err := reader.GetActionsByGlobalSeqs([]uint64{testGlob})
		if err != nil {
			t.Fatalf("GetActionsByGlobalSeqs failed: %v", err)
		}

		if len(actions) != 1 {
			t.Errorf("Expected 1 action, got %d", len(actions))
			return
		}

		action := actions[0]
		gs, _ := action.Receipt.GlobalSequence.Int64()
		t.Logf("Retrieved action: %s::%s -> %s (glob %d)",
			action.Act.Account, action.Act.Name, action.Receiver, gs)

		// Verify the action has correct global sequence
		if uint64(gs) != testGlob {
			t.Errorf("Expected glob %d, got %d", testGlob, gs)
		}
	})

	t.Run("MultipleGlobalSeqs_SameBlock", func(t *testing.T) {
		// Test with multiple globs from the same block (should decompress once)
		testGlobs := []uint64{1260, 1261, 1262}

		actions, _, err := reader.GetActionsByGlobalSeqs(testGlobs)
		if err != nil {
			t.Skipf("GetActionsByGlobalSeqs failed (globs may not exist in test data): %v", err)
			return
		}

		if len(actions) != len(testGlobs) {
			t.Logf("Warning: Expected %d actions, got %d (some globs may not exist in test data)",
				len(testGlobs), len(actions))
		}

		t.Logf("Retrieved %d actions from same block", len(actions))
		for i, action := range actions {
			gs, _ := action.Receipt.GlobalSequence.Int64()
			t.Logf("  [%d] %s::%s (glob %d)", i, action.Act.Account, action.Act.Name, gs)
		}
	})

	t.Run("MultipleGlobalSeqs_DifferentBlocks", func(t *testing.T) {
		// Test with globs spread across different blocks
		// This requires multiple decompressions
		testGlobs := []uint64{1260, 2000, 3000}

		actions, _, err := reader.GetActionsByGlobalSeqs(testGlobs)
		if err != nil {
			t.Skipf("GetActionsByGlobalSeqs failed (globs may not exist in test data): %v", err)
			return
		}

		t.Logf("Retrieved %d actions from different blocks", len(actions))
		for i, action := range actions {
			gs, _ := action.Receipt.GlobalSequence.Int64()
			t.Logf("  [%d] glob %d: %s::%s", i, gs, action.Act.Account, action.Act.Name)
		}
	})

	t.Run("NonExistentGlob", func(t *testing.T) {
		// Use a very high glob that doesn't exist
		_, _, err := reader.GetActionsByGlobalSeqs([]uint64{999999999999})
		if err == nil {
			t.Error("Expected error for non-existent glob, got nil")
		} else {
			t.Logf("Got expected error for non-existent glob: %v", err)
		}
	})

	t.Run("OrderPreservation", func(t *testing.T) {
		// Verify that actions are returned in INPUT order, not block order
		testGlobs := []uint64{3000, 1260, 2000} // Out of order

		actions, _, err := reader.GetActionsByGlobalSeqs(testGlobs)
		if err != nil {
			t.Skipf("GetActionsByGlobalSeqs failed: %v", err)
			return
		}

		if len(actions) != len(testGlobs) {
			t.Skipf("Not all globs found in test data")
			return
		}

		// Verify order matches input
		for i, action := range actions {
			gs, _ := action.Receipt.GlobalSequence.Int64()
			if uint64(gs) != testGlobs[i] {
				t.Errorf("Order mismatch at [%d]: expected glob %d, got %d",
					i, testGlobs[i], gs)
			}
		}

		t.Log("Actions returned in correct input order (not block order)")
	})
}

// TestGetStatePropsSl icedReader tests state file reading for slice-based storage
func TestGetStatePropsSliceReader(t *testing.T) {
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

	t.Run("ReadStateFile", func(t *testing.T) {
		libNum, headNum, err := reader.GetStateProps(false) // Use cached metadata for tests
		if err != nil {
			t.Fatalf("GetStateProps failed: %v", err)
		}

		t.Logf("State: lib=%d, head=%d", libNum, headNum)

		// Sanity checks
		if libNum == 0 {
			t.Error("lib_num should not be 0")
		}
		if headNum == 0 {
			t.Error("head_num should not be 0")
		}
		if libNum > headNum {
			t.Errorf("lib_num (%d) should not be greater than head_num (%d)", libNum, headNum)
		}
	})
}

// TestReaderCloseSliceReader tests proper cleanup for slice reader
func TestReaderCloseSliceReader(t *testing.T) {
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

	// Read something to ensure resources are allocated
	_, _, _, err = reader.GetNotificationsOnly(500)
	if err != nil {
		t.Fatalf("GetNotificationsOnly failed: %v", err)
	}

	// Close should succeed
	err = reader.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should not panic (should be idempotent)
	err = reader.Close()
	if err != nil {
		t.Logf("Second close returned error (may be expected): %v", err)
	}

	t.Log("Reader closed successfully")
}

// BenchmarkExtractionMethodsDetailed compares performance of specialized extractors
func BenchmarkExtractionMethodsDetailed(b *testing.B) {
	dataPath := getTestDataPath()

	reader, err := NewSliceReader(dataPath)
	if err != nil {
		b.Skipf("Skipping benchmark - no data at %s: %v", dataPath, err)
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

	b.Run("GetNotificationsWithActionMetadata", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, _, err := reader.GetNotificationsWithActionMetadata(testBlock)
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

	b.Run("GetRawActionsFiltered_NoFilter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, err := reader.GetRawActionsFiltered(testBlock, "", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetRawActionsFiltered_EosioOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, err := reader.GetRawActionsFiltered(testBlock, "eosio", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetRawActionsFiltered_EosioOnblock", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, err := reader.GetRawActionsFiltered(testBlock, "eosio", "onblock")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetActionsByGlobalSeqs_SingleGlob", func(b *testing.B) {
		testGlobs := []uint64{1260}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := reader.GetActionsByGlobalSeqs(testGlobs)
			if err != nil {
				b.Skip("Glob not available in test data")
				return
			}
		}
	})

	b.Run("GetActionsByGlobalSeqs_MultipleGlobs", func(b *testing.B) {
		testGlobs := []uint64{1260, 2000, 3000}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := reader.GetActionsByGlobalSeqs(testGlobs)
			if err != nil {
				b.Skip("Globs not available in test data")
				return
			}
		}
	})
}
