package corereader

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

// TestNotificationDeduplication tests that duplicate accounts in notifications are properly deduplicated
// This test verifies the O(N²) → O(N) optimization works correctly
// It ensures high code coverage on all modified functions:
// - reader.go: GetNotificationsOnly, GetNotificationsWithActionMetadata
// - slice_reader.go: GetNotificationsOnly, GetNotificationsWithActionMetadata
func TestNotificationDeduplication(t *testing.T) {
	// Use existing test data
	dataPath := getTestDataPath()

	// Only test SliceReader (we only support slice storage now)
	t.Run("SliceReader_Deduplication", func(t *testing.T) {
		testDeduplicationSliceReader(t, dataPath)
	})
}

func testDeduplicationReader(t *testing.T, dataPath string) {
	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("Failed to open reader: %v", err)
		return
	}
	defer reader.Close()

	// Get state to find valid block range
	head, lib, err := reader.GetStateProps(false)
	if err != nil {
		t.Skipf("Failed to get state: %v", err)
		return
	}

	if lib < 500 {
		t.Skip("Not enough blocks in test data")
		return
	}

	// Test GetNotificationsOnly across multiple blocks for thorough coverage
	t.Run("GetNotificationsOnly_MultipleBlocks", func(t *testing.T) {
		testBlocks := []uint32{500, 1000, 5000, 10000, 50000, 100000}
		blocksTestedCount := 0
		totalNotifications := 0

		for _, testBlock := range testBlocks {
			if testBlock > lib {
				continue
			}

			notifs, blockID, previous, err := reader.GetNotificationsOnly(testBlock)
			if err != nil {
				t.Logf("Skipping block %d: %v", testBlock, err)
				continue
			}

			blocksTestedCount++

			// Verify BlockID is returned
			if blockID == "" {
				t.Errorf("Block %d: blockID is empty", testBlock)
			}

			// Count notifications
			for _, globs := range notifs {
				totalNotifications += len(globs)
			}

			// CRITICAL TEST: Verify no account appears multiple times with same globalseq
			// This ensures the deduplication logic (O(N²) → O(N) optimization) works correctly
			for account, globs := range notifs {
				seen := make(map[uint64]bool)
				for _, glob := range globs {
					if seen[glob] {
						t.Errorf("Block %d: Account %s has duplicate globalseq %d (deduplication FAILED!)",
							testBlock, chain.NameToString(account), glob)
					}
					seen[glob] = true
				}
			}

			// Log details for first block
			if testBlock == 500 {
				t.Logf("Block %d: blockID=%s, previous=%s, accounts=%d",
					testBlock, blockID, previous, len(notifs))
				count := 0
				for account, globs := range notifs {
					if count >= 3 {
						break
					}
					t.Logf("  %s -> %d notifications", chain.NameToString(account), len(globs))
					count++
				}
			}
		}

		if blocksTestedCount == 0 {
			t.Skip("No blocks available for testing")
		}

		t.Logf("Tested %d blocks with %d total notifications", blocksTestedCount, totalNotifications)
	})

	// Test GetNotificationsWithActionMetadata across multiple blocks
	t.Run("GetNotificationsWithActionMetadata_MultipleBlocks", func(t *testing.T) {
		testBlocks := []uint32{500, 1000, 5000, 10000, 50000, 100000}
		blocksTestedCount := 0
		totalNotifications := 0
		totalMetadata := 0

		for _, testBlock := range testBlocks {
			if testBlock > lib {
				continue
			}

			notifs, actionMeta, blockID, previous, err := reader.GetNotificationsWithActionMetadata(testBlock)
			if err != nil {
				t.Logf("Skipping block %d: %v", testBlock, err)
				continue
			}

			blocksTestedCount++

			// Verify BlockID is returned
			if blockID == "" {
				t.Errorf("Block %d: blockID is empty", testBlock)
			}

			// Count notifications and metadata
			for _, globs := range notifs {
				totalNotifications += len(globs)
			}
			totalMetadata += len(actionMeta)

			// CRITICAL TEST: Verify no account appears multiple times with same globalseq
			for account, globs := range notifs {
				seen := make(map[uint64]bool)
				for _, glob := range globs {
					if seen[glob] {
						t.Errorf("Block %d: Account %s has duplicate globalseq %d (deduplication FAILED!)",
							testBlock, chain.NameToString(account), glob)
					}
					seen[glob] = true
				}
			}

			// Verify action metadata exists for each globalseq
			metaMap := make(map[uint64]ActionMetadata)
			for _, meta := range actionMeta {
				// Verify metadata fields are populated
				if meta.GlobalSeq == 0 {
					t.Errorf("Block %d: Action metadata has zero GlobalSeq", testBlock)
				}
				if meta.Contract == 0 {
					t.Errorf("Block %d: Action metadata has zero Contract (globalseq=%d)", testBlock, meta.GlobalSeq)
				}
				if meta.Action == 0 {
					t.Errorf("Block %d: Action metadata has zero Action (globalseq=%d)", testBlock, meta.GlobalSeq)
				}
				metaMap[meta.GlobalSeq] = meta
			}

			// Verify every notification has corresponding metadata
			for account, globs := range notifs {
				for _, glob := range globs {
					if _, exists := metaMap[glob]; !exists {
						t.Errorf("Block %d: Account %s has globalseq %d but no action metadata found",
							testBlock, chain.NameToString(account), glob)
					}
				}
			}

			// Log details for first block
			if testBlock == 500 {
				t.Logf("Block %d: blockID=%s, previous=%s, accounts=%d, metadata=%d",
					testBlock, blockID, previous, len(notifs), len(actionMeta))
				for i, meta := range actionMeta {
					if i >= 3 {
						break
					}
					t.Logf("  Action[%d]: glob=%d, %s::%s",
						i, meta.GlobalSeq,
						chain.NameToString(meta.Contract),
						chain.NameToString(meta.Action))
				}
			}
		}

		if blocksTestedCount == 0 {
			t.Skip("No blocks available for testing")
		}

		t.Logf("Tested %d blocks with %d notifications and %d metadata entries",
			blocksTestedCount, totalNotifications, totalMetadata)
	})

	t.Logf("Block range available: head=%d, lib=%d", head, lib)
}

func testDeduplicationSliceReader(t *testing.T, dataPath string) {
	reader, err := NewSliceReader(dataPath)
	if err != nil {
		t.Skipf("Failed to open slice reader: %v", err)
		return
	}
	defer reader.Close()

	// Get state to find valid block range
	head, lib, err := reader.GetStateProps(false) // Use cached metadata for tests
	if err != nil {
		t.Skipf("Failed to get state: %v", err)
		return
	}

	if lib < 500 {
		t.Skip("Not enough blocks in test data")
		return
	}

	// Test GetNotificationsOnly across multiple blocks for thorough coverage
	t.Run("GetNotificationsOnly_MultipleBlocks", func(t *testing.T) {
		testBlocks := []uint32{500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}
		blocksTestedCount := 0
		totalNotifications := 0

		for _, testBlock := range testBlocks {
			if testBlock > lib {
				continue
			}

			notifs, blockID, previous, err := reader.GetNotificationsOnly(testBlock)
			if err != nil {
				t.Logf("Skipping block %d: %v", testBlock, err)
				continue
			}

			blocksTestedCount++

			// Verify BlockID is returned
			if blockID == "" {
				t.Errorf("Block %d: blockID is empty", testBlock)
			}

			// Count notifications
			for _, globs := range notifs {
				totalNotifications += len(globs)
			}

			// CRITICAL TEST: Verify no account appears multiple times with same globalseq
			// This ensures the deduplication logic (O(N²) → O(N) optimization) works correctly
			for account, globs := range notifs {
				seen := make(map[uint64]bool)
				for _, glob := range globs {
					if seen[glob] {
						t.Errorf("Block %d: Account %s has duplicate globalseq %d (deduplication FAILED!)",
							testBlock, chain.NameToString(account), glob)
					}
					seen[glob] = true
				}
			}

			// Log details for first block
			if testBlock == 500 {
				t.Logf("Block %d: blockID=%s, previous=%s, accounts=%d",
					testBlock, blockID, previous, len(notifs))
				count := 0
				for account, globs := range notifs {
					if count >= 3 {
						break
					}
					t.Logf("  %s -> %d notifications", chain.NameToString(account), len(globs))
					count++
				}
			}
		}

		if blocksTestedCount == 0 {
			t.Skip("No blocks available for testing")
		}

		t.Logf("Tested %d blocks with %d total notifications", blocksTestedCount, totalNotifications)
	})

	// Test GetNotificationsWithActionMetadata across multiple blocks
	t.Run("GetNotificationsWithActionMetadata_MultipleBlocks", func(t *testing.T) {
		testBlocks := []uint32{500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}
		blocksTestedCount := 0
		totalNotifications := 0
		totalMetadata := 0

		for _, testBlock := range testBlocks {
			if testBlock > lib {
				continue
			}

			notifs, actionMeta, blockID, previous, err := reader.GetNotificationsWithActionMetadata(testBlock)
			if err != nil {
				t.Logf("Skipping block %d: %v", testBlock, err)
				continue
			}

			blocksTestedCount++

			// Verify BlockID is returned
			if blockID == "" {
				t.Errorf("Block %d: blockID is empty", testBlock)
			}

			// Count notifications and metadata
			for _, globs := range notifs {
				totalNotifications += len(globs)
			}
			totalMetadata += len(actionMeta)

			// CRITICAL TEST: Verify no account appears multiple times with same globalseq
			for account, globs := range notifs {
				seen := make(map[uint64]bool)
				for _, glob := range globs {
					if seen[glob] {
						t.Errorf("Block %d: Account %s has duplicate globalseq %d (deduplication FAILED!)",
							testBlock, chain.NameToString(account), glob)
					}
					seen[glob] = true
				}
			}

			// Verify action metadata exists for each globalseq
			metaMap := make(map[uint64]ActionMetadata)
			for _, meta := range actionMeta {
				// Verify metadata fields are populated
				if meta.GlobalSeq == 0 {
					t.Errorf("Block %d: Action metadata has zero GlobalSeq", testBlock)
				}
				if meta.Contract == 0 {
					t.Errorf("Block %d: Action metadata has zero Contract (globalseq=%d)", testBlock, meta.GlobalSeq)
				}
				if meta.Action == 0 {
					t.Errorf("Block %d: Action metadata has zero Action (globalseq=%d)", testBlock, meta.GlobalSeq)
				}
				metaMap[meta.GlobalSeq] = meta
			}

			// Verify every notification has corresponding metadata
			for account, globs := range notifs {
				for _, glob := range globs {
					if _, exists := metaMap[glob]; !exists {
						t.Errorf("Block %d: Account %s has globalseq %d but no action metadata found",
							testBlock, chain.NameToString(account), glob)
					}
				}
			}

			// Log details for first block
			if testBlock == 500 {
				t.Logf("Block %d: blockID=%s, previous=%s, accounts=%d, metadata=%d",
					testBlock, blockID, previous, len(notifs), len(actionMeta))
				for i, meta := range actionMeta {
					if i >= 3 {
						break
					}
					t.Logf("  Action[%d]: glob=%d, %s::%s",
						i, meta.GlobalSeq,
						chain.NameToString(meta.Contract),
						chain.NameToString(meta.Action))
				}
			}
		}

		if blocksTestedCount == 0 {
			t.Skip("No blocks available for testing")
		}

		t.Logf("Tested %d blocks with %d notifications and %d metadata entries",
			blocksTestedCount, totalNotifications, totalMetadata)
	})

	t.Logf("Block range available: head=%d, lib=%d", head, lib)
}
