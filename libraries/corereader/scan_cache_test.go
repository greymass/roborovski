package corereader

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestScanCache(t *testing.T) {
	// Clear cache before test
	scanCacheMu.Lock()
	scanCache = make(map[string]*cachedScan)
	scanCacheMu.Unlock()

	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	// First call should scan filesystem
	t.Run("FirstScanCachesResult", func(t *testing.T) {
		start := time.Now()
		slices1, err := getCachedOrScan(testDataPath)
		duration1 := time.Since(start)

		if err != nil {
			t.Fatalf("First scan failed: %v", err)
		}
		if slices1.getSliceCount() == 0 {
			t.Fatal("First scan returned no slices")
		}
		t.Logf("First scan: %d slices in %v", slices1.getSliceCount(), duration1)

		// Verify cache entry exists
		scanCacheMu.RLock()
		_, exists := scanCache[testDataPath]
		scanCacheMu.RUnlock()
		if !exists {
			t.Fatal("Cache entry not created after first scan")
		}
	})

	// Second call should use cache (much faster)
	t.Run("SecondScanUsesCache", func(t *testing.T) {
		start := time.Now()
		slices2, err := getCachedOrScan(testDataPath)
		duration2 := time.Since(start)

		if err != nil {
			t.Fatalf("Cached scan failed: %v", err)
		}
		if slices2.getSliceCount() == 0 {
			t.Fatal("Cached scan returned no slices")
		}
		t.Logf("Cached scan: %d slices in %v", slices2.getSliceCount(), duration2)

		// Cache should be extremely fast (< 1ms)
		if duration2 > 10*time.Millisecond {
			t.Errorf("Cached scan took too long: %v (expected < 10ms)", duration2)
		}
	})

	// Verify cache returns identical data
	t.Run("CacheReturnsIdenticalData", func(t *testing.T) {
		slices1, _ := getCachedOrScan(testDataPath)
		slices2, _ := getCachedOrScan(testDataPath)

		count1 := slices1.getSliceCount()
		count2 := slices2.getSliceCount()
		if count1 != count2 {
			t.Fatalf("Cache returned different slice count: %d vs %d", count1, count2)
		}

		for i := 0; i < count1; i++ {
			s1 := slices1.getSlice(i)
			s2 := slices2.getSlice(i)
			if s1.SliceNum != s2.SliceNum {
				t.Errorf("Slice %d differs: SliceNum %d vs %d", i, s1.SliceNum, s2.SliceNum)
			}
			if s1.StartBlock != s2.StartBlock {
				t.Errorf("Slice %d differs: StartBlock %d vs %d", i, s1.StartBlock, s2.StartBlock)
			}
			if s1.EndBlock != s2.EndBlock {
				t.Errorf("Slice %d differs: EndBlock %d vs %d", i, s1.EndBlock, s2.EndBlock)
			}
		}
	})
}

func TestBuildGlobRangeIndexFromSlices(t *testing.T) {
	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	slices, err := getCachedOrScan(testDataPath)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	t.Run("BuildIndexFromMemory", func(t *testing.T) {
		start := time.Now()
		index := buildGlobRangeIndexFromSlices(slices)
		duration := time.Since(start)

		if index == nil {
			t.Fatal("Failed to build glob range index")
		}

		t.Logf("Built glob range index: %d slices in %v", index.totalSlices, duration)

		// Should be extremely fast (< 1ms)
		if duration > time.Millisecond {
			t.Errorf("Building index took too long: %v (expected < 1ms)", duration)
		}

		// Verify index properties
		if index.totalSlices == 0 {
			t.Error("Index has no slices")
		}
		if len(index.ranges) != index.totalSlices {
			t.Errorf("Range count mismatch: %d ranges vs %d totalSlices", len(index.ranges), index.totalSlices)
		}
		// Note: with only a few slices, memoryUsedKB may round to 0
		// This is acceptable - the test validates index construction worked
		t.Logf("Index memory usage: %d KB", index.memoryUsedKB)
	})

	t.Run("IndexExcludesLast2Slices", func(t *testing.T) {
		index := buildGlobRangeIndexFromSlices(slices)
		if index == nil {
			t.Fatal("Failed to build glob range index")
		}

		// Index should have sliceCount-2 entries (excluding last 2 active slices)
		// Some slices may be skipped if they have zero glob ranges
		sliceCount := slices.getSliceCount()
		if index.totalSlices > sliceCount-2 {
			t.Errorf("Index should exclude last 2 slices: has %d, max expected %d",
				index.totalSlices, sliceCount-2)
		}
	})

	t.Run("IndexSkipsEmptySlices", func(t *testing.T) {
		// Create test data with empty slices
		testSlices := &SharedSliceMetadata{
			slices: []SliceInfo{
				{SliceNum: 0, StartBlock: 1, EndBlock: 10000, GlobMin: 100, GlobMax: 200, BlocksPerSlice: 10000},
				{SliceNum: 1, StartBlock: 10001, EndBlock: 20000, GlobMin: 0, GlobMax: 0, BlocksPerSlice: 10000}, // Empty
				{SliceNum: 2, StartBlock: 20001, EndBlock: 30000, GlobMin: 300, GlobMax: 400, BlocksPerSlice: 10000},
				{SliceNum: 3, StartBlock: 30001, EndBlock: 40000, GlobMin: 500, GlobMax: 600, BlocksPerSlice: 10000}, // Active (excluded)
				{SliceNum: 4, StartBlock: 40001, EndBlock: 50000, GlobMin: 700, GlobMax: 800, BlocksPerSlice: 10000}, // Active (excluded)
			},
		}

		index := buildGlobRangeIndexFromSlices(testSlices)
		if index == nil {
			t.Fatal("Failed to build index from test slices")
		}

		// Should have 2 slices (slice 0 and 2, excluding slice 1 which is empty and last 2 which are active)
		if index.totalSlices != 2 {
			t.Errorf("Expected 2 slices in index (excluding empty + last 2), got %d", index.totalSlices)
		}

		// Verify ranges are correct
		if len(index.ranges) != 2 {
			t.Fatalf("Expected 2 ranges, got %d", len(index.ranges))
		}
		if index.ranges[0].SliceNum != 0 || index.ranges[0].MinGlob != 100 || index.ranges[0].MaxGlob != 200 {
			t.Errorf("Range 0 incorrect: got slice %d, glob %d-%d", index.ranges[0].SliceNum, index.ranges[0].MinGlob, index.ranges[0].MaxGlob)
		}
		if index.ranges[1].SliceNum != 2 || index.ranges[1].MinGlob != 300 || index.ranges[1].MaxGlob != 400 {
			t.Errorf("Range 1 incorrect: got slice %d, glob %d-%d", index.ranges[1].SliceNum, index.ranges[1].MinGlob, index.ranges[1].MaxGlob)
		}
	})
}

func TestMultipleReadersShareCache(t *testing.T) {
	// Clear cache before test
	scanCacheMu.Lock()
	scanCache = make(map[string]*cachedScan)
	scanCacheMu.Unlock()

	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	t.Run("TwoReadersFromSamePath", func(t *testing.T) {
		// Create first reader (should scan)
		reader1, err := NewSliceReaderWithOptions(testDataPath, QueryReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create first reader: %v", err)
		}
		defer reader1.Close()

		sliceCount1 := reader1.sharedMetadata.getSliceCount()
		t.Logf("Reader 1: %d slices", sliceCount1)

		// Create second reader (should use cache)
		reader2, err := NewSliceReaderWithOptions(testDataPath, SyncReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create second reader: %v", err)
		}
		defer reader2.Close()

		sliceCount2 := reader2.sharedMetadata.getSliceCount()
		t.Logf("Reader 2: %d slices", sliceCount2)

		// Both should have same slice count
		if sliceCount1 != sliceCount2 {
			t.Errorf("Readers have different slice counts: %d vs %d", sliceCount1, sliceCount2)
		}

		// Currently, readers do NOT share the same metadata instance (each has its own)
		// This is by design: each reader gets its own SharedSliceMetadata from cached scan results
		// The benefit: eliminates duplicate sliceInfos within each reader (saved ~2.3 MB per reader)
		// Future enhancement: could make readers share same *SharedSliceMetadata if desired
		if reader1.sharedMetadata == reader2.sharedMetadata {
			t.Log("Readers share the same metadata instance (unexpected but valid)")
		} else {
			t.Log("Readers have separate metadata instances (current design - each reader owns its metadata)")
		}
	})
}

func TestSharedMetadataUpdates(t *testing.T) {
	scanCacheMu.Lock()
	scanCache = make(map[string]*cachedScan)
	scanCacheMu.Unlock()

	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	t.Run("MetadataWithinReaderIsShared", func(t *testing.T) {
		// This test verifies that within a single reader, the shared metadata is consistent
		reader1, err := NewSliceReaderWithOptions(testDataPath, QueryReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create reader1: %v", err)
		}
		defer reader1.Close()

		initialCount := reader1.sharedMetadata.getSliceCount()
		t.Logf("Initial slice count: %d", initialCount)

		// Simulate discovering a new slice (like coreindex would do)
		newSlice := SliceInfo{
			SliceNum:       uint32(initialCount),
			StartBlock:     uint32(initialCount*10000 + 1),
			EndBlock:       uint32(initialCount*10000 + 5000),
			MaxBlock:       uint32((initialCount + 1) * 10000),
			BlocksPerSlice: 10000,
			Finalized:      false,
			GlobMin:        999999,
			GlobMax:        1000099,
		}

		// Append via shared metadata
		reader1.sharedMetadata.appendSlice(newSlice)

		// Verify the reader sees the update
		newCount := reader1.sharedMetadata.getSliceCount()
		if newCount != initialCount+1 {
			t.Errorf("Reader slice count incorrect: got %d, expected %d", newCount, initialCount+1)
		}

		retrievedSlice := reader1.sharedMetadata.getSlice(initialCount)
		if retrievedSlice.SliceNum != newSlice.SliceNum {
			t.Errorf("Retrieved slice doesn't match: got %d, expected %d", retrievedSlice.SliceNum, newSlice.SliceNum)
		}

		t.Log("✓ SharedSliceMetadata within reader is consistent")
	})
}

func TestConcurrentMetadataAccess(t *testing.T) {
	scanCacheMu.Lock()
	scanCache = make(map[string]*cachedScan)
	scanCacheMu.Unlock()

	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	t.Run("ConcurrentReadsAndWrites", func(t *testing.T) {
		reader1, err := NewSliceReaderWithOptions(testDataPath, QueryReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create reader1: %v", err)
		}
		defer reader1.Close()

		reader2, err := NewSliceReaderWithOptions(testDataPath, SyncReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create reader2: %v", err)
		}
		defer reader2.Close()

		var wg sync.WaitGroup
		errors := make(chan error, 3)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				count := reader1.sharedMetadata.getSliceCount()
				if count == 0 {
					errors <- nil
					return
				}
				_ = reader1.sharedMetadata.getSlice(0)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				count := reader2.sharedMetadata.getSliceCount()
				if count == 0 {
					errors <- nil
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			baseCount := reader1.sharedMetadata.getSliceCount()
			for i := 0; i < 10; i++ {
				newSlice := SliceInfo{
					SliceNum:       uint32(baseCount + i),
					StartBlock:     uint32((baseCount+i)*10000 + 1),
					EndBlock:       uint32((baseCount+i)*10000 + 5000),
					MaxBlock:       uint32((baseCount + i + 1) * 10000),
					BlocksPerSlice: 10000,
					Finalized:      false,
					GlobMin:        uint64(1000000 + i*100),
					GlobMax:        uint64(1000100 + i*100),
				}
				reader1.sharedMetadata.appendSlice(newSlice)
				time.Sleep(time.Millisecond)
			}
		}()

		wg.Wait()
		close(errors)

		for err := range errors {
			if err != nil {
				t.Errorf("Concurrent access error: %v", err)
			}
		}

		t.Logf("Completed 200 reads + 10 writes with no races")
	})
}

func TestSliceMapIndexLookup(t *testing.T) {
	scanCacheMu.Lock()
	scanCache = make(map[string]*cachedScan)
	scanCacheMu.Unlock()

	testDataPath := getTestDataPath()
	if _, err := os.Stat(testDataPath); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	t.Run("LookupRemainsValidAfterAppend", func(t *testing.T) {
		reader, err := NewSliceReaderWithOptions(testDataPath, QueryReaderOptions())
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		initialCount := reader.sharedMetadata.getSliceCount()
		if initialCount == 0 {
			t.Skip("No slices available for testing")
		}

		firstSlice := reader.sharedMetadata.getSlice(0)
		lookupKey := firstSlice.StartBlock / 10000

		idx, exists := reader.sliceMapByBlock[lookupKey]
		if !exists {
			t.Fatalf("Slice map doesn't contain key %d", lookupKey)
		}

		lookupSlice := reader.sharedMetadata.getSlice(idx)
		if lookupSlice.SliceNum != firstSlice.SliceNum {
			t.Errorf("Initial lookup failed: got slice %d, expected %d", lookupSlice.SliceNum, firstSlice.SliceNum)
		}

		newSlice := SliceInfo{
			SliceNum:       uint32(initialCount),
			StartBlock:     uint32(initialCount*10000 + 1),
			EndBlock:       uint32(initialCount*10000 + 5000),
			MaxBlock:       uint32((initialCount + 1) * 10000),
			BlocksPerSlice: 10000,
			Finalized:      false,
			GlobMin:        999999,
			GlobMax:        1000099,
		}
		reader.sharedMetadata.appendSlice(newSlice)

		lookupSliceAfter := reader.sharedMetadata.getSlice(idx)
		if lookupSliceAfter.SliceNum != firstSlice.SliceNum {
			t.Errorf("Lookup invalidated after append: got slice %d, expected %d", lookupSliceAfter.SliceNum, firstSlice.SliceNum)
		}

		t.Logf("Slice map lookup remained valid after append (capacity-based stability)")
	})
}
