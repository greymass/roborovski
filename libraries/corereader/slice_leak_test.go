package corereader

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSyncReaderUnfinalizedTipNoMmapLeak: sync reader fetching the unfinalized tip must not leak a mmap per read.
func TestSyncReaderUnfinalizedTipNoMmapLeak(t *testing.T) {
	src := getTestDataPath()
	if _, err := os.Stat(src); os.IsNotExist(err) {
		t.Skip("Test data not available")
	}

	dir := filepath.Join(t.TempDir(), "slices")
	if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
		t.Fatalf("copy testdata: %v", err)
	}

	sr, err := NewSliceReaderWithOptions(dir, SyncReaderOptions())
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	defer sr.Close()

	if sr.globRangeIndex != nil {
		t.Fatal("expected glob range index disabled for sync reader")
	}

	// Mark the last slice unfinalized so it takes the live-tip (one-shot) path.
	slices := sr.sharedMetadata.getSlices()
	if len(slices) == 0 {
		t.Fatal("no slices in test data")
	}
	tip := &slices[len(slices)-1]
	tip.Finalized = false
	sr.sharedMetadata = newSharedSliceMetadata(slices)

	tipBlock := tip.StartBlock + 100
	if tipBlock > tip.EndBlock {
		tipBlock = tip.StartBlock
	}

	// Sanity read + warm any one-time allocations before measuring.
	if _, _, _, err := sr.GetNotificationsOnly(tipBlock); err != nil {
		t.Fatalf("initial read of tip block %d: %v", tipBlock, err)
	}

	before := LiveSliceMmaps()
	const iterations = 300
	for i := 0; i < iterations; i++ {
		if _, _, _, err := sr.GetNotificationsOnly(tipBlock); err != nil {
			t.Fatalf("read %d of tip block %d: %v", i, tipBlock, err)
		}
	}
	after := LiveSliceMmaps()

	if after > before {
		t.Fatalf("slice mmap leak on unfinalized tip: open mmaps grew from %d to %d over %d reads (%d leaked)",
			before, after, iterations, after-before)
	}
}
