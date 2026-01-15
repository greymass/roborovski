package corereader

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestNewBlockIndexCache(t *testing.T) {
	cache := NewBlockIndexCache("/tmp/test.cache")
	if cache == nil {
		t.Fatal("NewBlockIndexCache returned nil")
	}
	if cache.slices == nil {
		t.Error("slices map not initialized")
	}
}

func TestSliceBlockIndex_FindBlock(t *testing.T) {
	slice := &SliceBlockIndex{
		SliceNum:   1,
		StartBlock: 100,
		Entries: []BlockIndexCacheEntry{
			{BlockNum: 100, GlobMin: 1000, GlobMax: 1099},
			{BlockNum: 101, GlobMin: 1100, GlobMax: 1199},
			{BlockNum: 102, GlobMin: 1200, GlobMax: 1299},
		},
	}

	tests := []struct {
		name      string
		glob      uint64
		wantBlock uint32
		wantFound bool
	}{
		{"first entry start", 1000, 100, true},
		{"first entry mid", 1050, 100, true},
		{"first entry end", 1099, 100, true},
		{"second entry", 1150, 101, true},
		{"third entry", 1250, 102, true},
		{"before range", 999, 0, false},
		{"after range", 1300, 0, false},
		{"gap between entries", 1100, 101, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block, found := slice.FindBlock(tt.glob)
			if found != tt.wantFound {
				t.Errorf("FindBlock(%d) found = %v, want %v", tt.glob, found, tt.wantFound)
			}
			if found && block != tt.wantBlock {
				t.Errorf("FindBlock(%d) block = %d, want %d", tt.glob, block, tt.wantBlock)
			}
		})
	}
}

func TestSliceBlockIndex_FindBlock_Empty(t *testing.T) {
	slice := &SliceBlockIndex{
		SliceNum: 1,
		Entries:  []BlockIndexCacheEntry{},
	}

	_, found := slice.FindBlock(1000)
	if found {
		t.Error("expected not found for empty slice")
	}
}

func TestSliceBlockIndex_GetGlobRange(t *testing.T) {
	t.Run("with entries", func(t *testing.T) {
		slice := &SliceBlockIndex{
			Entries: []BlockIndexCacheEntry{
				{GlobMin: 1000, GlobMax: 1099},
				{GlobMin: 1100, GlobMax: 1199},
				{GlobMin: 1200, GlobMax: 1500},
			},
		}

		min, max := slice.GetGlobRange()
		if min != 1000 {
			t.Errorf("min = %d, want 1000", min)
		}
		if max != 1500 {
			t.Errorf("max = %d, want 1500", max)
		}
	})

	t.Run("empty", func(t *testing.T) {
		slice := &SliceBlockIndex{Entries: []BlockIndexCacheEntry{}}
		min, max := slice.GetGlobRange()
		if min != 0 || max != 0 {
			t.Errorf("expected (0, 0) for empty slice, got (%d, %d)", min, max)
		}
	})
}

func TestBlockIndexCache_AddSlice(t *testing.T) {
	cache := NewBlockIndexCache("")

	entries := []BlockIndexCacheEntry{
		{BlockNum: 100, GlobMin: 1000, GlobMax: 1099},
		{BlockNum: 101, GlobMin: 1100, GlobMax: 1199},
	}

	cache.AddSlice(1, 100, entries, false)

	slice, exists := cache.GetSlice(1)
	if !exists {
		t.Fatal("slice not found after AddSlice")
	}
	if slice.SliceNum != 1 {
		t.Errorf("SliceNum = %d, want 1", slice.SliceNum)
	}
	if slice.StartBlock != 100 {
		t.Errorf("StartBlock = %d, want 100", slice.StartBlock)
	}
	if len(slice.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(slice.Entries))
	}
	if slice.Finalized {
		t.Error("expected Finalized = false")
	}
}

func TestBlockIndexCache_AddSlice_Finalized(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(5, 500, nil, true)

	if cache.GetMaxFinalizedSlice() != 5 {
		t.Errorf("maxFinalizedSlice = %d, want 5", cache.GetMaxFinalizedSlice())
	}
	if !cache.IsDirty() {
		t.Error("expected dirty after adding finalized slice")
	}
}

func TestBlockIndexCache_AddBlockEntry(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddBlockEntry(1, 100, 1000, 1099, 0, 1024)
	cache.AddBlockEntry(1, 101, 1100, 1199, 1024, 2048)

	slice, exists := cache.GetSlice(1)
	if !exists {
		t.Fatal("slice not found")
	}
	if len(slice.Entries) != 2 {
		t.Fatalf("len(Entries) = %d, want 2", len(slice.Entries))
	}
	if slice.StartBlock != 100 {
		t.Errorf("StartBlock = %d, want 100", slice.StartBlock)
	}

	entry := slice.Entries[0]
	if entry.BlockNum != 100 {
		t.Errorf("entry.BlockNum = %d, want 100", entry.BlockNum)
	}
	if entry.GlobMin != 1000 {
		t.Errorf("entry.GlobMin = %d, want 1000", entry.GlobMin)
	}
	if entry.GlobMax != 1099 {
		t.Errorf("entry.GlobMax = %d, want 1099", entry.GlobMax)
	}
	if entry.FileOffset != 0 {
		t.Errorf("entry.FileOffset = %d, want 0", entry.FileOffset)
	}
	if entry.Size != 1024 {
		t.Errorf("entry.Size = %d, want 1024", entry.Size)
	}
}

func TestBlockIndexCache_FinalizeSlice(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddBlockEntry(1, 100, 1000, 1099, 0, 1024)
	cache.FinalizeSlice(1)

	slice, _ := cache.GetSlice(1)
	if !slice.Finalized {
		t.Error("expected slice to be finalized")
	}
	if cache.GetMaxFinalizedSlice() != 1 {
		t.Errorf("maxFinalizedSlice = %d, want 1", cache.GetMaxFinalizedSlice())
	}
	if !cache.IsDirty() {
		t.Error("expected dirty after finalize")
	}
}

func TestBlockIndexCache_FinalizeSlice_AlreadyFinalized(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, nil, true)
	cache.dirty.Store(false)

	cache.FinalizeSlice(1)
}

func TestBlockIndexCache_FinalizeSlice_NonExistent(t *testing.T) {
	cache := NewBlockIndexCache("")
	cache.FinalizeSlice(99)
}

func TestBlockIndexCache_FindBlock(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, []BlockIndexCacheEntry{
		{BlockNum: 100, GlobMin: 1000, GlobMax: 1099},
		{BlockNum: 101, GlobMin: 1100, GlobMax: 1199},
	}, true)

	block, found := cache.FindBlock(1, 1050)
	if !found {
		t.Error("expected to find block")
	}
	if block != 100 {
		t.Errorf("block = %d, want 100", block)
	}

	_, found = cache.FindBlock(99, 1050)
	if found {
		t.Error("expected not found for non-existent slice")
	}
}

func TestBlockIndexCache_FindSliceForGlob(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, []BlockIndexCacheEntry{
		{GlobMin: 1000, GlobMax: 1999},
	}, true)
	cache.AddSlice(2, 200, []BlockIndexCacheEntry{
		{GlobMin: 2000, GlobMax: 2999},
	}, true)
	cache.AddSlice(3, 300, []BlockIndexCacheEntry{}, true)

	tests := []struct {
		glob      uint64
		wantSlice uint32
		wantFound bool
	}{
		{1500, 1, true},
		{2500, 2, true},
		{500, 0, false},
		{3500, 0, false},
	}

	for _, tt := range tests {
		slice, found := cache.FindSliceForGlob(tt.glob)
		if found != tt.wantFound {
			t.Errorf("FindSliceForGlob(%d) found = %v, want %v", tt.glob, found, tt.wantFound)
		}
		if found && slice != tt.wantSlice {
			t.Errorf("FindSliceForGlob(%d) slice = %d, want %d", tt.glob, slice, tt.wantSlice)
		}
	}
}

func TestBlockIndexCache_GetBlockEntry(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, []BlockIndexCacheEntry{
		{BlockNum: 100, GlobMin: 1000, GlobMax: 1099, FileOffset: 0, Size: 1024},
		{BlockNum: 101, GlobMin: 1100, GlobMax: 1199, FileOffset: 1024, Size: 2048},
	}, true)

	entry, found := cache.GetBlockEntry(1, 100)
	if !found {
		t.Fatal("expected to find entry")
	}
	if entry.Size != 1024 {
		t.Errorf("entry.Size = %d, want 1024", entry.Size)
	}

	entry, found = cache.GetBlockEntry(1, 101)
	if !found {
		t.Fatal("expected to find entry for block 101")
	}
	if entry.FileOffset != 1024 {
		t.Errorf("entry.FileOffset = %d, want 1024", entry.FileOffset)
	}

	_, found = cache.GetBlockEntry(1, 999)
	if found {
		t.Error("expected not found for non-existent block")
	}

	_, found = cache.GetBlockEntry(99, 100)
	if found {
		t.Error("expected not found for non-existent slice")
	}
}

func TestBlockIndexCache_HasSlice(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, nil, false)

	if !cache.HasSlice(1) {
		t.Error("expected HasSlice(1) = true")
	}
	if cache.HasSlice(2) {
		t.Error("expected HasSlice(2) = false")
	}
}

func TestBlockIndexCache_GetStats(t *testing.T) {
	cache := NewBlockIndexCache("")

	cache.AddSlice(1, 100, []BlockIndexCacheEntry{
		{BlockNum: 100},
		{BlockNum: 101},
	}, true)
	cache.AddSlice(2, 200, []BlockIndexCacheEntry{
		{BlockNum: 200},
	}, false)

	stats := cache.GetStats()
	if stats.TotalSlices != 2 {
		t.Errorf("TotalSlices = %d, want 2", stats.TotalSlices)
	}
	if stats.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", stats.TotalEntries)
	}
	if stats.FinalizedSlices != 1 {
		t.Errorf("FinalizedSlices = %d, want 1", stats.FinalizedSlices)
	}
	if stats.ActiveSlices != 1 {
		t.Errorf("ActiveSlices = %d, want 1", stats.ActiveSlices)
	}
	if stats.MemoryBytes != 3*BlockIndexEntrySize {
		t.Errorf("MemoryBytes = %d, want %d", stats.MemoryBytes, 3*BlockIndexEntrySize)
	}
}

func TestBlockIndexCache_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	cache1 := NewBlockIndexCache(cachePath)
	cache1.AddSlice(1, 100, []BlockIndexCacheEntry{
		{BlockNum: 100, FileOffset: 0, Size: 1024, GlobMin: 1000, GlobMax: 1099},
		{BlockNum: 101, FileOffset: 1024, Size: 2048, GlobMin: 1100, GlobMax: 1199},
	}, true)
	cache1.AddSlice(2, 200, []BlockIndexCacheEntry{
		{BlockNum: 200, FileOffset: 0, Size: 512, GlobMin: 2000, GlobMax: 2099},
	}, true)
	cache1.AddSlice(3, 300, []BlockIndexCacheEntry{
		{BlockNum: 300, FileOffset: 0, Size: 256, GlobMin: 3000, GlobMax: 3099},
	}, false)

	err := cache1.SaveToFile()
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	if cache1.IsDirty() {
		t.Error("expected not dirty after save")
	}

	cache2 := NewBlockIndexCache(cachePath)
	err = cache2.LoadFromFile()
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}
	defer cache2.Close()

	if cache2.GetMaxFinalizedSlice() != 2 {
		t.Errorf("maxFinalizedSlice = %d, want 2", cache2.GetMaxFinalizedSlice())
	}

	slice1, exists := cache2.GetSlice(1)
	if !exists {
		t.Fatal("slice 1 not loaded")
	}
	if len(slice1.Entries) != 2 {
		t.Errorf("slice 1 entries = %d, want 2", len(slice1.Entries))
	}
	if slice1.StartBlock != 100 {
		t.Errorf("slice 1 StartBlock = %d, want 100", slice1.StartBlock)
	}

	entry := slice1.Entries[0]
	if entry.BlockNum != 100 || entry.FileOffset != 0 || entry.Size != 1024 {
		t.Errorf("entry mismatch: %+v", entry)
	}
	if entry.GlobMin != 1000 || entry.GlobMax != 1099 {
		t.Errorf("glob range mismatch: %d-%d", entry.GlobMin, entry.GlobMax)
	}

	slice2, exists := cache2.GetSlice(2)
	if !exists {
		t.Fatal("slice 2 not loaded")
	}
	if len(slice2.Entries) != 1 {
		t.Errorf("slice 2 entries = %d, want 1", len(slice2.Entries))
	}

	if cache2.HasSlice(3) {
		t.Error("non-finalized slice 3 should not be loaded")
	}
}

func TestBlockIndexCache_SaveToFile_NotDirty(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	cache := NewBlockIndexCache(cachePath)
	cache.AddSlice(1, 100, nil, false)

	err := cache.SaveToFile()
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Error("expected no file when not dirty")
	}
}

func TestBlockIndexCache_LoadFromFile_NotExists(t *testing.T) {
	cache := NewBlockIndexCache("/nonexistent/path/cache.dat")

	err := cache.LoadFromFile()
	if err != nil {
		t.Errorf("LoadFromFile should not fail for non-existent file: %v", err)
	}
}

func TestBlockIndexCache_LoadFromFile_InvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	data := make([]byte, 64)
	copy(data[0:4], "XXXX")
	os.WriteFile(cachePath, data, 0644)

	cache := NewBlockIndexCache(cachePath)
	err := cache.LoadFromFile()
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestBlockIndexCache_LoadFromFile_InvalidVersion(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	data := make([]byte, 64)
	copy(data[0:4], BlockIndexCacheMagic)
	binary.LittleEndian.PutUint32(data[4:8], 999)
	os.WriteFile(cachePath, data, 0644)

	cache := NewBlockIndexCache(cachePath)
	err := cache.LoadFromFile()
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestBlockIndexCache_LoadFromFile_TooSmall(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	os.WriteFile(cachePath, []byte("small"), 0644)

	cache := NewBlockIndexCache(cachePath)
	err := cache.LoadFromFile()
	if err == nil {
		t.Error("expected error for too small file")
	}
}

func TestBlockIndexCache_LoadFromFile_TruncatedSliceHeader(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	data := make([]byte, BlockIndexCacheHeader+5)
	copy(data[0:4], BlockIndexCacheMagic)
	binary.LittleEndian.PutUint32(data[4:8], BlockIndexCacheVersion)
	binary.LittleEndian.PutUint32(data[8:12], 1)
	os.WriteFile(cachePath, data, 0644)

	cache := NewBlockIndexCache(cachePath)
	err := cache.LoadFromFile()
	if err == nil {
		t.Error("expected error for truncated slice header")
	}
}

func TestBlockIndexCache_LoadFromFile_TruncatedEntries(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	data := make([]byte, BlockIndexCacheHeader+12+10)
	copy(data[0:4], BlockIndexCacheMagic)
	binary.LittleEndian.PutUint32(data[4:8], BlockIndexCacheVersion)
	binary.LittleEndian.PutUint32(data[8:12], 1)
	offset := BlockIndexCacheHeader
	binary.LittleEndian.PutUint32(data[offset:offset+4], 1)
	binary.LittleEndian.PutUint32(data[offset+4:offset+8], 100)
	binary.LittleEndian.PutUint32(data[offset+8:offset+12], 10)
	os.WriteFile(cachePath, data, 0644)

	cache := NewBlockIndexCache(cachePath)
	err := cache.LoadFromFile()
	if err == nil {
		t.Error("expected error for truncated entries")
	}
}

func TestBlockIndexCache_Close(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	cache := NewBlockIndexCache(cachePath)
	cache.AddSlice(1, 100, []BlockIndexCacheEntry{{BlockNum: 100}}, true)

	err := cache.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		t.Error("expected cache file to be saved on close")
	}
}

func TestBlockIndexCache_LoadSliceFromBlocksIndex(t *testing.T) {
	tmpDir := t.TempDir()
	sliceDir := filepath.Join(tmpDir, "slice_00001")
	os.MkdirAll(sliceDir, 0755)

	count := uint32(3)
	data := make([]byte, 4+int(count)*BlockIndexEntrySize)
	binary.LittleEndian.PutUint32(data[0:4], count)

	offset := 4
	for i := uint32(0); i < count; i++ {
		binary.LittleEndian.PutUint32(data[offset:offset+4], 100+i)
		binary.LittleEndian.PutUint64(data[offset+4:offset+12], uint64(i*1024))
		binary.LittleEndian.PutUint32(data[offset+12:offset+16], 512)
		binary.LittleEndian.PutUint64(data[offset+16:offset+24], uint64(1000+i*100))
		binary.LittleEndian.PutUint64(data[offset+24:offset+32], uint64(1099+i*100))
		offset += BlockIndexEntrySize
	}

	os.WriteFile(filepath.Join(sliceDir, "blocks.index"), data, 0644)

	cache := NewBlockIndexCache("")
	err := cache.LoadSliceFromBlocksIndex(sliceDir, 1, 100, true)
	if err != nil {
		t.Fatalf("LoadSliceFromBlocksIndex failed: %v", err)
	}

	slice, exists := cache.GetSlice(1)
	if !exists {
		t.Fatal("slice not loaded")
	}
	if len(slice.Entries) != 3 {
		t.Errorf("len(Entries) = %d, want 3", len(slice.Entries))
	}
	if slice.StartBlock != 100 {
		t.Errorf("StartBlock = %d, want 100", slice.StartBlock)
	}

	entry := slice.Entries[1]
	if entry.BlockNum != 101 {
		t.Errorf("entry.BlockNum = %d, want 101", entry.BlockNum)
	}
	if entry.FileOffset != 1024 {
		t.Errorf("entry.FileOffset = %d, want 1024", entry.FileOffset)
	}
	if entry.GlobMin != 1100 {
		t.Errorf("entry.GlobMin = %d, want 1100", entry.GlobMin)
	}
}

func TestBlockIndexCache_LoadSliceFromBlocksIndex_NotFound(t *testing.T) {
	cache := NewBlockIndexCache("")
	err := cache.LoadSliceFromBlocksIndex("/nonexistent/path", 1, 100, true)
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}

func TestBlockIndexCache_LoadSliceFromBlocksIndex_TooSmall(t *testing.T) {
	tmpDir := t.TempDir()
	sliceDir := filepath.Join(tmpDir, "slice")
	os.MkdirAll(sliceDir, 0755)
	os.WriteFile(filepath.Join(sliceDir, "blocks.index"), []byte{1, 2}, 0644)

	cache := NewBlockIndexCache("")
	err := cache.LoadSliceFromBlocksIndex(sliceDir, 1, 100, true)
	if err == nil {
		t.Error("expected error for too small blocks.index")
	}
}

func BenchmarkSliceBlockIndex_FindBlock(b *testing.B) {
	entries := make([]BlockIndexCacheEntry, 10000)
	for i := range entries {
		entries[i] = BlockIndexCacheEntry{
			BlockNum: uint32(100 + i),
			GlobMin:  uint64(i * 100),
			GlobMax:  uint64(i*100 + 99),
		}
	}

	slice := &SliceBlockIndex{
		SliceNum: 1,
		Entries:  entries,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slice.FindBlock(500000)
	}
}

func BenchmarkBlockIndexCache_FindBlock(b *testing.B) {
	cache := NewBlockIndexCache("")

	entries := make([]BlockIndexCacheEntry, 10000)
	for i := range entries {
		entries[i] = BlockIndexCacheEntry{
			BlockNum: uint32(100 + i),
			GlobMin:  uint64(i * 100),
			GlobMax:  uint64(i*100 + 99),
		}
	}
	cache.AddSlice(1, 100, entries, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.FindBlock(1, 500000)
	}
}
