package tracereader

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// Test Helper Functions
// =============================================================================

func randomString() string {
	return filepath.Base(os.TempDir())
}

// createTestIndexFile creates a minimal valid trace index file
func createTestIndexFile(path string, headBlock uint32, libBlock uint32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write version
	if err := binary.Write(f, binary.LittleEndian, HeaderVersion); err != nil {
		return err
	}

	// Write a block entry (variant 0)
	// Variant index (0 = BlockEntryV0)
	f.Write([]byte{0})

	// BlockEntryV0: Id (32 bytes), Number (4 bytes), Offset (8 bytes)
	id := [32]byte{0xAA, 0xBB, 0xCC}
	f.Write(id[:])
	binary.Write(f, binary.LittleEndian, headBlock)
	binary.Write(f, binary.LittleEndian, uint64(0))

	// Write a LIB entry (variant 1)
	f.Write([]byte{1})
	binary.Write(f, binary.LittleEndian, libBlock)

	return nil
}

// createTestIndexFileWithLib creates index file with only LIB entries
func createTestIndexFileWithLib(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write version
	if err := binary.Write(f, binary.LittleEndian, HeaderVersion); err != nil {
		return err
	}

	// Write multiple LIB entries (variant 1)
	for i := uint32(10); i <= 100; i += 10 {
		f.Write([]byte{1}) // Variant 1 = LibEntryV0
		binary.Write(f, binary.LittleEndian, i)
	}

	// Add one block entry at end
	f.Write([]byte{0}) // Variant 0 = BlockEntryV0
	id := [32]byte{0xEE}
	f.Write(id[:])
	binary.Write(f, binary.LittleEndian, uint32(105))
	binary.Write(f, binary.LittleEndian, uint64(12345))

	return nil
}

// createVarUint32 creates a varuint32 encoded value
func createVarUint32(val uint32) []byte {
	var buf bytes.Buffer
	for {
		b := byte(val & 0x7f)
		val >>= 7
		if val != 0 {
			b |= 0x80
		}
		buf.WriteByte(b)
		if val == 0 {
			break
		}
	}
	return buf.Bytes()
}

// createTestBlockTraceV2 creates a minimal BlockTraceV2 for testing
func createTestBlockTraceV2() []byte {
	var buf bytes.Buffer

	// ID (32 bytes)
	blockID := [32]byte{0xaa, 0xbb, 0xcc, 0xdd}
	buf.Write(blockID[:])

	// Number (uint32)
	binary.Write(&buf, binary.LittleEndian, uint32(1000))

	// PreviousID (32 bytes)
	prevID := [32]byte{0x11, 0x22, 0x33, 0x44}
	buf.Write(prevID[:])

	// Timestamp (uint32)
	binary.Write(&buf, binary.LittleEndian, uint32(1609459200))

	// Producer (uint64)
	binary.Write(&buf, binary.LittleEndian, uint64(0x0000000000ea3055))

	// TransactionMroot (32 bytes)
	txMroot := [32]byte{}
	buf.Write(txMroot[:])

	// ActionMroot (32 bytes)
	actMroot := [32]byte{}
	buf.Write(actMroot[:])

	// ScheduleVersion (uint32)
	binary.Write(&buf, binary.LittleEndian, uint32(1))

	// TransactionsVariant (0 for V2)
	buf.Write(createVarUint32(0))

	// Transactions count (0)
	buf.Write(createVarUint32(0))

	return buf.Bytes()
}

// =============================================================================
// Config and Helper Tests
// =============================================================================

func TestConfigBasic(t *testing.T) {
	conf := &Config{
		Debug:  true,
		Stride: 500,
		Dir:    "/tmp/test",
	}

	if conf.Stride != 500 {
		t.Errorf("Stride = %d; want 500", conf.Stride)
	}

	if conf.Dir != "/tmp/test" {
		t.Errorf("Dir = %q; want /tmp/test", conf.Dir)
	}
}

func TestStrideFmt(t *testing.T) {
	conf := &Config{Stride: 500}

	tests := []struct {
		blockNum uint32
		expected string
	}{
		{0, "0000000000-0000000500"},
		{1, "0000000000-0000000500"},
		{499, "0000000000-0000000500"},
		{500, "0000000500-0000001000"},
		{501, "0000000500-0000001000"},
		{999, "0000000500-0000001000"},
		{1000, "0000001000-0000001500"},
		{123456, "0000123000-0000123500"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := strideFmt(tt.blockNum, conf)
			if result != tt.expected {
				t.Errorf("strideFmt(%d) = %q; want %q", tt.blockNum, result, tt.expected)
			}
		})
	}
}

func TestGetInfoNoTraceFiles(t *testing.T) {
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    "/tmp/nonexistent-trace-dir",
	}

	_, err := GetInfo(conf)
	if err == nil {
		t.Error("Expected error for non-existent directory")
	}
}

func TestGetInfo_WithValidDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexFile, 100, 50); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	info, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}

	if info.Head != 100 {
		t.Errorf("GetInfo().Head = %d, want 100", info.Head)
	}
	if info.Lib != 50 {
		t.Errorf("GetInfo().Lib = %d, want 50", info.Lib)
	}
}

func TestGetInfo_CacheHit(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexFile, 200, 150); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	info1, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("First GetInfo() error = %v", err)
	}

	info2, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("Second GetInfo() error = %v", err)
	}

	if info1.Head != info2.Head || info1.Lib != info2.Lib {
		t.Errorf("Cache should return same values: first=%+v, second=%+v", info1, info2)
	}
}

func TestGetInfo_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	_, err := GetInfo(conf)
	if err == nil {
		t.Error("Expected error for empty directory")
	}
}

func TestGetInfo_MultipleStrideFiles(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile1 := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexFile1, 100, 50); err != nil {
		t.Fatalf("Failed to create test index file 1: %v", err)
	}

	indexFile2 := filepath.Join(tmpDir, "trace_index_0000000500-0000001000.log")
	if err := createTestIndexFile(indexFile2, 600, 550); err != nil {
		t.Fatalf("Failed to create test index file 2: %v", err)
	}

	info, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}

	if info.Head != 600 {
		t.Errorf("GetInfo().Head = %d, want 600 (from latest stride)", info.Head)
	}
}

func TestOpenSliceIndex_ValidVersion(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "test_index.log")

	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	binary.Write(f, binary.LittleEndian, HeaderVersion)
	f.Close()

	conf := &Config{Debug: false}
	file, err := openSliceIndex(indexFile, conf)
	if err != nil {
		t.Fatalf("openSliceIndex() error = %v", err)
	}
	defer file.Close()

	if file == nil {
		t.Error("openSliceIndex() returned nil file")
	}
}

func TestOpenSliceIndex_UnsupportedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "test_index.log")

	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	binary.Write(f, binary.LittleEndian, uint32(2))
	f.Close()

	conf := &Config{Debug: false}
	_, err = openSliceIndex(indexFile, conf)
	if err == nil {
		t.Error("Expected error for unsupported version")
	}
}

func TestOpenSliceIndex_HeaderlessFile(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "test_index.log")

	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	binary.Write(f, binary.LittleEndian, uint32(999999))
	f.Close()

	conf := &Config{Debug: false}
	file, err := openSliceIndex(indexFile, conf)
	if err != nil {
		t.Fatalf("openSliceIndex() error = %v (should handle headerless file)", err)
	}
	defer file.Close()

	if file == nil {
		t.Error("openSliceIndex() returned nil file")
	}
}

func TestOpenSliceIndex_NonExistent(t *testing.T) {
	conf := &Config{Debug: false}
	_, err := openSliceIndex("/nonexistent/path/to/file.log", conf)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestGetRawBlocksWithMetadata_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	_, err := GetRawBlocksWithMetadata(100, 10, conf)
	if err != ErrNotFound {
		t.Errorf("GetRawBlocksWithMetadata() error = %v, want ErrNotFound", err)
	}
}

func TestGetRawBlocksWithMetadata_BlockNotInIndex(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexFile, 100, 50); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	_, err := GetRawBlocksWithMetadata(200, 10, conf)
	if err != ErrNotFound {
		t.Errorf("GetRawBlocksWithMetadata() error = %v, want ErrNotFound (block 200 not in index starting at 100)", err)
	}
}

func TestGetRawBlocksWithMetadata_FoundWithTraceFile(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFileWithMultipleBlocks(indexFile, 100, 105); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	traceData := createTestBlockTraceV2()
	traceFile := filepath.Join(tmpDir, "trace_0000000000-0000000500.log")
	if err := os.WriteFile(traceFile, traceData, 0644); err != nil {
		t.Fatalf("Failed to create trace file: %v", err)
	}

	rawBlocks, err := GetRawBlocksWithMetadata(100, 1, conf)
	if err != nil {
		t.Fatalf("GetRawBlocksWithMetadata() error = %v", err)
	}

	if len(rawBlocks) == 0 {
		t.Error("Expected at least one raw block")
	}
}

func createTestIndexFileWithMultipleBlocks(path string, startBlock, endBlock uint32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.LittleEndian, HeaderVersion); err != nil {
		return err
	}

	offset := uint64(0)
	blockDataSize := uint64(100)

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		f.Write([]byte{0})

		id := [32]byte{byte(blockNum & 0xFF), byte((blockNum >> 8) & 0xFF)}
		f.Write(id[:])
		binary.Write(f, binary.LittleEndian, blockNum)
		binary.Write(f, binary.LittleEndian, offset)
		offset += blockDataSize
	}

	f.Write([]byte{1})
	binary.Write(f, binary.LittleEndian, startBlock-10)

	return nil
}

func TestGetInfo_DebugMode(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  true,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexFile, 100, 50); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	info, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}

	if info.Head != 100 {
		t.Errorf("GetInfo().Head = %d, want 100", info.Head)
	}
}

func TestGetInfo_LibEntryOnly(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Debug:  false,
		Stride: 500,
		Dir:    tmpDir,
	}

	indexFile := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFileWithLib(indexFile); err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}

	info, err := GetInfo(conf)
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}

	if info.Lib != 100 {
		t.Errorf("GetInfo().Lib = %d, want 100", info.Lib)
	}
	if info.Head != 105 {
		t.Errorf("GetInfo().Head = %d, want 105", info.Head)
	}
}

func TestOpenSliceIndex_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "empty.log")

	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Close()

	conf := &Config{Debug: false}
	_, err = openSliceIndex(indexFile, conf)
	if err == nil {
		t.Error("Expected error for empty file")
	}
}

// =============================================================================
