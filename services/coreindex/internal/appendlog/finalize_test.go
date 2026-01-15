package appendlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSliceFinalizeValidation_Success(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
	}

	if err := slice.Load(true); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for blockNum := uint32(1); blockNum <= 10; blockNum++ {
		data := []byte("block data")
		globMin := uint64(blockNum * 10)
		globMax := globMin + 9
		if _, err := slice.AppendBlock(blockNum, data, globMin, globMax, false); err != nil {
			t.Fatalf("AppendBlock %d failed: %v", blockNum, err)
		}
	}

	if err := slice.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	if !slice.Finalized {
		t.Error("Slice should be marked as finalized")
	}

	dataLogPath := filepath.Join(slicePath, "data.log")
	header, err := ReadDataLogHeader(dataLogPath)
	if err != nil {
		t.Fatalf("ReadDataLogHeader failed: %v", err)
	}
	if header.Finalized != 1 {
		t.Errorf("Header.Finalized = %d, expected 1", header.Finalized)
	}

	hashPath := filepath.Join(slicePath, "sha256.txt")
	hashData, err := os.ReadFile(hashPath)
	if err != nil {
		t.Fatalf("Failed to read sha256.txt: %v", err)
	}
	hashStr := string(hashData)
	if len(hashStr) < 64 {
		t.Errorf("sha256.txt content too short: %q", hashStr)
	}
	if hashStr[64] != '\n' {
		t.Errorf("sha256.txt should end with newline")
	}
}

func TestSliceFinalizeValidation_MissingBlocks(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
	}

	if err := slice.Load(true); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for blockNum := uint32(1); blockNum <= 5; blockNum++ {
		data := []byte("block data")
		globMin := uint64(blockNum * 10)
		globMax := globMin + 9
		if _, err := slice.AppendBlock(blockNum, data, globMin, globMax, false); err != nil {
			t.Fatalf("AppendBlock %d failed: %v", blockNum, err)
		}
	}

	err := slice.Finalize()
	if err == nil {
		t.Error("Finalize should fail with missing blocks")
	}
}

func TestSliceFinalizeValidation_WrongStartBlock(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
	}

	if err := slice.Load(true); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for blockNum := uint32(2); blockNum <= 11; blockNum++ {
		data := []byte("block data")
		globMin := uint64(blockNum * 10)
		globMax := globMin + 9
		if _, err := slice.AppendBlock(blockNum, data, globMin, globMax, false); err != nil {
			t.Fatalf("AppendBlock %d failed: %v", blockNum, err)
		}
	}

	err := slice.Finalize()
	if err == nil {
		t.Error("Finalize should fail with wrong start block")
	}
}

func TestSliceFinalizeValidation_Gap(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
	}

	if err := slice.Load(true); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for blockNum := uint32(1); blockNum <= 10; blockNum++ {
		if blockNum == 5 {
			continue
		}
		data := []byte("block data")
		globMin := uint64(blockNum * 10)
		globMax := globMin + 9
		if _, err := slice.AppendBlock(blockNum, data, globMin, globMax, false); err != nil {
			t.Fatalf("AppendBlock %d failed: %v", blockNum, err)
		}
	}

	err := slice.Finalize()
	if err == nil {
		t.Error("Finalize should fail with gap in block sequence")
	}
}

func TestSliceFinalizeValidation_NoHeader(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	dataLogPath := filepath.Join(slicePath, "data.log")
	// Create an empty data.log file (legacy format without header)
	f, err := os.Create(dataLogPath)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
		blockIndex:     NewBlockIndex(),
	}

	reader, err := NewReader(dataLogPath)
	if err != nil {
		t.Fatal(err)
	}
	slice.reader = reader

	err = slice.Finalize()
	if err != nil {
		t.Errorf("Finalize without header should succeed (legacy mode): %v", err)
	}

	hashPath := filepath.Join(slicePath, "sha256.txt")
	if _, err := os.Stat(hashPath); !os.IsNotExist(err) {
		t.Error("sha256.txt should NOT be created for legacy slices without header")
	}
}

func TestHashIncludesFinalizedFlag(t *testing.T) {
	tmpDir := t.TempDir()
	slicePath := filepath.Join(tmpDir, "history_0000000001-0000000010")
	if err := os.MkdirAll(slicePath, 0755); err != nil {
		t.Fatal(err)
	}

	slice := &Slice{
		SliceNum:       0,
		StartBlock:     1,
		MaxBlock:       10,
		BlocksPerSlice: 10,
		basePath:       slicePath,
	}

	if err := slice.Load(true); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for blockNum := uint32(1); blockNum <= 10; blockNum++ {
		data := []byte("block data")
		globMin := uint64(blockNum * 10)
		globMax := globMin + 9
		if _, err := slice.AppendBlock(blockNum, data, globMin, globMax, false); err != nil {
			t.Fatalf("AppendBlock %d failed: %v", blockNum, err)
		}
	}

	if err := slice.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	hashPath := filepath.Join(slicePath, "sha256.txt")
	hashData, err := os.ReadFile(hashPath)
	if err != nil {
		t.Fatalf("Failed to read sha256.txt: %v", err)
	}
	storedHash := string(hashData[:64])

	dataLogPath := filepath.Join(slicePath, "data.log")
	computedHash, err := ComputeFileHash(dataLogPath)
	if err != nil {
		t.Fatalf("ComputeFileHash failed: %v", err)
	}

	if storedHash != computedHash {
		t.Errorf("Stored hash %s != computed hash %s", storedHash, computedHash)
	}

	header, err := ReadDataLogHeader(dataLogPath)
	if err != nil {
		t.Fatal(err)
	}
	if header.Finalized != 1 {
		t.Error("Finalized flag should be 1 in the file that was hashed")
	}
}
