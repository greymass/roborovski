package appendlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHeaderRoundtrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "header-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataLogPath := filepath.Join(tmpDir, "data.log")

	header := NewDataLogHeader(10000, 1, 10000)

	writer, err := NewWriterWithHeader(dataLogPath, header)
	if err != nil {
		t.Fatalf("NewWriterWithHeader failed: %v", err)
	}

	testData := []byte("test block data")
	offset, err := writer.Append(testData)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if offset != HeaderSize {
		t.Errorf("First block offset = %d, expected %d", offset, HeaderSize)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	hasHdr, err := HasHeader(dataLogPath)
	if err != nil {
		t.Fatalf("HasHeader failed: %v", err)
	}
	if !hasHdr {
		t.Error("HasHeader returned false for headerized file")
	}

	readHeader, err := ReadDataLogHeader(dataLogPath)
	if err != nil {
		t.Fatalf("ReadDataLogHeader failed: %v", err)
	}

	if string(readHeader.Magic[:]) != HeaderMagic {
		t.Errorf("Magic = %q, expected %q", string(readHeader.Magic[:]), HeaderMagic)
	}
	if readHeader.Version != HeaderVersion {
		t.Errorf("Version = %d, expected %d", readHeader.Version, HeaderVersion)
	}
	if readHeader.BlocksPerSlice != 10000 {
		t.Errorf("BlocksPerSlice = %d, expected 10000", readHeader.BlocksPerSlice)
	}
	if readHeader.StartBlock != 1 {
		t.Errorf("StartBlock = %d, expected 1", readHeader.StartBlock)
	}
	if readHeader.EndBlock != 10000 {
		t.Errorf("EndBlock = %d, expected 10000", readHeader.EndBlock)
	}
	if readHeader.Finalized != 0 {
		t.Errorf("Finalized = %d, expected 0", readHeader.Finalized)
	}

	reader, err := NewReaderWithHeader(dataLogPath)
	if err != nil {
		t.Fatalf("NewReaderWithHeader failed: %v", err)
	}
	defer reader.Close()

	if reader.Header() == nil {
		t.Error("Reader.Header() returned nil")
	}

	data, err := reader.ReadAt(HeaderSize)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("ReadAt data = %q, expected %q", string(data), string(testData))
	}
}

func TestUpdateFinalizedFlag(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "finalized-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataLogPath := filepath.Join(tmpDir, "data.log")

	header := NewDataLogHeader(10000, 1, 10000)
	writer, err := NewWriterWithHeader(dataLogPath, header)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	readHeader, _ := ReadDataLogHeader(dataLogPath)
	if readHeader.Finalized != 0 {
		t.Errorf("Initial Finalized = %d, expected 0", readHeader.Finalized)
	}

	if err := UpdateFinalizedFlag(dataLogPath, true); err != nil {
		t.Fatalf("UpdateFinalizedFlag failed: %v", err)
	}

	readHeader, _ = ReadDataLogHeader(dataLogPath)
	if readHeader.Finalized != 1 {
		t.Errorf("After update Finalized = %d, expected 1", readHeader.Finalized)
	}
}

func TestWriterResumeWithHeader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resume-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataLogPath := filepath.Join(tmpDir, "data.log")

	header := NewDataLogHeader(10000, 1, 10000)

	writer1, err := NewWriterWithHeader(dataLogPath, header)
	if err != nil {
		t.Fatal(err)
	}
	offset1, _ := writer1.Append([]byte("block1"))
	writer1.Close()

	writer2, err := NewWriterWithHeader(dataLogPath, header)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	offset2, _ := writer2.Append([]byte("block2"))
	writer2.Close()

	if offset1 != HeaderSize {
		t.Errorf("First offset = %d, expected %d", offset1, HeaderSize)
	}
	if offset2 <= offset1 {
		t.Errorf("Second offset %d should be > first offset %d", offset2, offset1)
	}
}

func TestScanFromOffset(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "scan-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataLogPath := filepath.Join(tmpDir, "data.log")

	header := NewDataLogHeader(10000, 1, 10000)
	writer, _ := NewWriterWithHeader(dataLogPath, header)
	writer.Append([]byte("block1"))
	writer.Append([]byte("block2"))
	writer.Append([]byte("block3"))
	writer.Close()

	reader, _ := NewReaderWithHeader(dataLogPath)
	defer reader.Close()

	var blocks []string
	err = reader.ScanWithHeader(func(offset uint64, data []byte) error {
		blocks = append(blocks, string(data))
		return nil
	})
	if err != nil {
		t.Fatalf("ScanWithHeader failed: %v", err)
	}

	if len(blocks) != 3 {
		t.Errorf("Scanned %d blocks, expected 3", len(blocks))
	}
	if blocks[0] != "block1" || blocks[1] != "block2" || blocks[2] != "block3" {
		t.Errorf("Unexpected blocks: %v", blocks)
	}
}

func TestComputeFileHash(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hash-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.bin")
	testData := []byte("hello world")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}

	hash, err := ComputeFileHash(testFile)
	if err != nil {
		t.Fatalf("ComputeFileHash failed: %v", err)
	}

	expectedHash := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if hash != expectedHash {
		t.Errorf("Hash = %s, expected %s", hash, expectedHash)
	}

	if len(hash) != 64 {
		t.Errorf("Hash length = %d, expected 64", len(hash))
	}
}

func TestComputeFileHashAfterFinalizedFlag(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hash-finalized-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataLogPath := filepath.Join(tmpDir, "data.log")

	header := NewDataLogHeader(10, 1, 10)
	writer, err := NewWriterWithHeader(dataLogPath, header)
	if err != nil {
		t.Fatal(err)
	}
	writer.Append([]byte("block data"))
	writer.Close()

	hashBefore, err := ComputeFileHash(dataLogPath)
	if err != nil {
		t.Fatalf("ComputeFileHash before finalize failed: %v", err)
	}

	if err := UpdateFinalizedFlag(dataLogPath, true); err != nil {
		t.Fatal(err)
	}

	hashAfter, err := ComputeFileHash(dataLogPath)
	if err != nil {
		t.Fatalf("ComputeFileHash after finalize failed: %v", err)
	}

	if hashBefore == hashAfter {
		t.Error("Hash should change after finalized flag is set")
	}
}
