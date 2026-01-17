package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestIndexes_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "indexes_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if !idx.IsBulkMode() {
		t.Error("Expected bulk mode to be true initially")
	}

	libNum, headNum, err := idx.GetProperties()
	if err != nil {
		t.Fatal(err)
	}
	if libNum != 0 || headNum != 0 {
		t.Errorf("Expected 0,0 for fresh db, got %d,%d", libNum, headNum)
	}

	account := uint64(12345)
	contract := uint64(67890)
	action := uint64(11111)

	for i := uint64(0); i < 100; i++ {
		idx.Add(account, contract, action, 1000+i, 1000000+uint32(i))
	}

	if err := idx.Commit(100, 100); err != nil {
		t.Fatal(err)
	}

	libNum, headNum, err = idx.GetProperties()
	if err != nil {
		t.Fatal(err)
	}
	if libNum != 100 || headNum != 100 {
		t.Errorf("Expected 100,100 after commit, got %d,%d", libNum, headNum)
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexes_BulkToLiveTransition(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "indexes_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if !idx.IsBulkMode() {
		t.Error("Expected bulk mode initially")
	}

	account := uint64(12345)
	contract := uint64(67890)
	action := uint64(11111)

	for i := uint64(0); i < 50; i++ {
		idx.Add(account, contract, action, 1000+i, 1000000+uint32(i))
	}

	if err := idx.Commit(50, 50); err != nil {
		t.Fatal(err)
	}

	idx.SetBulkMode(false)

	if idx.IsBulkMode() {
		t.Error("Expected bulk mode to be false after SetBulkMode(false)")
	}

	for i := uint64(50); i < 100; i++ {
		idx.Add(account, contract, action, 1000+i, 1000000+uint32(i))
	}

	if err := idx.Commit(100, 100); err != nil {
		t.Fatal(err)
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexes_ImplementsInterface(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "indexes_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	var _ ActionIndexer = idx
}

func TestIndexes_QueryAfterAdd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "indexes_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	account := uint64(12345)
	contract := uint64(67890)
	action := uint64(11111)

	for i := uint64(0); i < 100; i++ {
		idx.Add(account, contract, action, 1000+i, 1000000+uint32(i))
	}

	if err := idx.Commit(100, 100); err != nil {
		t.Fatal(err)
	}

	idx.SetBulkMode(false)

	seqs, err := idx.chunkReader.GetLastN(account, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(seqs) != 10 {
		t.Errorf("Expected 10 sequences, got %d", len(seqs))
	}

	if len(seqs) > 0 && seqs[len(seqs)-1] != 1099 {
		t.Errorf("Expected last seq to be 1099, got %d", seqs[len(seqs)-1])
	}

	contractSeqs, err := idx.chunkReader.GetContractActionLastN(account, contract, action, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(contractSeqs) != 10 {
		t.Errorf("Expected 10 contract sequences, got %d", len(contractSeqs))
	}

	wildcardSeqs, err := idx.chunkReader.GetContractWildcardLastN(account, contract, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(wildcardSeqs) != 10 {
		t.Errorf("Expected 10 wildcard sequences, got %d", len(wildcardSeqs))
	}
}
