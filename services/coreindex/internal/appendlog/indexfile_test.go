package appendlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexFileAppendsInPlace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocks.index")
	var entries []BlockIndexEntry
	for n := uint32(2); n < 12; n++ {
		entries = append(entries, BlockIndexEntry{BlockNum: n, Offset: uint64(n) * 100, Size: 50, GlobMin: uint64(n), GlobMax: uint64(n)})
	}
	written, err := AppendIndexFile(path, entries[:4], 0, true)
	if err != nil || written != 4 {
		t.Fatalf("first append: %d, %v", written, err)
	}
	before, _ := os.ReadFile(path)
	written, err = AppendIndexFile(path, entries, written, true)
	if err != nil || written != len(entries) {
		t.Fatalf("second append: %d, %v", written, err)
	}
	after, _ := os.ReadFile(path)
	if string(after[4:len(before)]) != string(before[4:]) {
		t.Fatal("append rewrote committed entries")
	}
	idx, err := LoadBlockIndex(path)
	if err != nil || idx.Count() != len(entries) {
		t.Fatalf("reload: %v", err)
	}

	if err := TruncateIndexFile(path, 3); err != nil {
		t.Fatal(err)
	}
	idx, err = LoadBlockIndex(path)
	if err != nil || idx.Count() != 3 {
		t.Fatalf("after truncate: count %d, %v", idx.Count(), err)
	}
	if err := TruncateIndexFile(path, 5); err == nil {
		t.Fatal("truncate grew the index")
	}
}
