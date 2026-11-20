package appendlog

import (
	"bytes"
	"testing"
)

func TestSyncForCommitReadsHeadFromDisk(t *testing.T) {
	store, err := NewSliceStore(t.TempDir(), SliceStoreOptions{BlocksPerSlice: 100, EnableZstd: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetCommittedHead(0); err != nil {
		t.Fatal(err)
	}
	var batch []BlockEntry
	for n := uint32(2); n <= 150; n++ {
		batch = append(batch, BlockEntry{BlockNum: n, Data: bytes.Repeat([]byte{byte(n)}, 64), GlobMin: uint64(n), GlobMax: uint64(n)})
	}
	if err := store.AppendBlockBatch(batch); err != nil {
		t.Fatal(err)
	}
	store.headData = nil
	cs, err := store.SyncForCommit()
	if err != nil {
		t.Fatal(err)
	}
	if cs.Head != 150 || cs.SliceIndexLen != 50 {
		t.Fatalf("bounds head=%d len=%d", cs.Head, cs.SliceIndexLen)
	}
	if !bytes.Equal(cs.HeadData, batch[len(batch)-1].Data) {
		t.Fatal("head block read from disk differs from what was written")
	}
}
