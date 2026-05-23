package tracereader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type forkScenarioEntry struct {
	Number uint32
	Marker byte
	Size   int
}

func writeForkScenario(t *testing.T, dir string, lo, hi uint32, entries []forkScenarioEntry, libBlock uint32) (string, string) {
	t.Helper()
	stride := fmt.Sprintf("%010d-%010d", lo, hi)
	indexPath := filepath.Join(dir, "trace_index_"+stride+".log")
	dataPath := filepath.Join(dir, "trace_"+stride+".log")

	indexBuf := new(bytes.Buffer)
	dataBuf := new(bytes.Buffer)
	binary.Write(indexBuf, binary.LittleEndian, HeaderVersion)

	offset := uint64(0)
	for i, e := range entries {
		indexBuf.WriteByte(0) // variant tag = BlockEntryV0
		var id [32]byte
		id[0] = e.Marker
		id[1] = byte(i)
		indexBuf.Write(id[:])
		binary.Write(indexBuf, binary.LittleEndian, e.Number)
		binary.Write(indexBuf, binary.LittleEndian, offset)

		for j := 0; j < e.Size; j++ {
			dataBuf.WriteByte(e.Marker)
		}
		offset += uint64(e.Size)
	}

	indexBuf.WriteByte(1) // variant tag = LibEntryV0
	binary.Write(indexBuf, binary.LittleEndian, libBlock)

	if err := os.WriteFile(indexPath, indexBuf.Bytes(), 0644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(dataPath, dataBuf.Bytes(), 0644); err != nil {
		t.Fatalf("write data: %v", err)
	}
	return indexPath, dataPath
}

func TestGetRawBlocksWithMetadata_PicksCanonicalAcrossForks(t *testing.T) {
	dir := t.TempDir()
	conf := &Config{Stride: 500, Dir: dir}

	writeForkScenario(t, dir, 0, 500, []forkScenarioEntry{
		{Number: 100, Marker: 'A', Size: 64},
		{Number: 101, Marker: 'F', Size: 64}, // forked 101
		{Number: 101, Marker: 'C', Size: 64}, // canonical 101 (last entry wins)
		{Number: 102, Marker: 'D', Size: 64},
	}, 110)

	rawBlocks, err := GetRawBlocksWithMetadata(101, 1, conf)
	if err != nil {
		t.Fatalf("GetRawBlocksWithMetadata error: %v", err)
	}
	if len(rawBlocks) != 1 {
		t.Fatalf("expected 1 raw block, got %d", len(rawBlocks))
	}
	got := rawBlocks[0]
	if got.BlockNum != 101 {
		t.Fatalf("BlockNum = %d, want 101", got.BlockNum)
	}
	if len(got.RawBytes) != 64 {
		t.Fatalf("RawBytes len = %d, want 64", len(got.RawBytes))
	}
	for i, b := range got.RawBytes {
		if b != 'C' {
			t.Fatalf("RawBytes[%d] = %q, want 'C' (canonical). Forked marker 'F' indicates the reader picked the first-written (orphaned) trace_index entry instead of the last (canonical) one.", i, b)
		}
	}
}

func TestGetRawBlocksWithMetadata_PicksCanonicalForRangeAcrossForks(t *testing.T) {
	dir := t.TempDir()
	conf := &Config{Stride: 500, Dir: dir}

	writeForkScenario(t, dir, 0, 500, []forkScenarioEntry{
		{Number: 100, Marker: 'A', Size: 32},
		{Number: 101, Marker: 'F', Size: 32}, // forked 101
		{Number: 101, Marker: 'C', Size: 32}, // canonical 101
		{Number: 102, Marker: 'D', Size: 32},
	}, 110)

	rawBlocks, err := GetRawBlocksWithMetadata(100, 3, conf)
	if err != nil {
		t.Fatalf("GetRawBlocksWithMetadata error: %v", err)
	}
	if len(rawBlocks) != 3 {
		t.Fatalf("expected 3 raw blocks (100, 101, 102), got %d", len(rawBlocks))
	}

	want := []struct {
		num    uint32
		marker byte
	}{
		{100, 'A'},
		{101, 'C'},
		{102, 'D'},
	}
	for i, w := range want {
		if rawBlocks[i].BlockNum != w.num {
			t.Errorf("rawBlocks[%d].BlockNum = %d, want %d", i, rawBlocks[i].BlockNum, w.num)
		}
		if len(rawBlocks[i].RawBytes) == 0 || rawBlocks[i].RawBytes[0] != w.marker {
			t.Errorf("rawBlocks[%d] marker = %q, want %q", i, rawBlocks[i].RawBytes[0], w.marker)
		}
	}
}
