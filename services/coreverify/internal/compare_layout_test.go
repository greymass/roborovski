package internal

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const layoutFixtures = "../../../libraries/corereader/testdata/payload-layout"

func TestCompareAcceptsDataTableLayoutOnlyWithStaleEmptyData(t *testing.T) {
	for _, name := range []string{"vaulta-269189631-orphan", "vaulta-269184124-reorder"} {
		pa, err := os.ReadFile(filepath.Join(layoutFixtures, name+"-store.bin"))
		if err != nil {
			t.Fatal(err)
		}
		pb, err := os.ReadFile(filepath.Join(layoutFixtures, name+"-compare.bin"))
		if err != nil {
			t.Fatal(err)
		}
		for _, enabled := range []bool{false, true} {
			c := newStaleComparer(enabled)
			c.cfg.IgnoreElapsed = true
			c.comparePayload(1, 1, pa, pb, nil, nil, false)
			mismatches := c.report.Differences[DiffPayloadMismatch]
			if enabled && (mismatches != 0 || c.report.StaleLayoutBlocks != 1) {
				t.Errorf("%s with stale empty data: want no mismatch and one layout block, got %d and %d", name, mismatches, c.report.StaleLayoutBlocks)
			}
			if !enabled && (mismatches != 1 || c.report.StaleLayoutBlocks != 0) {
				t.Errorf("%s without stale empty data: want one mismatch, got %d and %d layout blocks", name, mismatches, c.report.StaleLayoutBlocks)
			}
		}
	}
}

func openTailAt(t *testing.T, store string, block uint32) {
	t.Helper()
	dir := filepath.Join(store, "history_0000010001-0000020000")
	idx, err := LoadBlockIndex(filepath.Join(dir, "blocks.index"))
	if err != nil {
		t.Fatal(err)
	}
	var kept []BlockIndexEntry
	for _, e := range idx.GetAllEntries() {
		if e.BlockNum <= block {
			kept = append(kept, e)
		}
	}
	last := kept[len(kept)-1]
	index := binary.LittleEndian.AppendUint32(nil, uint32(len(kept)))
	for _, e := range kept {
		index = binary.LittleEndian.AppendUint32(index, e.BlockNum)
		index = binary.LittleEndian.AppendUint64(index, e.Offset)
		index = binary.LittleEndian.AppendUint32(index, e.Size)
		index = binary.LittleEndian.AppendUint64(index, e.GlobMin)
		index = binary.LittleEndian.AppendUint64(index, e.GlobMax)
	}
	if err := os.WriteFile(filepath.Join(dir, "blocks.index"), index, 0644); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "data.log"))
	if err != nil {
		t.Fatal(err)
	}
	data = data[:last.Offset+4+uint64(last.Size)+4]
	data[20] = 0
	if err := os.WriteFile(filepath.Join(dir, "data.log"), data, 0644); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(dir, "sha256.txt"))
	os.RemoveAll(filepath.Join(store, "history_0000020001-0000030000"))

	var slices []SliceInfo
	raw, err := os.ReadFile(filepath.Join(store, "slices.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &slices); err != nil {
		t.Fatal(err)
	}
	slices = slices[:2]
	slices[1].EndBlock, slices[1].Finalized, slices[1].GlobMax = block, false, last.GlobMax
	raw, _ = json.Marshal(slices)
	if err := os.WriteFile(filepath.Join(store, "slices.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestCompareAcceptsOpenTailSliceAtRangeEnd(t *testing.T) {
	whole := newFixture(t, baseABIs, baseABIs).compare(t, 1, 10001)
	f := newFixture(t, baseABIs, baseABIs)
	openTailAt(t, f.storeB, 10001)

	report := f.compare(t, 1, 10001)
	if report.TotalDifferences != 0 || report.OpenTailSlices != 1 || report.BlocksCompared != whole.BlocksCompared {
		t.Errorf("range ending in the open slice: want 0 differences, 1 open tail slice and %d blocks, got %v, %d and %d: %+v", whole.BlocksCompared, report.Differences, report.OpenTailSlices, report.BlocksCompared, report.Issues)
	}

	beyond := f.compare(t, 1, 10002)
	if beyond.TotalDifferences == 0 {
		t.Errorf("range past the open slice's last block: want differences, got none")
	}
}
