package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFindLastBlockInIndexReadsGlobRange(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(fixtureSlices, "slices.json"))
	if err != nil {
		t.Fatal(err)
	}
	var infos []SliceInfo
	if err := json.Unmarshal(data, &infos); err != nil {
		t.Fatal(err)
	}
	v := &Validator{}
	for _, info := range infos {
		dir := filepath.Join(fixtureSlices, filepathForSlice(info))
		endBlock, globMin, globMax, err := v.findLastBlockInIndex(filepath.Join(dir, "blocks.index"))
		if err != nil {
			t.Fatal(err)
		}
		if endBlock != info.EndBlock || globMin != info.GlobMin || globMax != info.GlobMax {
			t.Errorf("slice %d: got end=%d glob=%d-%d, slices.json says end=%d glob=%d-%d",
				info.SliceNum, endBlock, globMin, globMax, info.EndBlock, info.GlobMin, info.GlobMax)
		}
	}
}

func TestValidateDataLogCountsCRCTrailer(t *testing.T) {
	root := t.TempDir()
	copyTree(t, fixtureSlices, root)
	dir := filepath.Join(root, "history_0000000001-0000010000")
	dataPath := filepath.Join(dir, "data.log")
	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := LoadBlockIndex(filepath.Join(dir, "blocks.index"))
	if err != nil {
		t.Fatal(err)
	}
	slice := SliceInfo{StartBlock: 1, EndBlock: 10000, MaxBlock: 10000}
	v := NewValidator(root, 1, 1000, true, false, 0, false, true)

	if issues := v.validateDataLog(slice, dir, idx); len(issues) != 0 {
		t.Fatalf("intact data.log reported issues: %+v", issues)
	}

	if err := os.Truncate(dataPath, info.Size()-2); err != nil {
		t.Fatal(err)
	}
	issues := v.validateDataLog(slice, dir, idx)
	if len(issues) != 1 || issues[0].Type != "DATA_OUT_OF_BOUNDS" {
		t.Fatalf("truncated CRC trailer: got %+v, want one DATA_OUT_OF_BOUNDS", issues)
	}
}

func filepathForSlice(info SliceInfo) string {
	return fmt.Sprintf("history_%010d-%010d", info.StartBlock, info.MaxBlock)
}
