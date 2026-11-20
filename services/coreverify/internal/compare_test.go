package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/compression"
	"github.com/greymass/roborovski/libraries/corereader"
)

const fixtureSlices = "../../../libraries/corereader/testdata/slices"

const abiHi = `{"version":"eosio::abi/1.1","types":[],"structs":[{"name":"hi","base":"","fields":[{"name":"user","type":"name"}]}],"actions":[{"name":"hi","type":"hi","ricardian_contract":""}],"tables":[]}`

const abiHiReformatted = `{
  "structs": [{"fields": [{"type": "name", "name": "user"}], "base": "", "name": "hi"}],
  "version": "eosio::abi/1.1",
  "actions": [{"ricardian_contract": "", "type": "hi", "name": "hi"}],
  "types": [],
  "tables": []
}`

const abiBye = `{"version":"eosio::abi/1.1","types":[],"structs":[{"name":"bye","base":"","fields":[]}],"actions":[{"name":"bye","type":"bye","ricardian_contract":""}],"tables":[]}`

type abiWrite struct {
	block    uint32
	contract string
	abi      string
}

var baseABIs = []abiWrite{
	{100, "alice", abiHi},
	{200, "alice", abiBye},
	{200, "bob", abiHi},
	{300, "alice", ""},
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func writeABIs(t *testing.T, dir string, writes []abiWrite) {
	t.Helper()
	w, err := abicache.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range writes {
		if err := w.Write(e.block, chain.StringToName(e.contract), []byte(e.abi)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

type fixture struct {
	storeA, storeB, abiA, abiB string
}

func newFixture(t *testing.T, abisA, abisB []abiWrite) fixture {
	t.Helper()
	root := t.TempDir()
	f := fixture{
		storeA: filepath.Join(root, "a"),
		storeB: filepath.Join(root, "b"),
		abiA:   filepath.Join(root, "abi-a"),
		abiB:   filepath.Join(root, "abi-b"),
	}
	copyTree(t, fixtureSlices, f.storeA)
	copyTree(t, fixtureSlices, f.storeB)
	writeABIs(t, f.abiA, abisA)
	writeABIs(t, f.abiB, abisB)
	return f
}

func (f fixture) compare(t *testing.T, start, end uint32) *CompareReport {
	t.Helper()
	report, err := NewComparer(CompareConfig{
		StorePath:      f.storeA,
		ComparePath:    f.storeB,
		ABIPath:        f.abiA,
		CompareABIPath: f.abiB,
		StartBlock:     start,
		EndBlock:       end,
		Workers:        4,
		BatchSize:      500,
		OutputJSON:     true,
	}).Compare()
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestCompareIdenticalStores(t *testing.T) {
	abisB := append([]abiWrite(nil), baseABIs...)
	abisB[0].abi = abiHiReformatted
	f := newFixture(t, baseABIs, abisB)

	report := f.compare(t, 0, 0)

	if report.TotalDifferences != 0 || report.Status != "passed" {
		t.Fatalf("expected no differences, got %v: %+v", report.Differences, report.Issues)
	}
	if report.StartBlock != 1 || report.EndBlock != 30000 {
		t.Errorf("range = %d-%d, want 1-30000", report.StartBlock, report.EndBlock)
	}
	if report.SlicesCompared != 3 || report.BlocksCompared != 29999 {
		t.Errorf("compared %d slices and %d blocks, want 3 and 29999", report.SlicesCompared, report.BlocksCompared)
	}
	if report.ActionsCompared == 0 || report.TrxCompared == 0 {
		t.Errorf("expected actions and transactions to be compared, got %d and %d", report.ActionsCompared, report.TrxCompared)
	}
	if report.ABIEntriesCompared != 4 {
		t.Errorf("ABI entries compared = %d, want 4", report.ABIEntriesCompared)
	}
	if report.Cases[CaseBlocks] != 29999 || report.Cases[CaseABIClears] != 1 || report.Cases[CaseABIMultiEntryBlocks] != 1 {
		t.Errorf("unexpected case counts: %v", report.Cases)
	}
}

func TestCompareChangedAction(t *testing.T) {
	f := newFixture(t, baseABIs, baseABIs)
	blockNum := changeOneAction(t, f.storeA, f.storeB)
	sliceStart := (blockNum-1)/10000*10000 + 1

	report := f.compare(t, sliceStart, sliceStart+9999)

	if report.Differences[DiffPayloadMismatch] != 1 {
		t.Errorf("PAYLOAD_MISMATCH = %d, want 1", report.Differences[DiffPayloadMismatch])
	}
	if report.Differences[DiffActionMismatch] != 1 {
		t.Errorf("ACTION_MISMATCH = %d, want 1", report.Differences[DiffActionMismatch])
	}
	for _, kind := range []string{DiffFrameError, DiffSHA256Mismatch, DiffIndexMismatch, DiffHeaderMismatch, DiffBlockMissing, DiffReadError} {
		if report.Differences[kind] != 0 {
			t.Errorf("unexpected %s: %d", kind, report.Differences[kind])
		}
	}
	for _, issue := range report.Issues {
		if issue.BlockNum != blockNum {
			t.Errorf("difference reported at block %d, want %d: %+v", issue.BlockNum, blockNum, issue)
		}
	}
}

func TestCompareReorderedABIEntry(t *testing.T) {
	abisB := []abiWrite{baseABIs[0], baseABIs[2], baseABIs[1], baseABIs[3]}
	f := newFixture(t, baseABIs, abisB)

	report := f.compare(t, 1, 1000)

	if report.Differences[DiffABIOrderMismatch] != 1 || report.TotalDifferences != 1 {
		t.Fatalf("want exactly one ABI_ORDER_MISMATCH, got %v: %+v", report.Differences, report.Issues)
	}
	if report.Issues[0].BlockNum != 200 {
		t.Errorf("order mismatch at block %d, want 200", report.Issues[0].BlockNum)
	}
}

func TestCompareDroppedABIClear(t *testing.T) {
	f := newFixture(t, baseABIs, baseABIs[:3])

	report := f.compare(t, 1, 1000)

	if report.Differences[DiffABIEntryMissing] != 1 || report.TotalDifferences != 1 {
		t.Fatalf("want exactly one ABI_ENTRY_MISSING, got %v: %+v", report.Differences, report.Issues)
	}
	if report.Issues[0].BlockNum != 300 {
		t.Errorf("missing entry at block %d, want 300", report.Issues[0].BlockNum)
	}
}

func TestCompareSnapshotABIEntriesAreListed(t *testing.T) {
	abisB := append([]abiWrite{{50, "carol", abiHi}}, baseABIs...)
	f := newFixture(t, baseABIs, abisB)

	report, err := NewComparer(CompareConfig{
		StorePath: f.storeA, ComparePath: f.storeB, ABIPath: f.abiA, CompareABIPath: f.abiB,
		StartBlock: 1, EndBlock: 1000, ABISnapshotBlock: 50, OutputJSON: true,
	}).Compare()
	if err != nil {
		t.Fatal(err)
	}
	if report.TotalDifferences != 0 {
		t.Fatalf("expected no differences, got %v", report.Differences)
	}
	if len(report.SnapshotABIEntries) != 1 || report.SnapshotABIEntries[0].Contract != "carol" {
		t.Errorf("snapshot entries = %+v", report.SnapshotABIEntries)
	}
}

// changeOneAction rewrites one block in storeB so a single action's data differs, and returns its block number.
func changeOneAction(t *testing.T, storeA, storeB string) uint32 {
	t.Helper()
	reader, err := corereader.Open(storeA, corereader.DefaultOpenConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for _, slice := range []string{"history_0000010001-0000020000", "history_0000000001-0000010000"} {
		dirB := filepath.Join(storeB, slice)
		s, err := openStoreSlice(dirB)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := LoadBlockIndex(filepath.Join(dirB, "blocks.index"))
		if err != nil {
			t.Fatal(err)
		}
		for i, entry := range idx.GetAllEntries() {
			raw, err := reader.GetRawBlockBatch(entry.BlockNum, entry.BlockNum)
			if err != nil || len(raw) != 1 || len(raw[0].Actions) == 0 {
				continue
			}
			for _, act := range raw[0].Actions {
				traces, _, err := reader.GetActionsByGlobalSeqs([]uint64{act.GlobalSeqUint64})
				if err != nil || len(traces) != 1 {
					continue
				}
				data, _ := hex.DecodeString(traces[0].Act.Data)
				if len(data) < 4 {
					continue
				}
				payload, err := s.payload(entry)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Count(payload, data) != 1 {
					continue
				}
				at := bytes.Index(payload, data) + len(data) - 1
				payload[at] ^= 0xff
				s.file.Close()
				rewriteRecord(t, dirB, i, payload)
				return entry.BlockNum
			}
		}
		s.file.Close()
	}
	t.Fatal("no block with uniquely located action data found in fixture")
	return 0
}

func rewriteRecord(t *testing.T, dir string, entryIdx int, payload []byte) {
	t.Helper()
	frame, err := compression.ZstdCompressLevel(nil, payload, 3)
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(dir, "data.log")
	f, err := os.OpenFile(dataPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	record := make([]byte, 4, 4+len(frame)+4)
	binary.LittleEndian.PutUint32(record, uint32(len(frame)))
	record = append(record, frame...)
	record = binary.LittleEndian.AppendUint32(record, crc32.ChecksumIEEE(frame))
	if _, err := f.Write(record); err != nil {
		t.Fatal(err)
	}
	f.Close()

	indexPath := filepath.Join(dir, "blocks.index")
	index, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	pos := 4 + entryIdx*32
	binary.LittleEndian.PutUint64(index[pos+4:pos+12], uint64(offset))
	binary.LittleEndian.PutUint32(index[pos+12:pos+16], uint32(len(frame)))
	if err := os.WriteFile(indexPath, index, 0644); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if err := os.WriteFile(filepath.Join(dir, "sha256.txt"), []byte(hex.EncodeToString(sum[:])), 0644); err != nil {
		t.Fatal(err)
	}
}
