package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	goeosio "github.com/greymass/go-eosio/pkg/chain"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/compression"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

const (
	dataLogHeaderSize = 32
	maxReportedIssues = 1000
	maxStaleSamples   = 20
)

const (
	DiffSliceMissing       = "SLICE_MISSING"
	DiffHeaderMismatch     = "HEADER_MISMATCH"
	DiffSlicesJSONMismatch = "SLICES_JSON_MISMATCH"
	DiffSHA256Mismatch     = "SHA256_MISMATCH"
	DiffBlockMissing       = "BLOCK_MISSING"
	DiffIndexMismatch      = "INDEX_MISMATCH"
	DiffFrameError         = "FRAME_ERROR"
	DiffPayloadMismatch    = "PAYLOAD_MISMATCH"
	DiffBlockMismatch      = "BLOCK_MISMATCH"
	DiffTrxIDsMismatch     = "TRX_IDS_MISMATCH"
	DiffTrxDataMismatch    = "TRX_DATA_MISMATCH"
	DiffActionMismatch     = "ACTION_MISMATCH"
	DiffReadError          = "READ_ERROR"
	DiffABIOrderMismatch   = "ABI_ORDER_MISMATCH"
	DiffABIEntryMissing    = "ABI_ENTRY_MISSING"
	DiffABIContentMismatch = "ABI_CONTENT_MISMATCH"
	DiffABIDataError       = "ABI_DATA_ERROR"
)

const (
	CaseBlocks              = "blocks"
	CaseOnblockOnlyBlocks   = "onblock_only_blocks"
	CaseTransactions        = "transactions"
	CaseMultiActionTrx      = "multi_action_transactions"
	CaseInlineActions       = "inline_actions"
	CaseNotifications       = "notifications"
	CaseSetabiActions       = "setabi_actions"
	CaseOnerrorActions      = "onerror_actions"
	CaseZeroAuthActions     = "zero_auth_actions"
	CaseEmptyDataActions    = "empty_data_actions"
	CaseSoftFailTrx         = "soft_fail_transactions"
	CaseHardFailTrx         = "hard_fail_transactions"
	CaseDelayedTrx          = "delayed_transactions"
	CaseDeferredTrx         = "deferred_transactions"
	CaseSigK1               = "signatures_k1"
	CaseSigR1               = "signatures_r1"
	CaseSigWebAuthn         = "signatures_webauthn"
	CaseABIEntries          = "abi_entries"
	CaseABIClears           = "abi_clears"
	CaseABIMultiEntryBlocks = "abi_multi_entry_blocks"
)

var allCases = []string{
	CaseBlocks, CaseOnblockOnlyBlocks, CaseTransactions, CaseMultiActionTrx,
	CaseInlineActions, CaseNotifications, CaseSetabiActions, CaseOnerrorActions,
	CaseZeroAuthActions, CaseEmptyDataActions, CaseSoftFailTrx, CaseHardFailTrx,
	CaseDelayedTrx, CaseDeferredTrx, CaseSigK1, CaseSigR1, CaseSigWebAuthn,
	CaseABIEntries, CaseABIClears, CaseABIMultiEntryBlocks,
}

var (
	setabiNameUint64  = chain.StringToName("setabi")
	onerrorNameUint64 = chain.StringToName("onerror")
)

type CompareConfig struct {
	StorePath        string
	ComparePath      string
	ABIPath          string
	CompareABIPath   string
	StartBlock       uint32
	EndBlock         uint32
	ABISnapshotBlock uint32
	Workers          int
	BatchSize        int
	OutputJSON       bool
	IgnoreElapsed    bool
	StaleEmptyData   bool
}

type ABIEntryInfo struct {
	BlockNum uint32 `json:"block_num"`
	Contract string `json:"contract"`
	Length   uint32 `json:"length"`
}

type CompareReport struct {
	Version            string            `json:"version"`
	Mode               string            `json:"mode"`
	StorePath          string            `json:"store_path"`
	ComparePath        string            `json:"compare_path"`
	ABIPath            string            `json:"abi_path,omitempty"`
	CompareABIPath     string            `json:"compare_abi_path,omitempty"`
	StartBlock         uint32            `json:"start_block"`
	EndBlock           uint32            `json:"end_block"`
	Timestamp          time.Time         `json:"timestamp"`
	Status             string            `json:"status"`
	SlicesCompared     int               `json:"slices_compared"`
	BlocksCompared     int               `json:"blocks_compared"`
	TrxCompared        int               `json:"transactions_compared"`
	ActionsCompared    int               `json:"actions_compared"`
	ABIEntriesCompared int               `json:"abi_entries_compared"`
	Differences        map[string]int    `json:"differences"`
	TotalDifferences   int               `json:"total_differences"`
	IgnoreElapsed      bool              `json:"ignore_elapsed,omitempty"`
	ElapsedDiffs       int               `json:"elapsed_differences,omitempty"`
	StaleEmptyData     bool              `json:"stale_empty_data,omitempty"`
	StaleEmptyActions  map[string]int    `json:"stale_empty_data_actions,omitempty"`
	StaleEmptyBlocks   int               `json:"stale_empty_data_blocks,omitempty"`
	StaleEmptySamples  []uint32          `json:"stale_empty_data_samples,omitempty"`
	StaleLayoutBlocks  int               `json:"stale_data_layout_blocks,omitempty"`
	OpenTailSlices     int               `json:"open_tail_slices,omitempty"`
	Cases              map[string]int    `json:"cases"`
	SnapshotABIEntries []ABIEntryInfo    `json:"snapshot_abi_entries,omitempty"`
	Issues             []ValidationIssue `json:"issues"`
	IssuesTruncated    bool              `json:"issues_truncated,omitempty"`
	DurationSeconds    float64           `json:"duration_seconds"`
}

type Comparer struct {
	cfg CompareConfig

	readerA corereader.Reader
	readerB corereader.Reader

	slicesJSONA map[uint32]json.RawMessage
	slicesJSONB map[uint32]json.RawMessage

	mu     sync.Mutex
	report *CompareReport
}

func NewComparer(cfg CompareConfig) *Comparer {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	return &Comparer{cfg: cfg}
}

func (c *Comparer) Compare() (*CompareReport, error) {
	startTime := time.Now()
	c.report = &CompareReport{
		Version:        "1.0.0",
		Mode:           "COMPARE",
		StorePath:      c.cfg.StorePath,
		ComparePath:    c.cfg.ComparePath,
		ABIPath:        c.cfg.ABIPath,
		CompareABIPath: c.cfg.CompareABIPath,
		Timestamp:      time.Now(),
		Differences:    make(map[string]int),
		Cases:          make(map[string]int),
		Issues:         make([]ValidationIssue, 0),
		IgnoreElapsed:  c.cfg.IgnoreElapsed,
		StaleEmptyData: c.cfg.StaleEmptyData,
	}
	if c.cfg.StaleEmptyData {
		c.report.StaleEmptyActions = make(map[string]int)
	}
	for _, name := range allCases {
		c.report.Cases[name] = 0
	}

	if (c.cfg.ABIPath == "") != (c.cfg.CompareABIPath == "") {
		return nil, fmt.Errorf("--abi-path and --compare-abi-path must be given together")
	}

	slicesA, err := listSliceDirs(c.cfg.StorePath)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	slicesB, err := listSliceDirs(c.cfg.ComparePath)
	if err != nil {
		return nil, fmt.Errorf("compare store: %w", err)
	}
	if len(slicesA) == 0 {
		return nil, fmt.Errorf("no slices found in %s", c.cfg.StorePath)
	}

	start, end := c.cfg.StartBlock, c.cfg.EndBlock
	if start == 0 {
		start = slicesA[0].start
	}
	if end == 0 {
		last := slicesA[len(slicesA)-1]
		lastBlock, _, _, err := (&Validator{}).findLastBlockInIndex(filepath.Join(c.cfg.StorePath, last.name, "blocks.index"))
		if err != nil {
			return nil, fmt.Errorf("cannot find last block of %s: %w", last.name, err)
		}
		end = lastBlock
	}
	if end < start {
		return nil, fmt.Errorf("end block %d is before start block %d", end, start)
	}
	c.report.StartBlock, c.report.EndBlock = start, end

	if c.slicesJSONA, err = loadSlicesJSON(c.cfg.StorePath); err != nil {
		return nil, fmt.Errorf("store slices.json: %w", err)
	}
	if c.slicesJSONB, err = loadSlicesJSON(c.cfg.ComparePath); err != nil {
		return nil, fmt.Errorf("compare store slices.json: %w", err)
	}

	cfg := corereader.DefaultOpenConfig()
	if c.readerA, err = corereader.Open(c.cfg.StorePath, cfg); err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	defer c.readerA.Close()
	if c.readerB, err = corereader.Open(c.cfg.ComparePath, cfg); err != nil {
		return nil, fmt.Errorf("open compare store: %w", err)
	}
	defer c.readerB.Close()

	if !c.cfg.OutputJSON {
		logger.Printf("info", "Compare: %s against %s, blocks %d-%d", c.cfg.StorePath, c.cfg.ComparePath, start, end)
	}

	for _, pair := range pairSlices(slicesA, slicesB, start, end) {
		c.compareSlice(pair, start, end)
	}

	if c.cfg.ABIPath != "" {
		if err := c.compareABIs(start, end); err != nil {
			return nil, err
		}
	}

	for _, n := range c.report.Differences {
		c.report.TotalDifferences += n
	}
	c.report.Status = "passed"
	if c.report.TotalDifferences > 0 {
		c.report.Status = "failed"
	}
	c.report.DurationSeconds = time.Since(startTime).Seconds()
	return c.report, nil
}

func (c *Comparer) diff(kind string, issue ValidationIssue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.report.Differences[kind]++
	if len(c.report.Issues) >= maxReportedIssues {
		c.report.IssuesTruncated = true
		return
	}
	issue.Severity = "ERROR"
	issue.Type = kind
	c.report.Issues = append(c.report.Issues, issue)
}

func (c *Comparer) addCases(counts map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range counts {
		c.report.Cases[k] += v
	}
}

type sliceDir struct {
	name       string
	start, max uint32
}

func listSliceDirs(root string) ([]sliceDir, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []sliceDir
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var s, m uint32
		if n, err := fmt.Sscanf(e.Name(), "history_%010d-%010d", &s, &m); err != nil || n != 2 {
			continue
		}
		out = append(out, sliceDir{name: e.Name(), start: s, max: m})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out, nil
}

type slicePair struct {
	name       string
	start, max uint32
	inA, inB   bool
}

func pairSlices(a, b []sliceDir, start, end uint32) []slicePair {
	byName := make(map[string]*slicePair)
	var order []string
	add := func(d sliceDir, isA bool) {
		if d.max < start || d.start > end {
			return
		}
		p, ok := byName[d.name]
		if !ok {
			p = &slicePair{name: d.name, start: d.start, max: d.max}
			byName[d.name] = p
			order = append(order, d.name)
		}
		if isA {
			p.inA = true
		} else {
			p.inB = true
		}
	}
	for _, d := range a {
		add(d, true)
	}
	for _, d := range b {
		add(d, false)
	}
	sort.Slice(order, func(i, j int) bool { return byName[order[i]].start < byName[order[j]].start })
	out := make([]slicePair, len(order))
	for i, name := range order {
		out[i] = *byName[name]
	}
	return out
}

func loadSlicesJSON(root string) (map[uint32]json.RawMessage, error) {
	data, err := os.ReadFile(filepath.Join(root, "slices.json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	out := make(map[uint32]json.RawMessage, len(raw))
	for _, r := range raw {
		var info SliceInfo
		if err := json.Unmarshal(r, &info); err != nil {
			return nil, err
		}
		out[info.SliceNum] = r
	}
	return out, nil
}

type storeSlice struct {
	dir     string
	file    *os.File
	entries map[uint32]BlockIndexEntry
}

func openStoreSlice(dir string) (*storeSlice, error) {
	idx, err := LoadBlockIndex(filepath.Join(dir, "blocks.index"))
	if err != nil {
		return nil, fmt.Errorf("blocks.index: %w", err)
	}
	f, err := os.Open(filepath.Join(dir, "data.log"))
	if err != nil {
		return nil, err
	}
	s := &storeSlice{dir: dir, file: f, entries: make(map[uint32]BlockIndexEntry, idx.Count())}
	for _, e := range idx.GetAllEntries() {
		s.entries[e.BlockNum] = e
	}
	return s, nil
}

func (s *storeSlice) header() ([]byte, error) {
	buf := make([]byte, dataLogHeaderSize)
	n, err := s.file.ReadAt(buf, 0)
	if n < dataLogHeaderSize {
		return buf[:n], err
	}
	return buf, nil
}

func (s *storeSlice) payload(e BlockIndexEntry) ([]byte, error) {
	frame := make([]byte, 4+int(e.Size)+4)
	if _, err := s.file.ReadAt(frame, int64(e.Offset)); err != nil {
		return nil, fmt.Errorf("read frame at offset %d: %w", e.Offset, err)
	}
	if size := binary.LittleEndian.Uint32(frame[0:4]); size != e.Size {
		return nil, fmt.Errorf("size prefix %d does not match index size %d", size, e.Size)
	}
	body := frame[4 : 4+e.Size]
	if want, got := binary.LittleEndian.Uint32(frame[4+e.Size:]), crc32.ChecksumIEEE(body); want != got {
		return nil, fmt.Errorf("CRC mismatch: trailer %08x, computed %08x", want, got)
	}
	if len(body) >= 4 && binary.LittleEndian.Uint32(body[0:4]) == 0xFD2FB528 {
		out, err := compression.ZstdDecompress(nil, body)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		return out, nil
	}
	return body, nil
}

func verifySHA256(dir string) (bool, string, error) {
	want, err := os.ReadFile(filepath.Join(dir, "sha256.txt"))
	if os.IsNotExist(err) {
		return true, "", nil
	}
	if err != nil {
		return false, "", err
	}
	data, err := os.ReadFile(filepath.Join(dir, "data.log"))
	if err != nil {
		return false, "", err
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	return strings.TrimSpace(string(want)) == got, got, nil
}

func (c *Comparer) compareSlice(p slicePair, start, end uint32) {
	sliceNum := (p.start - 1) / 10000
	if !p.inA || !p.inB {
		store := "compare"
		if !p.inA {
			store = "store"
		}
		c.diff(DiffSliceMissing, ValidationIssue{SliceNum: sliceNum, File: p.name, Message: fmt.Sprintf("slice %s is missing from the %s", p.name, store)})
		return
	}

	a, err := openStoreSlice(filepath.Join(c.cfg.StorePath, p.name))
	if err != nil {
		c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, File: p.name, Message: "store: " + err.Error()})
		return
	}
	defer a.file.Close()
	b, err := openStoreSlice(filepath.Join(c.cfg.ComparePath, p.name))
	if err != nil {
		c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, File: p.name, Message: "compare: " + err.Error()})
		return
	}
	defer b.file.Close()

	c.mu.Lock()
	c.report.SlicesCompared++
	c.mu.Unlock()

	openTail := false
	ha, errA := a.header()
	hb, errB := b.header()
	if errA == nil && errB == nil && !bytes.Equal(ha, hb) && openTailHeader(ha, hb, p.start, end) {
		openTail = true
	} else if errA != nil || errB != nil || !bytes.Equal(ha, hb) {
		c.diff(DiffHeaderMismatch, ValidationIssue{SliceNum: sliceNum, File: "data.log", Message: fmt.Sprintf("header store=%x compare=%x", ha, hb)})
	}

	if c.slicesJSONA != nil || c.slicesJSONB != nil {
		ja, okA := c.slicesJSONA[sliceNum]
		jb, okB := c.slicesJSONB[sliceNum]
		var ia, ib SliceInfo
		if okA {
			json.Unmarshal(ja, &ia)
		}
		if okB {
			json.Unmarshal(jb, &ib)
		}
		if okA && okB && ia != ib && openTailInfo(ia, ib, end) {
			openTail = true
		} else if okA != okB || ia != ib {
			c.diff(DiffSlicesJSONMismatch, ValidationIssue{SliceNum: sliceNum, File: "slices.json", Message: fmt.Sprintf("store=%s compare=%s", ja, jb)})
		}
	}

	if openTail {
		c.mu.Lock()
		c.report.OpenTailSlices++
		c.mu.Unlock()
	}

	for _, s := range []struct {
		label, dir string
	}{{"store", a.dir}, {"compare", b.dir}} {
		ok, got, err := verifySHA256(s.dir)
		if err != nil || !ok {
			msg := fmt.Sprintf("%s sha256.txt does not match data.log (computed %s)", s.label, got)
			if err != nil {
				msg = fmt.Sprintf("%s sha256 check failed: %v", s.label, err)
			}
			c.diff(DiffSHA256Mismatch, ValidationIssue{SliceNum: sliceNum, File: "sha256.txt", Message: msg})
		}
	}

	lo, hi := max(p.start, start), min(p.max, end)
	var common []uint32
	for b64 := uint64(lo); b64 <= uint64(hi); b64++ {
		bn := uint32(b64)
		ea, okA := a.entries[bn]
		eb, okB := b.entries[bn]
		switch {
		case !okA && !okB:
			continue
		case okA != okB:
			store := "compare"
			if !okA {
				store = "store"
			}
			c.diff(DiffBlockMissing, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "blocks.index", Message: fmt.Sprintf("block %d is missing from the %s", bn, store)})
			continue
		}
		if ea.GlobMin != eb.GlobMin || ea.GlobMax != eb.GlobMax {
			c.diff(DiffIndexMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "blocks.index", Message: fmt.Sprintf("glob range store=%d-%d compare=%d-%d", ea.GlobMin, ea.GlobMax, eb.GlobMin, eb.GlobMax)})
		}
		common = append(common, bn)
	}

	work := make(chan []uint32)
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for run := range work {
				c.compareRun(sliceNum, a, b, run)
			}
		}()
	}
	for _, run := range splitRuns(common, c.cfg.BatchSize) {
		work <- run
	}
	close(work)
	wg.Wait()
}

func splitRuns(blocks []uint32, batch int) [][]uint32 {
	var out [][]uint32
	for i := 0; i < len(blocks); {
		j := i + 1
		for j < len(blocks) && j-i < batch && blocks[j] == blocks[j-1]+1 {
			j++
		}
		out = append(out, blocks[i:j])
		i = j
	}
	return out
}

// openTailHeader accepts a compare slice still open at the range end against the same slice finalized in the store.
func openTailHeader(ha, hb []byte, start, end uint32) bool {
	if len(ha) != len(hb) || len(ha) < 21 || ha[20] != 1 || hb[20] != 0 {
		return false
	}
	if !bytes.Equal(ha[:20], hb[:20]) || !bytes.Equal(ha[21:], hb[21:]) {
		return false
	}
	per := binary.LittleEndian.Uint32(ha[8:12])
	return end >= start && end < start+per-1
}

func openTailInfo(ia, ib SliceInfo, end uint32) bool {
	if !ia.Finalized || ib.Finalized || ib.EndBlock < end || ib.EndBlock >= ia.EndBlock || ib.GlobMax >= ia.GlobMax {
		return false
	}
	ib.EndBlock, ib.Finalized, ib.GlobMax = ia.EndBlock, true, ia.GlobMax
	return ia == ib
}

func (c *Comparer) compareRun(sliceNum uint32, a, b *storeSlice, run []uint32) {
	type payloadPair struct {
		a, b       []byte
		errA, errB error
	}
	payloads := make(map[uint32]payloadPair, len(run))
	for _, bn := range run {
		var p payloadPair
		p.a, p.errA = a.payload(a.entries[bn])
		p.b, p.errB = b.payload(b.entries[bn])
		payloads[bn] = p
	}
	stale := make(map[uint32]bool)
	defer func() {
		for _, bn := range run {
			p := payloads[bn]
			c.comparePayload(sliceNum, bn, p.a, p.b, p.errA, p.errB, stale[bn])
		}
	}()

	first, last := run[0], run[len(run)-1]
	rawA, errA := c.readerA.GetRawBlockBatch(first, last)
	rawB, errB := c.readerB.GetRawBlockBatch(first, last)
	if errA != nil || errB != nil {
		c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: first, Message: fmt.Sprintf("GetRawBlockBatch %d-%d: store=%v compare=%v", first, last, errA, errB)})
		return
	}
	byNumB := make(map[uint32]*corereader.RawBlock, len(rawB))
	for i := range rawB {
		byNumB[rawB[i].BlockNum] = &rawB[i]
	}

	for i := range rawA {
		ra := &rawA[i]
		rb, ok := byNumB[ra.BlockNum]
		if !ok {
			c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: ra.BlockNum, Message: "block not returned by the compare store"})
			continue
		}
		p := payloads[ra.BlockNum]
		if c.compareBlock(sliceNum, ra, rb, p.a, p.b) {
			stale[ra.BlockNum] = true
		}
	}
	c.mu.Lock()
	c.report.BlocksCompared += len(rawA)
	c.mu.Unlock()
}

func (c *Comparer) comparePayload(sliceNum, bn uint32, pa, pb []byte, errA, errB error, stale bool) {
	if errA != nil {
		c.diff(DiffFrameError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "data.log", Message: "store: " + errA.Error()})
	}
	if errB != nil {
		c.diff(DiffFrameError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "data.log", Message: "compare: " + errB.Error()})
	}
	switch {
	case errA != nil || errB != nil:
	case stale:
		c.mu.Lock()
		c.report.StaleEmptyBlocks++
		if len(c.report.StaleEmptySamples) < maxStaleSamples {
			c.report.StaleEmptySamples = append(c.report.StaleEmptySamples, bn)
		}
		c.mu.Unlock()
	case c.cfg.IgnoreElapsed:
		c.comparePayloadIgnoringElapsed(sliceNum, bn, pa, pb)
	case !bytes.Equal(pa, pb) && !c.layoutOnly(pa, pb, false):
		c.diff(DiffPayloadMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "data.log", Message: fmt.Sprintf("decompressed payload differs (store %d bytes, compare %d bytes, first difference at byte %d)", len(pa), len(pb), firstDifference(pa, pb))})
	}
}

func (c *Comparer) layoutOnly(pa, pb []byte, ignoreElapsed bool) bool {
	if !c.cfg.StaleEmptyData {
		return false
	}
	same, err := corereader.PayloadsHaveSameContent(pa, pb, ignoreElapsed)
	if err != nil || !same {
		return false
	}
	c.mu.Lock()
	c.report.StaleLayoutBlocks++
	c.mu.Unlock()
	return true
}

func (c *Comparer) staleEmptyData(sliceNum uint32, ra, rb *corereader.RawBlock, pa, pb []byte) (actsA, actsB []corereader.CanonicalAction, stale bool) {
	actsA, actsB = ra.Actions, rb.Actions
	if !c.cfg.StaleEmptyData || len(actsA) != len(actsB) {
		return actsA, actsB, false
	}
	if !ra.HasActionData() && pa != nil {
		d := corereader.ParseRawBlockWithData(pa, ra.BlockNum)
		ra = &d
	}
	if !rb.HasActionData() && pb != nil {
		d := corereader.ParseRawBlockWithData(pb, rb.BlockNum)
		rb = &d
	}
	if !ra.HasActionData() || !rb.HasActionData() {
		c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: ra.BlockNum, Message: "raw block has no action data"})
		return actsA, actsB, false
	}
	actsA = append([]corereader.CanonicalAction(nil), actsA...)
	actsB = append([]corereader.CanonicalAction(nil), actsB...)
	for i := range actsA {
		da, db := ra.GetActionData(actsA[i].DataIndex), rb.GetActionData(actsB[i].DataIndex)
		actsA[i].DataIndex, actsB[i].DataIndex = 0, 0
		switch {
		case bytes.Equal(da, db):
		case len(db) == 0:
			stale = true
		default:
			c.diff(DiffBlockMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: ra.BlockNum, Message: fmt.Sprintf("action %d data differs: store %d bytes, compare %d bytes", i, len(da), len(db))})
		}
	}
	return actsA, actsB, stale
}

func (c *Comparer) compareBlock(sliceNum uint32, ra, rb *corereader.RawBlock, pa, pb []byte) bool {
	bn := ra.BlockNum
	counts := map[string]int{CaseBlocks: 1}
	actsA, actsB, stale := c.staleEmptyData(sliceNum, ra, rb, pa, pb)

	for _, f := range []struct {
		name string
		a, b any
	}{
		{"block_time", ra.BlockTime, rb.BlockTime},
		{"actions", actsA, actsB},
		{"action_meta", ra.ActionMeta, rb.ActionMeta},
		{"notifications", ra.Notifications, rb.Notifications},
		{"names_in_block", ra.NamesInBlock, rb.NamesInBlock},
	} {
		if !reflect.DeepEqual(f.a, f.b) {
			c.diff(DiffBlockMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("%s differs: store=%s compare=%s", f.name, summarize(f.a), summarize(f.b))})
		}
	}

	idsA, errA := c.readerA.GetTransactionIDsRaw(bn)
	idsB, errB := c.readerB.GetTransactionIDsRaw(bn)
	switch {
	case errA != nil || errB != nil:
		c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("GetTransactionIDsRaw: store=%v compare=%v", errA, errB)})
	case !reflect.DeepEqual(idsA, idsB):
		c.diff(DiffTrxIDsMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("transaction IDs differ: store=%d compare=%d", len(idsA), len(idsB))})
	}

	onblockOnly := len(idsA) == 1 && len(ra.Actions) > 0
	for _, act := range ra.Actions {
		if act.ContractUint64 != eosioNameUint64 || act.ActionUint64 != onblockNameUint64 {
			onblockOnly = false
			break
		}
	}
	if onblockOnly {
		counts[CaseOnblockOnlyBlocks]++
	}

	for _, id := range idsA {
		trxID := hex.EncodeToString(id[:])
		ta, errA := c.readerA.GetTransactionData(bn, trxID)
		tb, errB := c.readerB.GetTransactionData(bn, trxID)
		if errA != nil || errB != nil {
			c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("GetTransactionData %s: store=%v compare=%v", trxID, errA, errB)})
			continue
		}
		if c.cfg.IgnoreElapsed {
			ta, tb = withoutElapsed(ta), withoutElapsed(tb)
		}
		if c.cfg.StaleEmptyData {
			ta = withoutStaleData(ta, tb)
		}
		if !reflect.DeepEqual(ta, tb) {
			c.diff(DiffTrxDataMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("transaction %s differs", trxID)})
		}
		countTransaction(ta, counts)
	}

	seqs := make([]uint64, len(ra.Actions))
	for i, act := range ra.Actions {
		seqs[i] = act.GlobalSeqUint64
	}
	if len(seqs) > 0 {
		actsA, _, errA := c.readerA.GetActionsByGlobalSeqs(seqs)
		actsB, _, errB := c.readerB.GetActionsByGlobalSeqs(seqs)
		if errA != nil || errB != nil {
			c.diff(DiffReadError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("GetActionsByGlobalSeqs: store=%v compare=%v", errA, errB)})
		} else {
			c.compareActions(sliceNum, bn, actsA, actsB)
			for i := range actsA {
				countAction(&actsA[i], counts)
			}
		}
	}

	c.mu.Lock()
	c.report.TrxCompared += len(idsA)
	c.report.ActionsCompared += len(seqs)
	c.mu.Unlock()
	c.addCases(counts)
	return stale
}

func withoutStaleData(a, b *corereader.TransactionData) *corereader.TransactionData {
	if len(a.Actions) != len(b.Actions) {
		return a
	}
	out := *a
	out.Actions = append([]chain.ActionTrace(nil), a.Actions...)
	for i := range out.Actions {
		if b.Actions[i].Act.Data == "" && out.Actions[i].Act.Data != "" {
			out.Actions[i].Act.Data = ""
			out.Actions[i].Receipt.ActDigest = b.Actions[i].Receipt.ActDigest
		}
	}
	return &out
}

func withoutElapsed(t *corereader.TransactionData) *corereader.TransactionData {
	out := *t
	out.Actions = append([]chain.ActionTrace(nil), t.Actions...)
	for i := range out.Actions {
		out.Actions[i].Elapsed = 0
	}
	return &out
}

func (c *Comparer) comparePayloadIgnoringElapsed(sliceNum, bn uint32, pa, pb []byte) {
	na, ea, errA := corereader.PayloadWithoutElapsed(pa)
	nb, eb, errB := corereader.PayloadWithoutElapsed(pb)
	if errA != nil || errB != nil {
		c.diff(DiffFrameError, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "data.log", Message: fmt.Sprintf("payload parse: store=%v compare=%v", errA, errB)})
		return
	}
	if !bytes.Equal(na, nb) {
		if c.layoutOnly(pa, pb, true) {
			return
		}
		c.diff(DiffPayloadMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, File: "data.log", Message: fmt.Sprintf("decompressed payload without elapsed differs (store %d bytes, compare %d bytes, first difference at byte %d)", len(na), len(nb), firstDifference(na, nb))})
		return
	}
	n := 0
	for i := range ea {
		if ea[i] != eb[i] {
			n++
		}
	}
	if n > 0 {
		c.mu.Lock()
		c.report.ElapsedDiffs += n
		c.mu.Unlock()
	}
}

func (c *Comparer) compareActions(sliceNum, bn uint32, actsA, actsB []chain.ActionTrace) {
	if len(actsA) != len(actsB) {
		c.diff(DiffActionMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("action count store=%d compare=%d", len(actsA), len(actsB))})
		return
	}
	for i := range actsA {
		if c.cfg.IgnoreElapsed || c.cfg.StaleEmptyData {
			a, b := actsA[i], actsB[i]
			if c.cfg.IgnoreElapsed {
				a.Elapsed, b.Elapsed = 0, 0
			}
			isStale := c.cfg.StaleEmptyData && b.Act.Data == "" && a.Act.Data != ""
			if isStale {
				a.Act.Data, a.Receipt.ActDigest = "", b.Receipt.ActDigest
			}
			if reflect.DeepEqual(a, b) {
				if isStale {
					c.mu.Lock()
					c.report.StaleEmptyActions[a.Act.Account+"::"+a.Act.Name]++
					c.mu.Unlock()
				}
				continue
			}
		}
		if !reflect.DeepEqual(actsA[i], actsB[i]) {
			ja, _ := json.Marshal(actsA[i])
			jb, _ := json.Marshal(actsB[i])
			c.diff(DiffActionMismatch, ValidationIssue{SliceNum: sliceNum, BlockNum: bn, Message: fmt.Sprintf("action global_seq %d differs: store=%s compare=%s", actsA[i].GlobalSeqUint64, truncate(string(ja)), truncate(string(jb)))})
		}
	}
}

func countTransaction(t *corereader.TransactionData, counts map[string]int) {
	counts[CaseTransactions]++
	if len(t.Actions) > 1 {
		counts[CaseMultiActionTrx]++
	}
	switch t.Status {
	case 1:
		counts[CaseSoftFailTrx]++
	case 2:
		counts[CaseHardFailTrx]++
	case 3:
		counts[CaseDelayedTrx]++
	}
	isOnblock := len(t.Actions) > 0 && t.Actions[0].Act.Account == "eosio" && t.Actions[0].Act.Name == "onblock"
	if len(t.Signatures) == 0 && !isOnblock {
		counts[CaseDeferredTrx]++
	}
	for _, sig := range t.Signatures {
		if len(sig) == 0 {
			continue
		}
		switch sig[0] {
		case 0:
			counts[CaseSigK1]++
		case 1:
			counts[CaseSigR1]++
		case 2:
			counts[CaseSigWebAuthn]++
		}
	}
}

func countAction(a *chain.ActionTrace, counts map[string]int) {
	if a.Receiver != a.Act.Account {
		counts[CaseNotifications]++
	} else if a.CreatorAO != 0 {
		counts[CaseInlineActions]++
	}
	if a.Act.Account == "eosio" {
		switch chain.StringToName(a.Act.Name) {
		case setabiNameUint64:
			if a.Receiver == "eosio" {
				counts[CaseSetabiActions]++
			}
		case onerrorNameUint64:
			counts[CaseOnerrorActions]++
		}
	}
	if len(a.Act.Authorization) == 0 {
		counts[CaseZeroAuthActions]++
	}
	if a.Act.Data == "" {
		counts[CaseEmptyDataActions]++
	}
}

type abiEntry struct {
	blockNum uint32
	contract uint64
	offset   uint64
	content  []byte
	err      error
}

func loadABIEntries(root string, start, end uint32) ([]abiEntry, error) {
	index, err := os.ReadFile(filepath.Join(root, "abis.index"))
	if err != nil {
		return nil, err
	}
	if len(index)%20 != 0 {
		return nil, fmt.Errorf("abis.index size %d is not a multiple of 20", len(index))
	}
	dat, err := os.Open(filepath.Join(root, "abis.dat"))
	if err != nil {
		return nil, err
	}
	defer dat.Close()

	var out []abiEntry
	for off := 0; off < len(index); off += 20 {
		e := abiEntry{
			blockNum: binary.LittleEndian.Uint32(index[off : off+4]),
			contract: binary.LittleEndian.Uint64(index[off+4 : off+12]),
			offset:   binary.LittleEndian.Uint64(index[off+12 : off+20]),
		}
		if e.blockNum < start || e.blockNum > end {
			continue
		}
		header := make([]byte, 16)
		if _, err := dat.ReadAt(header, int64(e.offset)); err != nil {
			e.err = fmt.Errorf("read header at %d: %w", e.offset, err)
		} else if bn, ct := binary.LittleEndian.Uint32(header[0:4]), binary.LittleEndian.Uint64(header[4:12]); bn != e.blockNum || ct != e.contract {
			e.err = fmt.Errorf("abis.dat header at %d names block %d contract %s", e.offset, bn, chain.NameToString(ct))
		} else if n := binary.LittleEndian.Uint32(header[12:16]); n > 0 {
			e.content = make([]byte, n)
			if _, err := dat.ReadAt(e.content, int64(e.offset)+16); err != nil {
				e.err = fmt.Errorf("read content at %d: %w", e.offset, err)
			}
		}
		out = append(out, e)
	}
	return out, nil
}

func canonicalABI(content []byte) []byte {
	if len(content) == 0 {
		return nil
	}
	var abi goeosio.Abi
	if err := json.Unmarshal(content, &abi); err != nil {
		return content
	}
	out, err := json.Marshal(&abi)
	if err != nil {
		return content
	}
	return out
}

func groupByBlock(entries []abiEntry) ([]uint32, map[uint32][]abiEntry) {
	groups := make(map[uint32][]abiEntry)
	var blocks []uint32
	for _, e := range entries {
		if _, ok := groups[e.blockNum]; !ok {
			blocks = append(blocks, e.blockNum)
		}
		groups[e.blockNum] = append(groups[e.blockNum], e)
	}
	return blocks, groups
}

func (c *Comparer) compareABIs(start, end uint32) error {
	entriesA, err := loadABIEntries(c.cfg.ABIPath, start, end)
	if err != nil {
		return fmt.Errorf("store ABI cache: %w", err)
	}
	entriesB, err := loadABIEntries(c.cfg.CompareABIPath, start, end)
	if err != nil {
		return fmt.Errorf("compare ABI cache: %w", err)
	}

	if s := c.cfg.ABISnapshotBlock; s != 0 {
		kept := entriesB[:0:0]
		for _, e := range entriesB {
			if e.blockNum == s {
				c.report.SnapshotABIEntries = append(c.report.SnapshotABIEntries, ABIEntryInfo{BlockNum: e.blockNum, Contract: chain.NameToString(e.contract), Length: uint32(len(e.content))})
				continue
			}
			kept = append(kept, e)
		}
		entriesB = kept
	}

	for _, e := range entriesA {
		c.report.Cases[CaseABIEntries]++
		if len(e.content) == 0 && e.err == nil {
			c.report.Cases[CaseABIClears]++
		}
	}
	for _, s := range []struct {
		label   string
		entries []abiEntry
	}{{"store", entriesA}, {"compare", entriesB}} {
		for _, e := range s.entries {
			if e.err != nil {
				c.diff(DiffABIDataError, ValidationIssue{BlockNum: e.blockNum, File: "abis.dat", Message: fmt.Sprintf("%s %s: %v", s.label, chain.NameToString(e.contract), e.err)})
			}
		}
	}

	blocksA, groupsA := groupByBlock(entriesA)
	blocksB, groupsB := groupByBlock(entriesB)
	seen := make(map[uint32]bool)
	var blocks []uint32
	for _, bn := range append(blocksA, blocksB...) {
		if !seen[bn] {
			seen[bn] = true
			blocks = append(blocks, bn)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	for _, bn := range blocks {
		ga, gb := groupsA[bn], groupsB[bn]
		if len(ga) > 1 {
			c.report.Cases[CaseABIMultiEntryBlocks]++
		}
		c.compareABIBlock(bn, ga, gb)
	}
	return nil
}

func abiKey(e abiEntry) string {
	return fmt.Sprintf("%s/%t", chain.NameToString(e.contract), len(e.content) == 0)
}

func (c *Comparer) compareABIBlock(bn uint32, ga, gb []abiEntry) {
	keysA := make([]string, len(ga))
	keysB := make([]string, len(gb))
	for i, e := range ga {
		keysA[i] = abiKey(e)
	}
	for i, e := range gb {
		keysB[i] = abiKey(e)
	}

	if !reflect.DeepEqual(keysA, keysB) {
		remaining := make(map[string]int)
		for _, k := range keysB {
			remaining[k]++
		}
		missingFromB := 0
		for _, k := range keysA {
			if remaining[k] > 0 {
				remaining[k]--
				continue
			}
			missingFromB++
			c.diff(DiffABIEntryMissing, ValidationIssue{BlockNum: bn, File: "abis.index", Message: fmt.Sprintf("entry %s (contract/clear) is missing from the compare ABI cache", k)})
		}
		extraInB := 0
		for k, n := range remaining {
			for ; n > 0; n-- {
				extraInB++
				c.diff(DiffABIEntryMissing, ValidationIssue{BlockNum: bn, File: "abis.index", Message: fmt.Sprintf("entry %s (contract/clear) is missing from the store ABI cache", k)})
			}
		}
		if missingFromB == 0 && extraInB == 0 {
			c.diff(DiffABIOrderMismatch, ValidationIssue{BlockNum: bn, File: "abis.index", Message: fmt.Sprintf("entry order store=%v compare=%v", keysA, keysB)})
		}
		return
	}

	for i := range ga {
		c.report.ABIEntriesCompared++
		if ga[i].err != nil || gb[i].err != nil {
			continue
		}
		if !bytes.Equal(canonicalABI(ga[i].content), canonicalABI(gb[i].content)) {
			c.diff(DiffABIContentMismatch, ValidationIssue{BlockNum: bn, File: "abis.dat", Message: fmt.Sprintf("ABI for %s differs after canonical parsing", chain.NameToString(ga[i].contract))})
		}
	}
}

func firstDifference(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func summarize(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return truncate(string(data))
}

func truncate(s string) string {
	const limit = 300
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

func (c *Comparer) PrintReport(report *CompareReport) {
	if c.cfg.OutputJSON {
		data, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(data))
		return
	}

	if report.Status == "passed" {
		logger.Printf("result", "COMPARISON PASSED")
	} else {
		logger.Printf("result", "COMPARISON FAILED - %d differences", report.TotalDifferences)
	}
	for _, issue := range report.Issues {
		logger.Printf("error", "%s block %d - %s", issue.Type, issue.BlockNum, issue.Message)
	}
	if report.IssuesTruncated {
		logger.Printf("warning", "only the first %d differences are listed", maxReportedIssues)
	}

	var kinds []string
	for k := range report.Differences {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		logger.Printf("summary", "Difference %s: %d", k, report.Differences[k])
	}
	logger.Printf("summary", "Blocks %d-%d: %d slices, %d blocks, %d transactions, %d actions, %d ABI entries compared",
		report.StartBlock, report.EndBlock, report.SlicesCompared, report.BlocksCompared, report.TrxCompared, report.ActionsCompared, report.ABIEntriesCompared)
	if report.IgnoreElapsed {
		logger.Printf("summary", "Elapsed excluded: %d action records differ only in elapsed", report.ElapsedDiffs)
	}
	if report.StaleEmptyData {
		logger.Printf("summary", "Stale empty data accepted: %d blocks, actions by name %v", report.StaleEmptyBlocks, report.StaleEmptyActions)
	}
	for _, k := range allCases {
		logger.Printf("summary", "Case %s: %d", k, report.Cases[k])
	}
	for _, e := range report.SnapshotABIEntries {
		logger.Printf("summary", "Snapshot ABI entry at block %d for %s (%d bytes): check against get_abi", e.BlockNum, e.Contract, e.Length)
	}
	logger.Printf("summary", "Duration: %.2fs", report.DurationSeconds)
}
