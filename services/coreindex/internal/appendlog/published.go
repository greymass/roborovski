package appendlog

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/greymass/roborovski/libraries/coremanifest"
)

// CoreBounds is the committed extent of core storage: the head block and the slice that holds it.
type CoreBounds struct {
	Head          uint32
	SliceDataEnd  uint64
	SliceIndexLen uint32
}

// CommitState is what a commit publishes for core storage.
type CommitState struct {
	CoreBounds
	HeadData []byte // decompressed head block
}

type sliceDir struct {
	num        uint32
	start, end uint32
	path       string
}

func listSliceDirs(basePath string, span uint32) ([]sliceDir, error) {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var dirs []sliceDir
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !strings.HasPrefix(name, "history_") {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(name, "history_"), "-", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("unexpected slice directory %s", name)
		}
		start, err1 := strconv.ParseUint(parts[0], 10, 32)
		end, err2 := strconv.ParseUint(parts[1], 10, 32)
		if err1 != nil || err2 != nil || start == 0 || end < start {
			return nil, fmt.Errorf("unexpected slice directory %s", name)
		}
		if uint32(end-start+1) != span || (start-1)%uint64(span) != 0 {
			return nil, fmt.Errorf("slice directory %s does not match the %d-block slice span", name, span)
		}
		dirs = append(dirs, sliceDir{num: uint32((start - 1) / uint64(span)), start: uint32(start), end: uint32(end), path: filepath.Join(basePath, name)})
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].num < dirs[j].num })
	return dirs, nil
}

func syncPath(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// SyncForCommit makes every core file written since the last commit durable and returns the bounds to publish.
func (s *SliceStore) SyncForCommit() (CommitState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sm := s.sliceManager
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if active := sm.activeSlice; active != nil {
		active.mu.Lock()
		var err error
		if active.blockLog != nil {
			err = active.blockLog.Sync()
		}
		if err == nil {
			err = active.saveIndicesWithSync(true)
		}
		active.mu.Unlock()
		if err != nil {
			return CommitState{}, fmt.Errorf("sync active slice %d: %w", active.SliceNum, err)
		}
	}
	for dir := range sm.unsynced {
		for _, name := range []string{"data.log", "blocks.index"} {
			if err := syncPath(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return CommitState{}, fmt.Errorf("sync %s: %w", filepath.Join(dir, name), err)
			}
		}
		if err := coremanifest.SyncDir(dir); err != nil {
			return CommitState{}, err
		}
	}
	if sm.rootChanged {
		if err := coremanifest.SyncDir(sm.basePath); err != nil {
			return CommitState{}, err
		}
	}
	sm.unsynced = make(map[string]bool)
	sm.rootChanged = false

	var state CommitState
	if s.head == 0 {
		return state, nil
	}
	var entries []BlockIndexEntry
	headSlice := sm.BlockToSliceNum(s.head)
	if sm.activeSlice != nil && sm.activeSlice.SliceNum == headSlice {
		entries = sm.activeSlice.blockIndex.GetAllEntries()
	} else {
		idx, err := LoadBlockIndex(filepath.Join(sm.slicePath(headSlice), "blocks.index"))
		if err != nil {
			return CommitState{}, fmt.Errorf("load index of head slice %d: %w", headSlice, err)
		}
		entries = idx.GetAllEntries()
	}
	if len(entries) == 0 || entries[len(entries)-1].BlockNum != s.head {
		return CommitState{}, fmt.Errorf("head slice %d does not end at head block %d", headSlice, s.head)
	}
	last := entries[len(entries)-1]
	state.Head = s.head
	state.SliceIndexLen = uint32(len(entries))
	state.SliceDataEnd = last.Offset + 8 + uint64(last.Size)
	if s.headData == nil {
		reader, err := NewReaderWithHeader(filepath.Join(sm.slicePath(headSlice), "data.log"))
		if err != nil {
			return CommitState{}, err
		}
		raw, err := reader.ReadAt(last.Offset)
		reader.Close()
		if err != nil {
			return CommitState{}, fmt.Errorf("read head block %d: %w", s.head, err)
		}
		s.headData = raw
	}
	data, err := decompressData(s.headData)
	if err != nil {
		return CommitState{}, err
	}
	state.HeadData = data
	return state, nil
}

// SetCommittedHead positions the store at the committed head and limits readers to published blocks.
func (s *SliceStore) SetCommittedHead(head uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gated = true
	s.published = head
	if s.lib != head || s.head != head {
		s.lib = head
		s.head = head
		s.headData = nil
	}
	return nil
}

// MarkPublished lets readers see blocks up to head once its manifest is durable.
func (s *SliceStore) MarkPublished(head uint32) {
	s.mu.Lock()
	s.published = head
	s.mu.Unlock()
}

// TruncateReport lists what recovery removed beyond the committed boundary.
type TruncateReport struct {
	RemovedSlices   []string
	DataBytesCut    int64
	IndexEntriesCut int64
	FinalizedReset  bool
}

func (r TruncateReport) Changed() bool {
	return len(r.RemovedSlices) > 0 || r.DataBytesCut > 0 || r.IndexEntriesCut > 0 || r.FinalizedReset
}

// TruncateToCommitted removes everything beyond b, and changes nothing when the store holds less than b.
func TruncateToCommitted(basePath string, span uint32, b CoreBounds) (TruncateReport, error) {
	var report TruncateReport
	dirs, err := listSliceDirs(basePath, span)
	if err != nil {
		return report, err
	}
	headSlice := uint32(0)
	if b.Head > 0 {
		headSlice = (b.Head - 1) / span
	}

	var head *sliceDir
	for i := range dirs {
		if b.Head > 0 && dirs[i].num == headSlice {
			head = &dirs[i]
		}
	}
	if b.Head > 0 {
		if head == nil {
			return report, fmt.Errorf("committed head block %d: slice %d is missing", b.Head, headSlice)
		}
		if err := checkCommittedSlice(head.path, b); err != nil {
			return report, err
		}
	}

	for _, d := range dirs {
		if b.Head == 0 || d.num > headSlice {
			if err := os.RemoveAll(d.path); err != nil {
				return report, err
			}
			report.RemovedSlices = append(report.RemovedSlices, filepath.Base(d.path))
		}
	}

	var lastEntry BlockIndexEntry
	finalized := false
	if head != nil {
		dataPath := filepath.Join(head.path, "data.log")
		indexPath := filepath.Join(head.path, "blocks.index")
		st, err := os.Stat(dataPath)
		if err != nil {
			return report, err
		}
		if st.Size() > int64(b.SliceDataEnd) {
			report.DataBytesCut = st.Size() - int64(b.SliceDataEnd)
			if err := truncateAndSync(dataPath, int64(b.SliceDataEnd)); err != nil {
				return report, err
			}
		}
		ist, err := os.Stat(indexPath)
		if err != nil {
			return report, err
		}
		onDisk, err := readIndexCount(indexPath)
		if err != nil {
			return report, err
		}
		if ist.Size() != 4+int64(b.SliceIndexLen)*32 || onDisk != b.SliceIndexLen {
			if onDisk > b.SliceIndexLen {
				report.IndexEntriesCut = int64(onDisk - b.SliceIndexLen)
			}
			if err := TruncateIndexFile(indexPath, b.SliceIndexLen); err != nil {
				return report, err
			}
		}
		entry, err := readIndexEntry(indexPath, b.SliceIndexLen-1)
		if err != nil {
			return report, err
		}
		lastEntry = entry
		hdr, err := ReadDataLogHeader(dataPath)
		if err != nil {
			return report, err
		}
		finalized = hdr.Finalized == 1
		if finalized && b.Head < hdr.EndBlock {
			if err := UpdateFinalizedFlag(dataPath, false); err != nil {
				return report, err
			}
			os.Remove(filepath.Join(head.path, "sha256.txt"))
			finalized = false
			report.FinalizedReset = true
		}
		if report.Changed() {
			if err := coremanifest.SyncDir(head.path); err != nil {
				return report, err
			}
		}
	}
	if len(report.RemovedSlices) > 0 {
		if err := coremanifest.SyncDir(basePath); err != nil {
			return report, err
		}
	}
	if err := rewriteSliceInfos(basePath, span, b.Head, headSlice, lastEntry, finalized); err != nil {
		return report, err
	}
	return report, nil
}

func checkCommittedSlice(dir string, b CoreBounds) error {
	if b.SliceIndexLen == 0 {
		return fmt.Errorf("committed head block %d: manifest names an empty index", b.Head)
	}
	indexPath := filepath.Join(dir, "blocks.index")
	entry, err := readIndexEntry(indexPath, b.SliceIndexLen-1)
	if err != nil {
		return fmt.Errorf("committed head block %d: %w", b.Head, err)
	}
	if entry.BlockNum != b.Head {
		return fmt.Errorf("committed head block %d: index entry %d holds block %d", b.Head, b.SliceIndexLen-1, entry.BlockNum)
	}
	if end := entry.Offset + 8 + uint64(entry.Size); end != b.SliceDataEnd {
		return fmt.Errorf("committed head block %d ends at byte %d, manifest says %d", b.Head, end, b.SliceDataEnd)
	}
	st, err := os.Stat(filepath.Join(dir, "data.log"))
	if err != nil {
		return err
	}
	if st.Size() < int64(b.SliceDataEnd) {
		return fmt.Errorf("committed head block %d: data.log holds %d bytes, manifest commits %d", b.Head, st.Size(), b.SliceDataEnd)
	}
	return nil
}

func readIndexCount(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var buf [4]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return 0, fmt.Errorf("read count of %s: %w", path, err)
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func readIndexEntry(path string, i uint32) (BlockIndexEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return BlockIndexEntry{}, err
	}
	defer f.Close()
	var buf [32]byte
	if _, err := f.ReadAt(buf[:], 4+int64(i)*32); err != nil {
		return BlockIndexEntry{}, fmt.Errorf("read entry %d of %s: %w", i, path, err)
	}
	le := binary.LittleEndian
	return BlockIndexEntry{
		BlockNum: le.Uint32(buf[0:4]),
		Offset:   le.Uint64(buf[4:12]),
		Size:     le.Uint32(buf[12:16]),
		GlobMin:  le.Uint64(buf[16:24]),
		GlobMax:  le.Uint64(buf[24:32]),
	}, nil
}

func truncateAndSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

func rewriteSliceInfos(basePath string, span, head, headSlice uint32, last BlockIndexEntry, finalized bool) error {
	path := filepath.Join(basePath, "slices.json")
	var infos []SliceInfo
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &infos); err != nil {
			return fmt.Errorf("parse slices.json: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	kept := infos[:0]
	found := false
	for _, info := range infos {
		if head == 0 || info.SliceNum > headSlice {
			continue
		}
		if info.SliceNum == headSlice {
			info.EndBlock = head
			info.Finalized = finalized
			info.GlobMax = last.GlobMax
			found = true
		}
		kept = append(kept, info)
	}
	if head > 0 && !found {
		kept = append(kept, SliceInfo{
			SliceNum: headSlice, StartBlock: headSlice*span + 1, EndBlock: head, MaxBlock: (headSlice + 1) * span,
			BlocksPerSlice: span, Finalized: finalized, GlobMax: last.GlobMax,
		})
		sort.Slice(kept, func(i, j int) bool { return kept[i].SliceNum < kept[j].SliceNum })
	}
	data, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	return coremanifest.WriteFileAtomic(path, data)
}

// Adoption describes the committed boundary of a store written before the publication protocol.
type Adoption struct {
	CoreBounds
	HeadData []byte
}

// FindAdoptionBoundary takes the newest indexed block, which must pass its CRC check, as the committed head.
func FindAdoptionBoundary(basePath string, span uint32) (Adoption, error) {
	var a Adoption
	if data, err := os.ReadFile(filepath.Join(basePath, "slices.json")); err == nil {
		var infos []SliceInfo
		if err := json.Unmarshal(data, &infos); err != nil {
			return a, fmt.Errorf("parse slices.json: %w", err)
		}
		for _, info := range infos {
			if info.BlocksPerSlice != 0 && info.BlocksPerSlice != span {
				return a, fmt.Errorf("slices.json records %d-block slices, configured span is %d", info.BlocksPerSlice, span)
			}
		}
	}
	dirs, err := listSliceDirs(basePath, span)
	if err != nil {
		return a, err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		idx, err := LoadBlockIndex(filepath.Join(dirs[i].path, "blocks.index"))
		if err != nil || idx.Count() == 0 {
			continue
		}
		entries := idx.GetAllEntries()
		for j := 1; j < len(entries); j++ {
			if entries[j].BlockNum != entries[j-1].BlockNum+1 {
				return a, fmt.Errorf("slice %s index jumps from block %d to %d", filepath.Base(dirs[i].path), entries[j-1].BlockNum, entries[j].BlockNum)
			}
		}
		last := entries[len(entries)-1]
		reader, err := NewReaderWithHeader(filepath.Join(dirs[i].path, "data.log"))
		if err != nil {
			return a, fmt.Errorf("open tail slice %s: %w", filepath.Base(dirs[i].path), err)
		}
		raw, err := reader.ReadAt(last.Offset)
		reader.Close()
		if err != nil {
			return a, fmt.Errorf("tail block %d fails its check (%v); repair the store before adopting it", last.BlockNum, err)
		}
		data, err := decompressData(raw)
		if err != nil {
			return a, fmt.Errorf("tail block %d: %w", last.BlockNum, err)
		}
		a.Head = last.BlockNum
		a.SliceIndexLen = uint32(len(entries))
		a.SliceDataEnd = last.Offset + 8 + uint64(last.Size)
		a.HeadData = data
		break
	}
	return a, nil
}
