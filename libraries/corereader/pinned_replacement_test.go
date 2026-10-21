package corereader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func copyTestSlices(t *testing.T) string {
	t.Helper()
	src := getTestDataPath()
	if _, err := os.Stat(src); os.IsNotExist(err) {
		t.Skip("Test data not available")
	}
	dir := filepath.Join(t.TempDir(), "slices")
	if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
		t.Fatalf("copy testdata: %v", err)
	}
	return dir
}

func sliceDirName(info SliceInfo) string {
	return fmt.Sprintf("history_%010d-%010d", info.StartBlock, info.MaxBlock)
}

// replaceSliceDir mimics slicefix: write <dir>.repaired, rename the live dir to .old, rename .repaired into place, delete .old.
func replaceSliceDir(t *testing.T, live string, write func(src, dst string)) {
	t.Helper()
	old := live + ".old"
	repaired := live + ".repaired"
	if err := os.MkdirAll(repaired, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	write(live, repaired)
	if err := os.Rename(live, old); err != nil {
		t.Fatalf("rename live: %v", err)
	}
	if err := os.Rename(repaired, live); err != nil {
		t.Fatalf("rename repaired: %v", err)
	}
	if err := os.RemoveAll(old); err != nil {
		t.Fatalf("remove old: %v", err)
	}
}

func handleUnlinked(t *testing.T, f *os.File) bool {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat: %v", err)
	}
	return st.Nlink == 0
}

func readBlockIndexFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read blocks.index: %v", err)
	}
	return data
}

func TestPinnedSliceUnpinnedWhenIndexed(t *testing.T) {
	dir := copyTestSlices(t)

	sr, err := NewSliceReaderWithOptions(dir, QueryReaderOptions())
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	defer sr.Close()
	if sr.globRangeIndex == nil {
		t.Fatal("expected glob range index")
	}

	slices := sr.sharedMetadata.getSlices()
	target := slices[len(slices)-1]
	indexed := sr.globRangeIndex.highestIndexedSlice.Load()
	sr.globRangeIndex.highestIndexedSlice.Store(target.SliceNum - 1)
	if !sr.isPinnedSlice(target.SliceNum) {
		t.Fatal("target slice not pinned")
	}

	block := target.StartBlock
	if _, _, _, err := sr.GetNotificationsOnly(block); err != nil {
		t.Fatalf("read before replace: %v", err)
	}
	sr.pinnedSlicesMu.RLock()
	pinned := sr.pinnedSlices[target.SliceNum]
	sr.pinnedSlicesMu.RUnlock()
	if pinned == nil {
		t.Fatal("no pinned reader")
	}

	replaceSliceDir(t, filepath.Join(dir, sliceDirName(target)), func(src, dst string) {
		if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
			t.Fatalf("copy back: %v", err)
		}
	})

	sr.globRangeIndex.highestIndexedSlice.Store(max(indexed, target.SliceNum))
	sr.unpinIndexedSlices()

	sr.pinnedSlicesMu.RLock()
	_, stillPinned := sr.pinnedSlices[target.SliceNum]
	sr.pinnedSlicesMu.RUnlock()
	if stillPinned {
		t.Fatalf("slice %d still pinned after the glob range index covered it", target.SliceNum)
	}
	if pinned.blockFile != nil {
		t.Fatal("unpinned idle reader was not closed")
	}

	if _, _, _, err := sr.GetNotificationsOnly(block); err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	sr.sliceReadersMu.RLock()
	reader := sr.sliceReaders[target.SliceNum]
	sr.sliceReadersMu.RUnlock()
	if reader == nil {
		t.Fatal("slice not served through the LRU after unpinning")
	}
	if handleUnlinked(t, reader.blockFile) {
		t.Fatalf("reader for slice %d still reads the deleted data.log after the slice was replaced", target.SliceNum)
	}
}

func TestPinnedSliceUnpinWaitsForInFlightRead(t *testing.T) {
	dir := copyTestSlices(t)

	sr, err := NewSliceReaderWithOptions(dir, QueryReaderOptions())
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	defer sr.Close()
	if sr.globRangeIndex == nil {
		t.Fatal("expected glob range index")
	}

	slices := sr.sharedMetadata.getSlices()
	target := slices[len(slices)-1]
	sr.globRangeIndex.highestIndexedSlice.Store(target.SliceNum - 1)

	reader, err := sr.getSliceReader(target.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}

	sr.globRangeIndex.highestIndexedSlice.Store(target.SliceNum)
	sr.unpinIndexedSlices()

	if reader.blockFile == nil {
		t.Fatal("unpin closed a reader that was still in use")
	}
	if _, err := reader.readBlockData(target.StartBlock); err != nil {
		t.Fatalf("read on in-flight reader: %v", err)
	}
	sr.releaseSliceReader(reader)
	if reader.blockFile != nil {
		t.Fatal("last release did not close the unpinned reader")
	}
}

// Reading a block the tip index lacked pairs the replacement's shifted offsets with the old mmap.
func TestPinnedSliceReaderRecoversFromReplacementWithShiftedOffsets(t *testing.T) {
	dir := copyTestSlices(t)

	sr, err := NewSliceReaderWithOptions(dir, QueryReaderOptions())
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	defer sr.Close()
	if sr.globRangeIndex == nil {
		t.Fatal("expected glob range index")
	}

	slices := sr.sharedMetadata.getSlices()
	target := slices[len(slices)-1]
	live := filepath.Join(dir, sliceDirName(target))

	origIndex := readBlockIndexFile(t, filepath.Join(live, "blocks.index"))
	origData, err := os.ReadFile(filepath.Join(live, "data.log"))
	if err != nil {
		t.Fatalf("read data.log: %v", err)
	}

	const entrySize = 32
	count := (len(origIndex) - 4) / entrySize
	half := count / 2
	lateBlock := binary.LittleEndian.Uint32(origIndex[4+(half+1)*entrySize:])

	tipData := bytes.Clone(origData)
	tipData[20] = 0
	if err := os.WriteFile(filepath.Join(live, "data.log"), tipData, 0o644); err != nil {
		t.Fatalf("write tip data.log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(live, "blocks.index"), origIndex[:4+half*entrySize], 0o644); err != nil {
		t.Fatalf("write tip blocks.index: %v", err)
	}

	sr.globRangeIndex.highestIndexedSlice.Store(target.SliceNum - 1)
	reader, err := sr.getSliceReader(target.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}
	defer sr.releaseSliceReader(reader)
	reader.mmapWaitTimeout = 200 * time.Millisecond
	if _, err := reader.readBlockData(target.StartBlock); err != nil {
		t.Fatalf("read before replace: %v", err)
	}

	const pad = 4096
	replaceSliceDir(t, live, func(_, dst string) {
		shifted := make([]byte, 0, len(origData)+pad)
		shifted = append(shifted, origData[:DataLogHeaderSize]...)
		shifted = append(shifted, make([]byte, pad)...)
		shifted = append(shifted, origData[DataLogHeaderSize:]...)
		if err := os.WriteFile(filepath.Join(dst, "data.log"), shifted, 0o644); err != nil {
			t.Fatalf("write shifted data.log: %v", err)
		}
		index := bytes.Clone(origIndex)
		for off := 4; off+entrySize <= len(index); off += entrySize {
			o := binary.LittleEndian.Uint64(index[off+4:])
			binary.LittleEndian.PutUint64(index[off+4:], o+pad)
		}
		if err := os.WriteFile(filepath.Join(dst, "blocks.index"), index, 0o644); err != nil {
			t.Fatalf("write shifted blocks.index: %v", err)
		}
	})

	got, err := reader.readBlockData(lateBlock)
	if err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	if handleUnlinked(t, reader.blockFile) {
		t.Fatal("pinned reader still holds the deleted data.log")
	}

	fresh, err := newSliceReader(live, target.SliceNum)
	if err != nil {
		t.Fatalf("fresh reader: %v", err)
	}
	defer fresh.close()
	want, err := fresh.readBlockData(lateBlock)
	if err != nil {
		t.Fatalf("fresh read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("block %d from reopened reader differs from a fresh reader", lateBlock)
	}
}

type replacementFixture struct {
	sr        *SliceReader
	target    SliceInfo
	live      string
	origIndex []byte
	origData  []byte
	half      int
}

const blockIndexEntrySize = 32

func newReplacementFixture(t *testing.T, finalized bool) *replacementFixture {
	t.Helper()
	dir := copyTestSlices(t)
	sr, err := NewSliceReaderWithOptions(dir, QueryReaderOptions())
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	t.Cleanup(func() { sr.Close() })
	if sr.globRangeIndex == nil {
		t.Fatal("expected glob range index")
	}

	slices := sr.sharedMetadata.getSlices()
	f := &replacementFixture{sr: sr, target: slices[len(slices)-1]}
	f.live = filepath.Join(dir, sliceDirName(f.target))
	f.origIndex = readBlockIndexFile(t, filepath.Join(f.live, "blocks.index"))
	f.origData, err = os.ReadFile(filepath.Join(f.live, "data.log"))
	if err != nil {
		t.Fatalf("read data.log: %v", err)
	}
	f.half = (len(f.origIndex) - 4) / blockIndexEntrySize / 2

	tipData := bytes.Clone(f.origData)
	if !finalized {
		tipData[20] = 0
	}
	if err := os.WriteFile(filepath.Join(f.live, "data.log"), tipData, 0o644); err != nil {
		t.Fatalf("write tip data.log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.live, "blocks.index"), f.origIndex[:4+f.half*blockIndexEntrySize], 0o644); err != nil {
		t.Fatalf("write tip blocks.index: %v", err)
	}
	sr.globRangeIndex.highestIndexedSlice.Store(f.target.SliceNum - 1)
	return f
}

func (f *replacementFixture) blockAt(i int) uint32 {
	return binary.LittleEndian.Uint32(f.origIndex[4+i*blockIndexEntrySize:])
}

func (f *replacementFixture) replaceShifted(t *testing.T, pad int) {
	t.Helper()
	replaceSliceDir(t, f.live, func(_, dst string) {
		shifted := make([]byte, 0, len(f.origData)+pad)
		shifted = append(shifted, f.origData[:DataLogHeaderSize]...)
		shifted = append(shifted, make([]byte, pad)...)
		shifted = append(shifted, f.origData[DataLogHeaderSize:]...)
		if err := os.WriteFile(filepath.Join(dst, "data.log"), shifted, 0o644); err != nil {
			t.Errorf("write shifted data.log: %v", err)
		}
		index := bytes.Clone(f.origIndex)
		for off := 4; off+blockIndexEntrySize <= len(index); off += blockIndexEntrySize {
			o := binary.LittleEndian.Uint64(index[off+4:])
			binary.LittleEndian.PutUint64(index[off+4:], o+uint64(pad))
		}
		if err := os.WriteFile(filepath.Join(dst, "blocks.index"), index, 0o644); err != nil {
			t.Errorf("write shifted blocks.index: %v", err)
		}
	})
}

func (f *replacementFixture) expectedBlocks(t *testing.T) map[uint32][]byte {
	t.Helper()
	fresh, err := newSliceReader(f.live, f.target.SliceNum)
	if err != nil {
		t.Fatalf("fresh reader: %v", err)
	}
	defer fresh.close()
	want := make(map[uint32][]byte)
	for i := 0; i < 2*f.half; i++ {
		bn := f.blockAt(i)
		data, err := fresh.readBlockData(bn)
		if err != nil {
			t.Fatalf("fresh read %d: %v", bn, err)
		}
		want[bn] = data
	}
	return want
}

func TestFinalizedSliceReaderIndexMissAfterReplacement(t *testing.T) {
	f := newReplacementFixture(t, true)
	reader, err := f.sr.getSliceReader(f.target.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}
	defer f.sr.releaseSliceReader(reader)
	if _, err := reader.readBlockData(f.target.StartBlock); err != nil {
		t.Fatalf("read before replace: %v", err)
	}

	f.replaceShifted(t, 4096)
	want := f.expectedBlocks(t)

	lateBlock := f.blockAt(f.half + 1)
	got, err := reader.readBlockData(lateBlock)
	if err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	if !bytes.Equal(got, want[lateBlock]) {
		t.Fatalf("block %d read the replacement's offsets from the old data.log", lateBlock)
	}
}

func TestSliceReaderConcurrentReadsAcrossReplacement(t *testing.T) {
	for _, finalized := range []bool{true, false} {
		t.Run(fmt.Sprintf("finalized=%v", finalized), func(t *testing.T) {
			f := newReplacementFixture(t, finalized)
			reader, err := f.sr.getSliceReader(f.target.SliceNum)
			if err != nil {
				t.Fatalf("getSliceReader: %v", err)
			}
			defer f.sr.releaseSliceReader(reader)
			reader.mmapWaitTimeout = 2 * time.Second

			var original map[uint32][]byte
			{
				want := make(map[uint32][]byte)
				for i := 0; i < f.half; i++ {
					bn := f.blockAt(i)
					data, err := reader.readBlockData(bn)
					if err != nil {
						t.Fatalf("read %d before replace: %v", bn, err)
					}
					want[bn] = data
				}
				original = want
			}

			var stop atomic.Bool
			var wg sync.WaitGroup
			errs := make(chan error, 64)
			var expected atomic.Pointer[map[uint32][]byte]
			expected.Store(&original)
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := g; !stop.Load(); i += 7 {
						want := *expected.Load()
						bn := f.blockAt(i % (2 * f.half))
						got, err := reader.readBlockData(bn)
						if exp, ok := want[bn]; err == nil && ok && !bytes.Equal(got, exp) {
							err = fmt.Errorf("block %d returned wrong bytes", bn)
						} else if err != nil && ok {
							err = fmt.Errorf("block %d: %w", bn, err)
						} else {
							err = nil
						}
						if err != nil {
							select {
							case errs <- err:
							default:
							}
						}
					}
				}(g)
			}

			time.Sleep(20 * time.Millisecond)
			f.replaceShifted(t, 4096)
			all := f.expectedBlocks(t)
			expected.Store(&all)
			time.Sleep(200 * time.Millisecond)
			stop.Store(true)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

func newIndexUpdateFixture(t *testing.T) (*SliceReader, string, SliceInfo) {
	t.Helper()
	dir := copyTestSlices(t)
	opts := QueryReaderOptions()
	opts.UnindexedSliceCount = 1
	sr, err := NewSliceReaderWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSliceReaderWithOptions: %v", err)
	}
	t.Cleanup(func() { sr.Close() })
	if sr.globRangeIndex == nil {
		t.Fatal("expected glob range index")
	}
	sr.SetGlobRangeUpdateCooldown(0)
	slices := sr.sharedMetadata.getSlices()
	if sr.globRangeIndex.highestIndexedSlice.Load() != slices[0].SliceNum {
		t.Fatalf("expected startup index through slice %d", slices[0].SliceNum)
	}
	return sr, dir, slices[1]
}

func TestGlobRangeIndexUpdateAdvancesAndUnpins(t *testing.T) {
	sr, _, next := newIndexUpdateFixture(t)

	reader, err := sr.getSliceReader(next.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}
	if reader.owner != ownerPinned {
		t.Fatal("expected slice beyond the index to be pinned")
	}
	minGlob, _ := reader.getGlobRangeFromBlockIndex()
	sr.releaseSliceReader(reader)
	rangesBefore := len(sr.globRangeIndex.ranges)

	if err := sr.UpdateGlobRangeIndexWithNewSlices(); err != nil {
		t.Fatalf("update: %v", err)
	}

	if got := sr.globRangeIndex.highestIndexedSlice.Load(); got != next.SliceNum {
		t.Fatalf("index through slice %d, want %d", got, next.SliceNum)
	}
	if len(sr.globRangeIndex.ranges) != rangesBefore+1 {
		t.Fatalf("ranges %d, want %d", len(sr.globRangeIndex.ranges), rangesBefore+1)
	}
	sr.pinnedSlicesMu.RLock()
	pinned := len(sr.pinnedSlices)
	sr.pinnedSlicesMu.RUnlock()
	if pinned != 0 {
		t.Fatalf("%d slices still pinned after the index covered them", pinned)
	}
	if reader.blockFile != nil {
		t.Fatal("unpinned reader was not closed")
	}

	info, fromDisk, err := sr.findSliceForGlobWithRangeIndexTracked(minGlob)
	if err != nil {
		t.Fatalf("glob lookup: %v", err)
	}
	if info.SliceNum != next.SliceNum || fromDisk {
		t.Fatalf("glob %d resolved to slice %d (fromDisk=%v), want slice %d from the index", minGlob, info.SliceNum, fromDisk, next.SliceNum)
	}
	if meta := sr.sharedMetadata.getSliceByNum(next.SliceNum); meta == nil || !meta.Finalized {
		t.Fatal("metadata not marked finalized")
	}
}

func TestGlobRangeIndexUpdateStopsAtUnfinalizedSlice(t *testing.T) {
	sr, dir, next := newIndexUpdateFixture(t)

	dataLog := filepath.Join(dir, sliceDirName(next), "data.log")
	data, err := os.ReadFile(dataLog)
	if err != nil {
		t.Fatalf("read data.log: %v", err)
	}
	data[20] = 0
	if err := os.WriteFile(dataLog, data, 0o644); err != nil {
		t.Fatalf("write data.log: %v", err)
	}

	before := sr.globRangeIndex.highestIndexedSlice.Load()
	if err := sr.UpdateGlobRangeIndexWithNewSlices(); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := sr.globRangeIndex.highestIndexedSlice.Load(); got != before {
		t.Fatalf("index advanced to slice %d past an unfinalized slice", got)
	}
}

func TestGlobRangeIndexUpdateLoopRuns(t *testing.T) {
	orig := globRangeIndexUpdateInterval
	globRangeIndexUpdateInterval = 10 * time.Millisecond
	t.Cleanup(func() { globRangeIndexUpdateInterval = orig })

	sr, _, next := newIndexUpdateFixture(t)
	deadline := time.Now().Add(2 * time.Second)
	for sr.globRangeIndex.highestIndexedSlice.Load() != next.SliceNum {
		if time.Now().After(deadline) {
			t.Fatal("background loop never advanced the glob range index")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *replacementFixture) globAt(i int) uint64 {
	return binary.LittleEndian.Uint64(f.origIndex[4+i*blockIndexEntrySize+16:])
}

func (f *replacementFixture) shiftedFiles(pad int) ([]byte, []byte) {
	data := make([]byte, 0, len(f.origData)+pad)
	data = append(data, f.origData[:DataLogHeaderSize]...)
	data = append(data, make([]byte, pad)...)
	data = append(data, f.origData[DataLogHeaderSize:]...)
	index := bytes.Clone(f.origIndex)
	for off := 4; off+blockIndexEntrySize <= len(index); off += blockIndexEntrySize {
		o := binary.LittleEndian.Uint64(index[off+4:])
		binary.LittleEndian.PutUint64(index[off+4:], o+uint64(pad))
	}
	return data, index
}

func TestSliceReaderWaitsForIndexAfterDataLogSwap(t *testing.T) {
	f := newReplacementFixture(t, false)
	reader, err := f.sr.getSliceReader(f.target.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}
	defer f.sr.releaseSliceReader(reader)
	reader.mmapWaitTimeout = 200 * time.Millisecond

	early := f.blockAt(1)
	want, err := reader.readBlockData(early)
	if err != nil {
		t.Fatalf("read before swap: %v", err)
	}

	// coreindex's rebuildSliceFiltered order: write data.log.new, remove data.log, rename, then rewrite blocks.index.
	shiftedData, shiftedIndex := f.shiftedFiles(4096)
	dataLog := filepath.Join(f.live, "data.log")
	if err := os.WriteFile(dataLog+".new", shiftedData, 0o644); err != nil {
		t.Fatalf("write data.log.new: %v", err)
	}
	if err := os.Remove(dataLog); err != nil {
		t.Fatalf("remove data.log: %v", err)
	}
	if err := os.Rename(dataLog+".new", dataLog); err != nil {
		t.Fatalf("rename data.log: %v", err)
	}

	reader.readBlockData(f.blockAt(f.half + 1))

	got, err := reader.readBlockData(early)
	if err != nil {
		t.Fatalf("read during swap: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("block %d changed while blocks.index was stale", early)
	}

	if err := os.WriteFile(filepath.Join(f.live, "blocks.index"), shiftedIndex, 0o644); err != nil {
		t.Fatalf("write blocks.index: %v", err)
	}
	all := f.expectedBlocks(t)
	for _, bn := range []uint32{f.blockAt(f.half + 1), early} {
		got, err := reader.readBlockData(bn)
		if err != nil {
			t.Fatalf("read %d after swap: %v", bn, err)
		}
		if !bytes.Equal(got, all[bn]) {
			t.Fatalf("block %d wrong after swap", bn)
		}
	}
	if handleUnlinked(t, reader.blockFile) {
		t.Fatal("reader still holds the removed data.log")
	}
}

func TestBinarySearchGlobReloadReopensReplacedSlice(t *testing.T) {
	f := newReplacementFixture(t, true)
	reader, err := f.sr.getSliceReader(f.target.SliceNum)
	if err != nil {
		t.Fatalf("getSliceReader: %v", err)
	}
	defer f.sr.releaseSliceReader(reader)
	if _, err := reader.readBlockData(f.target.StartBlock); err != nil {
		t.Fatalf("read before replace: %v", err)
	}

	f.replaceShifted(t, 4096)
	want := f.expectedBlocks(t)

	late := f.blockAt(f.half + 1)
	bn, err := reader.binarySearchGlob(f.globAt(f.half + 1))
	if err != nil {
		t.Fatalf("binarySearchGlob: %v", err)
	}
	if bn != late {
		t.Fatalf("glob resolved to block %d, want %d", bn, late)
	}
	got, err := reader.readBlockData(late)
	if err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	if !bytes.Equal(got, want[late]) {
		t.Fatalf("block %d read the replacement's offsets from the old data.log", late)
	}
}
