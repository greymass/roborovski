package corereader

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func midGlobOfSlice(t *testing.T, root string, sliceNum uint32) uint64 {
	t.Helper()
	idx, err := os.ReadFile(filepath.Join(slicePathFor(root, sliceNum), "blocks.index"))
	if err != nil {
		t.Fatal(err)
	}
	n := int(binary.LittleEndian.Uint32(idx[0:4]))
	e := idx[4+(n/4)*32 : 4+(n/4+1)*32]
	return binary.LittleEndian.Uint64(e[16:24])
}

func staleQueryReader(t *testing.T) (*SliceReader, string) {
	t.Helper()
	root := copyTestSlices(t)
	publishHead(t, root, 1, 5000)
	sr, err := NewSliceReaderWithOptions(root, QueryReaderOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sr.Close() })
	if _, _, err := sr.GetStateProps(true); err != nil {
		t.Fatal(err)
	}
	if sr.globRangeIndex == nil {
		sr.globRangeIndex = &GlobRangeIndex{updateCooldown: time.Hour}
	}
	return sr, root
}

func TestGlobQueryAfterStaleViewRecovers(t *testing.T) {
	sr, root := staleQueryReader(t)

	publishHead(t, root, 1, 25000)
	glob := midGlobOfSlice(t, root, 1)

	_, staleErr := sr.GetBlockByGlob(glob)
	t.Logf("query before the reader refreshes its view: %v", staleErr)

	if head, _, err := sr.GetStateProps(true); err != nil || head != 25000 {
		t.Fatalf("GetStateProps = %d, %v; want 25000", head, err)
	}
	for i := 0; i < 3; i++ {
		if _, err := sr.GetBlockByGlob(glob); err != nil {
			t.Fatalf("glob %d after refresh to head 25000 (attempt %d): %v", glob, i, err)
		}
	}
}

func TestGlobAboveStaleViewRefreshesFirst(t *testing.T) {
	sr, root := staleQueryReader(t)

	publishHead(t, root, 1, 25000)
	glob := midGlobOfSlice(t, root, 1)

	if _, err := sr.GetBlockByGlob(glob); err != nil {
		t.Fatalf("glob %d with a newer published view: %v", glob, err)
	}
	if lim, _ := sr.headLimit(); lim != 25000 {
		t.Fatalf("head after lookup = %d, want 25000", lim)
	}
	sr.pinnedSlicesMu.RLock()
	defer sr.pinnedSlicesMu.RUnlock()
	for num, r := range sr.pinnedSlices {
		if len(r.blockIndex) == 0 {
			t.Fatalf("slice %d pinned with an empty index", num)
		}
	}
}

func TestGlobAbovePublishedHeadIsBeyondHead(t *testing.T) {
	sr, root := staleQueryReader(t)
	glob := midGlobOfSlice(t, root, 1)

	_, err := sr.GetBlockByGlob(glob)
	if !errors.Is(err, ErrBeyondHead) {
		t.Fatalf("glob %d above head 5000: err = %v, want ErrBeyondHead", glob, err)
	}
	sr.pinnedSlicesMu.RLock()
	defer sr.pinnedSlicesMu.RUnlock()
	if r, ok := sr.pinnedSlices[1]; ok {
		t.Fatalf("slice 1 pinned under head 5000 with %d index entries", len(r.blockIndex))
	}
}

func TestEmptyTrimmedReaderIsNotCached(t *testing.T) {
	sr, root := staleQueryReader(t)
	if err := sr.sharedMetadata.addSliceIfMissing(t, root, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := sr.getSliceReader(1); !errors.Is(err, ErrBeyondHead) {
		t.Fatalf("getSliceReader(1) under head 5000: err = %v, want ErrBeyondHead", err)
	}
	sr.pinnedSlicesMu.RLock()
	_, pinned := sr.pinnedSlices[1]
	sr.pinnedSlicesMu.RUnlock()
	sr.sliceReadersMu.RLock()
	_, cached := sr.sliceReaders[1]
	sr.sliceReadersMu.RUnlock()
	if pinned || cached {
		t.Fatalf("empty reader kept: pinned=%v cached=%v", pinned, cached)
	}
}

func (sm *SharedSliceMetadata) addSliceIfMissing(t *testing.T, root string, sliceNum uint32) error {
	t.Helper()
	if sm.getSliceByNum(sliceNum) != nil {
		return nil
	}
	end, gmin, gmax, err := findLastBlockInIndex(filepath.Join(slicePathFor(root, sliceNum), "blocks.index"))
	if err != nil {
		return err
	}
	start := sliceNum*10000 + 1
	sm.addSlice(SliceInfo{SliceNum: sliceNum, StartBlock: start, EndBlock: end, MaxBlock: start + 9999, BlocksPerSlice: 10000, GlobMin: gmin, GlobMax: gmax})
	return nil
}
