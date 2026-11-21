package corereader

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/coremanifest"
)

func publishHead(t *testing.T, root string, epoch uint64, head uint32) {
	t.Helper()
	m := &coremanifest.Manifest{Epoch: epoch, Head: head}
	if head > 0 {
		idx, err := os.ReadFile(filepath.Join(slicePathFor(root, sliceNumForBlock(head)), "blocks.index"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; 4+(i+1)*32 <= len(idx); i++ {
			e := idx[4+i*32 : 4+(i+1)*32]
			if binary.LittleEndian.Uint32(e[0:4]) > head {
				break
			}
			m.SliceIndexLen = uint32(i + 1)
			m.SliceDataEnd = binary.LittleEndian.Uint64(e[4:12]) + 4 + uint64(binary.LittleEndian.Uint32(e[12:16])) + 4
		}
	}
	time.Sleep(2 * time.Millisecond)
	if err := coremanifest.WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
}

func readable(sr *SliceReader, block uint32) error {
	_, err := sr.GetTransactionIDsRaw(block)
	return err
}

func TestManifestBoundsReader(t *testing.T) {
	root := copyTestSlices(t)
	publishHead(t, root, 1, 15000)

	sr, err := NewSliceReaderWithOptions(root, QueryReaderOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	head, lib, err := sr.GetStateProps(true)
	if err != nil || head != 15000 || lib != 15000 {
		t.Fatalf("GetStateProps = %d, %d, %v; want 15000", head, lib, err)
	}
	if err := readable(sr, 15000); err != nil {
		t.Fatalf("block at head: %v", err)
	}
	for _, b := range []uint32{15001, 20000, 25000} {
		if err := readable(sr, b); !errors.Is(err, ErrBeyondHead) {
			t.Fatalf("block %d above head: err = %v, want ErrBeyondHead", b, err)
		}
	}
	if n := len(sr.GetSliceInfos()); n != 2 {
		t.Fatalf("reader knows %d slices, want 2 (through the head slice)", n)
	}

	publishHead(t, root, 1, 25000)
	if head, _, _ := sr.GetStateProps(true); head != 25000 {
		t.Fatalf("after growth head = %d, want 25000", head)
	}
	if err := readable(sr, 25000); err != nil {
		t.Fatalf("block 25000 after growth: %v", err)
	}
	if err := readable(sr, 25001); !errors.Is(err, ErrBeyondHead) {
		t.Fatalf("block 25001: err = %v, want ErrBeyondHead", err)
	}

	publishHead(t, root, 2, 5000)
	if err := os.RemoveAll(slicePathFor(root, 2)); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(slicePathFor(root, 1)); err != nil {
		t.Fatal(err)
	}
	if head, _, _ := sr.GetStateProps(true); head != 5000 {
		t.Fatalf("after reset head = %d, want 5000", head)
	}
	if err := readable(sr, 4000); err != nil {
		t.Fatalf("retained block 4000: %v", err)
	}
	if err := readable(sr, 15000); !errors.Is(err, ErrBeyondHead) {
		t.Fatalf("removed block 15000: err = %v, want ErrBeyondHead", err)
	}
	if n := len(sr.GetSliceInfos()); n != 1 {
		t.Fatalf("reader knows %d slices after reset, want 1", n)
	}
}

func TestManifestReaderNotReady(t *testing.T) {
	root := t.TempDir()
	if _, err := NewSliceReader(root); !errors.Is(err, ErrNotReady) {
		t.Fatalf("empty directory: err = %v, want ErrNotReady", err)
	}
	if !IsRetriableError(ErrNotReady) {
		t.Fatal("ErrNotReady must be retriable")
	}

	if err := coremanifest.WriteManifest(root, &coremanifest.Manifest{Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSliceReader(root); !errors.Is(err, ErrNotReady) {
		t.Fatalf("empty manifest: err = %v, want ErrNotReady", err)
	}
}

func TestManifestMissingAfterSeen(t *testing.T) {
	root := copyTestSlices(t)
	publishHead(t, root, 1, 15000)
	sr, err := NewSliceReaderWithOptions(root, QueryReaderOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	if err := os.Remove(coremanifest.ManifestPath(root)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sr.GetStateProps(true); !errors.Is(err, ErrNotReady) {
		t.Fatalf("manifest removed: err = %v, want ErrNotReady", err)
	}
}

func TestLegacyStoreUnbounded(t *testing.T) {
	root := copyTestSlices(t)
	sr, err := NewSliceReaderWithOptions(root, QueryReaderOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	if sr.Manifest() != nil {
		t.Fatal("legacy store should have no manifest")
	}
	if err := readable(sr, 30000); err != nil {
		t.Fatalf("legacy read of block 30000: %v", err)
	}
}

func TestSlicesCacheEpoch(t *testing.T) {
	root := t.TempDir()
	in := []SliceInfo{{SliceNum: 0, StartBlock: 1, EndBlock: 10000, MaxBlock: 10000, BlocksPerSlice: 10000, Finalized: true}}
	if err := SaveSlicesToCacheEpoch(root, in, 7); err != nil {
		t.Fatal(err)
	}
	out, epoch, err := loadSlicesCacheWithEpoch(root)
	if err != nil || epoch != 7 || len(out) != 1 || out[0].EndBlock != 10000 {
		t.Fatalf("round trip: %+v epoch %d err %v", out, epoch, err)
	}
}
