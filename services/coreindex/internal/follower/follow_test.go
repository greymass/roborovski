package follower

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

const span = 10000

func blob(n uint32) []byte {
	b := make([]byte, 40+int(n%13)*7)
	binary.LittleEndian.PutUint32(b[0:4], n)
	id := sha256.Sum256([]byte(fmt.Sprintf("block-%d", n)))
	copy(b[8:40], id[:])
	for i := 40; i < len(b); i++ {
		b[i] = byte(n) ^ byte(i)
	}
	return b
}

func headID(b []byte) ([32]byte, error) {
	var id [32]byte
	if len(b) < 40 {
		return id, errors.New("short block")
	}
	copy(id[:], b[8:40])
	return id, nil
}

type writer struct {
	core, abi string
	pub       *publish.Publisher
	store     *appendlog.SliceStore
	abiW      *abicache.Writer
}

func openWriter(t *testing.T, core, abi string) *writer {
	t.Helper()
	pub, err := publish.Open(publish.Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if err := pub.Prepare(); err != nil {
		pub.Close()
		t.Fatalf("prepare: %v", err)
	}
	store, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: span})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetCommittedHead(pub.Manifest().Head); err != nil {
		t.Fatal(err)
	}
	w, err := abicache.NewWriter(abi)
	if err != nil {
		t.Fatal(err)
	}
	return &writer{core: core, abi: abi, pub: pub, store: store, abiW: w}
}

func (w *writer) write(t *testing.T, from, to uint32) {
	t.Helper()
	var batch []appendlog.BlockEntry
	for n := from; n <= to; n++ {
		batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: blob(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
		if n%37 == 0 {
			if err := w.abiW.Write(n, uint64(n), []byte(fmt.Sprintf(`{"n":%d}`, n))); err != nil {
				t.Fatal(err)
			}
		}
		if len(batch) == 1000 {
			if err := w.store.AppendBlockBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := w.store.AppendBlockBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
}

func (w *writer) commit(t *testing.T) {
	t.Helper()
	if _, err := w.pub.Commit(w.store, w.abiW); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func (w *writer) close() {
	w.store.Close()
	w.abiW.Close()
	w.pub.Close()
}

// kill drops the locks and leaves every buffered and uncommitted write behind, as a crashed process would.
func (w *writer) kill() { w.pub.ReleaseLocks() }

// resetTo follows the reset order: epoch-bumped manifest first, then slice removal, then the ABI index prefix by rename.
func resetTo(t *testing.T, core, abi string, b uint32) {
	t.Helper()
	coreLock, err := coremanifest.AcquireWriterLock(core)
	if err != nil {
		t.Fatal(err)
	}
	defer coreLock.Release()
	abiLock, err := coremanifest.AcquireWriterLock(abi)
	if err != nil {
		t.Fatal(err)
	}
	defer abiLock.Release()

	old, err := coremanifest.ReadManifest(core)
	if err != nil {
		t.Fatal(err)
	}
	m := *old
	m.Epoch++
	m.Head = b
	m.HeadID, _ = headID(blob(b))
	headSlice := (b - 1) / span
	headDir := sliceDir(core, headSlice)
	idx, err := os.ReadFile(filepath.Join(headDir, "blocks.index"))
	if err != nil {
		t.Fatal(err)
	}
	m.SliceIndexLen, m.SliceDataEnd = 0, 0
	for i := 0; 4+(i+1)*32 <= len(idx); i++ {
		e := idx[4+i*32 : 4+(i+1)*32]
		if binary.LittleEndian.Uint32(e[0:4]) > b {
			break
		}
		m.SliceIndexLen = uint32(i + 1)
		m.SliceDataEnd = binary.LittleEndian.Uint64(e[4:12]) + 8 + uint64(binary.LittleEndian.Uint32(e[12:16]))
	}
	if m.ABIDataEnd, m.ABIIndexLen, err = abicache.CommittedBounds(abi, b); err != nil {
		t.Fatal(err)
	}
	if err := coremanifest.WriteManifest(core, &m); err != nil {
		t.Fatal(err)
	}
	if err := coremanifest.WritePublished(core, &m); err != nil {
		t.Fatal(err)
	}
	for n := headSlice + 1; ; n++ {
		if _, err := os.Stat(sliceDir(core, n)); err != nil {
			break
		}
		if err := os.RemoveAll(sliceDir(core, n)); err != nil {
			t.Fatal(err)
		}
	}
	ai, err := os.ReadFile(filepath.Join(abi, "abis.index"))
	if err != nil {
		t.Fatal(err)
	}
	if err := coremanifest.WriteFileAtomic(filepath.Join(abi, "abis.index"), ai[:m.ABIIndexLen*20]); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(abi, "abis.dat"), int64(m.ABIDataEnd)); err != nil {
		t.Fatal(err)
	}
}

func sliceDir(root string, n uint32) string {
	start := n*span + 1
	return filepath.Join(root, fmt.Sprintf("history_%010d-%010d", start, start+span-1))
}

type recorder struct {
	mu         sync.Mutex
	broadcasts []uint32
	epochs     int
	live       bool
}

func (r *recorder) hooks() Hooks {
	return Hooks{
		Broadcast: func(n uint32, data []byte) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.broadcasts = append(r.broadcasts, n)
		},
		EpochChanged: func(*coremanifest.Manifest) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.epochs++
		},
		SetLive: func(live bool) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.live = live
		},
	}
}

func (r *recorder) take() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.broadcasts
	r.broadcasts = nil
	return out
}

func expectRange(t *testing.T, got []uint32, from, to uint32) {
	t.Helper()
	if len(got) != int(to-from+1) {
		t.Fatalf("broadcast %d blocks, want %d (%d..%d)", len(got), to-from+1, from, to)
	}
	for i, n := range got {
		if n != from+uint32(i) {
			t.Fatalf("broadcast %d at position %d, want %d", n, i, from+uint32(i))
		}
	}
}

func poll(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
}

func expectServed(t *testing.T, s *Store, head uint32, state State) {
	t.Helper()
	if got := s.GetHead(); got != head {
		t.Fatalf("served head %d, want %d", got, head)
	}
	if got := s.State(); got != state {
		t.Fatalf("state %s, want %s", got, state)
	}
	data, err := s.GetBlock(head)
	if err != nil || !bytes.Equal(data, blob(head)) {
		t.Fatalf("block %d at the head: %v", head, err)
	}
	if _, err := s.GetBlock(head + 1); !errors.Is(err, appendlog.ErrNotFound) {
		t.Fatalf("block %d above the head: err = %v, want ErrNotFound", head+1, err)
	}
	if n, err := s.GetBlockByGlob(uint64(head)*10 + 5); err != nil || n != head {
		t.Fatalf("glob in block %d: got %d, %v", head, n, err)
	}
	if _, err := s.GetBlockByGlob(uint64(head+1)*10 + 5); err == nil {
		t.Fatalf("glob in block %d above the head was served", head+1)
	}
}

func TestFollowerTracksTraceWriter(t *testing.T) {
	dir := t.TempDir()
	core, abi := filepath.Join(dir, "core"), filepath.Join(dir, "core", "abis")
	if err := os.MkdirAll(core, 0755); err != nil {
		t.Fatal(err)
	}

	rec := &recorder{}
	s := NewStore(core, corereader.QueryReaderOptions(), rec.hooks())
	defer s.Close()

	poll(t, s)
	if s.State() != NotReady || s.GetHead() != 0 {
		t.Fatalf("before any manifest: state %s head %d", s.State(), s.GetHead())
	}

	w := openWriter(t, core, abi)
	w.write(t, 2, 5000)
	w.commit(t)
	poll(t, s)
	expectServed(t, s, 5000, Ready)
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("first view broadcast %d blocks of existing history", len(got))
	}

	const retained = 9000
	var stop atomic.Bool
	var readFailures atomic.Int64
	var firstFailure atomic.Value
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		rng := rand.New(rand.NewSource(1))
		for !stop.Load() {
			n := uint32(2 + rng.Intn(4999))
			data, err := s.GetBlock(n)
			if err != nil || !bytes.Equal(data, blob(n)) {
				readFailures.Add(1)
				firstFailure.CompareAndSwap(nil, fmt.Sprintf("block %d: %v", n, err))
			}
		}
	}()

	// Growth inside the active slice: uncommitted blocks stay invisible.
	w.write(t, 5001, retained)
	poll(t, s)
	expectServed(t, s, 5000, Ready)
	w.commit(t)
	poll(t, s)
	expectServed(t, s, retained, Ready)
	expectRange(t, rec.take(), 5001, retained)

	// Rollover into the second slice.
	w.write(t, retained+1, 15000)
	w.commit(t)
	poll(t, s)
	expectServed(t, s, 15000, Ready)
	expectRange(t, rec.take(), retained+1, 15000)

	abiReader, err := abicache.NewReader(abi)
	if err != nil {
		t.Fatal(err)
	}
	defer abiReader.Close()
	if err := abiReader.SetLimit(func() (uint32, bool) { return s.Manifest().ABIIndexLen, true }); err != nil {
		t.Fatal(err)
	}

	// Writer crash with an uncommitted tail in the next slice: the head is uncertain until a writer recovers.
	w.write(t, 15001, 21000)
	w.kill()
	poll(t, s)
	expectServed(t, s, 15000, Uncertain)
	if rec.live {
		t.Fatal("live streaming stayed on with an uncertain head")
	}
	if _, err := abiReader.Get(15022, 30000); err == nil {
		t.Fatal("ABI written after the manifest was served")
	}
	if _, err := abiReader.Get(14985, 30000); err != nil {
		t.Fatalf("committed ABI at block 14985: %v", err)
	}

	w = openWriter(t, core, abi)
	poll(t, s)
	expectServed(t, s, 15000, Ready)
	w.write(t, 15001, 16000)
	w.commit(t)
	poll(t, s)
	expectServed(t, s, 16000, Ready)
	expectRange(t, rec.take(), 15001, 16000)
	w.close()

	// Reset to block 12000 and rebuild.
	resetTo(t, core, abi, 12000)
	poll(t, s)
	if rec.epochs != 1 {
		t.Fatalf("epoch change hook ran %d times, want 1", rec.epochs)
	}
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("reset broadcast %d blocks", len(got))
	}
	if s.GetHead() != 12000 {
		t.Fatalf("served head after reset %d, want 12000", s.GetHead())
	}
	if _, err := s.GetBlock(14000); !errors.Is(err, appendlog.ErrNotFound) {
		t.Fatalf("removed block 14000: err = %v", err)
	}
	if _, err := abiReader.Get(12025, 30000); err == nil {
		t.Fatal("ABI above the reset boundary was served")
	}

	w = openWriter(t, core, abi)
	poll(t, s)
	expectServed(t, s, 12000, Ready)
	w.write(t, 12001, 13000)
	w.commit(t)
	poll(t, s)
	expectServed(t, s, 13000, Ready)
	expectRange(t, rec.take(), 12001, 13000)
	if _, err := abiReader.Get(12025, 30000); err != nil {
		t.Fatalf("rebuilt ABI at block 12025: %v", err)
	}
	w.close()

	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	readers.Wait()
	if n := readFailures.Load(); n > 0 {
		t.Fatalf("%d reads of retained history failed; first: %v", n, firstFailure.Load())
	}
}
