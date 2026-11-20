package publish

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

const span = 100

var errKilled = errors.New("killed")

func blockID(n uint32) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("block-%d", n)))
}

func blob(n uint32) []byte {
	b := make([]byte, 40+int(n%17)*11)
	binary.LittleEndian.PutUint32(b[0:4], n)
	binary.LittleEndian.PutUint32(b[4:8], n*2)
	id := blockID(n)
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

type env struct {
	core, abi string
	pub       *Publisher
	store     *appendlog.SliceStore
	abiW      *abicache.Writer
}

func roots(t *testing.T) (string, string) {
	dir := t.TempDir()
	return filepath.Join(dir, "core"), filepath.Join(dir, "core", "abis")
}

func open(t *testing.T, core, abi string) *env {
	t.Helper()
	pub, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID})
	if err != nil {
		t.Fatalf("open: %v", err)
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
	return &env{core: core, abi: abi, pub: pub, store: store, abiW: w}
}

// kill abandons the process state: buffered writes are lost, only the locks are released.
func (e *env) kill() { e.pub.ReleaseLocks() }

func (e *env) close(t *testing.T) {
	t.Helper()
	if err := e.store.Close(); err != nil {
		t.Fatal(err)
	}
	e.abiW.Close()
	e.pub.Close()
}

func (e *env) write(t *testing.T, from, to uint32) {
	t.Helper()
	var batch []appendlog.BlockEntry
	for n := from; n <= to; n++ {
		batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: blob(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
		if n%37 == 0 {
			if err := e.abiW.Write(n, uint64(n), []byte(fmt.Sprintf(`{"version":"eosio::abi/1.1","n":%d}`, n))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.store.AppendBlockBatch(batch); err != nil {
		t.Fatal(err)
	}
}

func (e *env) commit(t *testing.T) {
	t.Helper()
	if _, err := e.pub.Commit(e.store, e.abiW); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func (e *env) checkBlocks(t *testing.T, head uint32) {
	t.Helper()
	if got := e.pub.Manifest().Head; got != head {
		t.Fatalf("committed head %d, want %d", got, head)
	}
	if got := e.store.GetHead(); got != head {
		t.Fatalf("store head %d, want %d", got, head)
	}
	for n := uint32(2); n <= head; n++ {
		got, err := e.store.GetBlock(n)
		if err != nil {
			t.Fatalf("block %d: %v", n, err)
		}
		if !bytes.Equal(got, blob(n)) {
			t.Fatalf("block %d differs", n)
		}
	}
	if m := e.pub.Manifest(); m.HeadID != blockID(head) {
		t.Fatalf("manifest head ID is not block %d's", head)
	}
	if want := uint32(head / 37); e.pub.Manifest().ABIIndexLen != want {
		t.Fatalf("manifest commits %d ABI entries, want %d", e.pub.Manifest().ABIIndexLen, want)
	}
}

// storeFiles returns every history and ABI file's bytes, keyed by path relative to the core root.
func storeFiles(t *testing.T, core string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.Walk(core, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		base := filepath.Base(path)
		if info.IsDir() || !(base == "data.log" || base == "blocks.index" || base == "abis.dat" || base == "abis.index") {
			return nil
		}
		rel, _ := filepath.Rel(core, path)
		b, err := os.ReadFile(path)
		out[rel] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func compareFiles(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	var names []string
	for k := range want {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if !bytes.Equal(got[k], want[k]) {
			at := 0
			for at < len(got[k]) && at < len(want[k]) && got[k][at] == want[k][at] {
				at++
			}
			t.Errorf("%s differs from a run with no crash at byte %d (%d bytes, want %d)", k, at, len(got[k]), len(want[k]))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected file %s", k)
		}
	}
}

func reference(t *testing.T, to uint32) map[string][]byte {
	core, abi := roots(t)
	e := open(t, core, abi)
	e.write(t, 2, to)
	e.commit(t)
	e.close(t)
	return storeFiles(t, core)
}

func TestCommitOrder(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	defer e.close(t)
	e.write(t, 2, 90)
	e.commit(t)
	e.write(t, 91, 120)

	var seen []Step
	e.pub.FaultHook = func(s Step) error {
		seen = append(seen, s)
		m, err := coremanifest.ReadManifest(core)
		if err != nil {
			return err
		}
		switch s {
		case StepABISynced, StepCoreSynced:
			if m.Head != 90 {
				t.Errorf("manifest moved to %d before step %d finished", m.Head, s)
			}
		case StepManifestWritten:
			if m.Head != 120 {
				t.Errorf("manifest head %d after commit, want 120", m.Head)
			}
		}
		return nil
	}
	e.commit(t)
	if want := []Step{StepABISynced, StepCoreSynced, StepManifestWritten}; fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("steps %v, want %v", seen, want)
	}
	e.pub.FaultHook = nil
	if changed, err := e.pub.Commit(e.store, e.abiW); err != nil || changed {
		t.Fatalf("empty commit: changed=%v err=%v", changed, err)
	}
}

func TestKillAtEachStep(t *testing.T) {
	want := reference(t, 300)
	cases := []struct {
		name string
		step Step
		head uint32
	}{
		{"before commit", 0, 90},
		{"after ABI sync", StepABISynced, 90},
		{"after core sync", StepCoreSynced, 90},
		{"after manifest", StepManifestWritten, 260},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			core, abi := roots(t)
			e := open(t, core, abi)
			e.write(t, 2, 90)
			e.commit(t)
			e.write(t, 91, 260)
			if err := e.store.Sync(); err != nil {
				t.Fatal(err)
			}
			if c.step != 0 {
				e.pub.FaultHook = func(s Step) error {
					if s == c.step {
						return errKilled
					}
					return nil
				}
				if _, err := e.pub.Commit(e.store, e.abiW); !errors.Is(err, errKilled) {
					t.Fatalf("commit returned %v, want the injected kill", err)
				}
			}
			e.kill()

			r := open(t, core, abi)
			r.checkBlocks(t, c.head)
			r.write(t, c.head+1, 300)
			r.commit(t)
			r.close(t)

			compareFiles(t, storeFiles(t, core), want)
			final := open(t, core, abi)
			final.checkBlocks(t, 300)
			final.close(t)
		})
	}
}

func TestRecoveryResetsFinalizedSlice(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	e.write(t, 2, 90)
	e.commit(t)
	e.write(t, 91, 130)
	if err := e.store.Sync(); err != nil {
		t.Fatal(err)
	}
	e.kill()

	hdr, err := appendlog.ReadDataLogHeader(filepath.Join(core, "history_0000000001-0000000100", "data.log"))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Finalized != 1 {
		t.Fatal("rotation did not finalize slice 0; the test needs it finalized before recovery")
	}

	r := open(t, core, abi)
	defer r.close(t)
	hdr, err = appendlog.ReadDataLogHeader(filepath.Join(core, "history_0000000001-0000000100", "data.log"))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Finalized != 0 {
		t.Fatal("recovery left slice 0 finalized below its last block")
	}
	if _, err := os.Stat(filepath.Join(core, "history_0000000101-0000000200")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("uncommitted slice 1 survived recovery")
	}
	r.checkBlocks(t, 90)
}

func TestRecoveryRefusesMissingCommittedBytes(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	e.write(t, 2, 150)
	e.commit(t)
	e.close(t)

	data := filepath.Join(core, "history_0000000101-0000000200", "data.log")
	st, _ := os.Stat(data)
	if err := os.Truncate(data, st.Size()-5); err != nil {
		t.Fatal(err)
	}
	pub, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Prepare(); err == nil {
		t.Fatal("recovery accepted a store missing committed bytes")
	}
}

func TestSecondWriterRefused(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	defer e.close(t)
	if _, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID}); !errors.Is(err, coremanifest.ErrLocked) {
		t.Fatalf("second writer got %v, want ErrLocked", err)
	}
	other := filepath.Join(t.TempDir(), "elsewhere")
	if _, err := Open(Config{CoreRoot: other, ABIRoot: abi, SliceSpan: span, HeadID: headID}); !errors.Is(err, coremanifest.ErrLocked) {
		t.Fatalf("writer sharing only the ABI root got %v, want ErrLocked", err)
	}
	if held, err := coremanifest.WriterLockHeld(core); err != nil || !held {
		t.Fatalf("lock probe: held=%v err=%v", held, err)
	}
}

func TestBindingChecks(t *testing.T) {
	core, abi := roots(t)
	open(t, core, abi).close(t)
	coreB, abiB := roots(t)
	open(t, coreB, abiB).close(t)

	if _, err := Open(Config{CoreRoot: core, ABIRoot: abiB, SliceSpan: span, HeadID: headID}); !errors.Is(err, coremanifest.ErrBindingMismatch) {
		t.Fatalf("ABI cache of another store accepted: %v", err)
	}
	if _, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: 10000, HeadID: headID}); err == nil {
		t.Fatal("different slice span accepted")
	}
	chain := [32]byte{1}
	p, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID, ChainID: chain})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Prepare(); err != nil {
		t.Fatal(err)
	}
	p.Close()
	for _, root := range []string{core, abi} {
		if b, _ := coremanifest.ReadBinding(root); b.ChainID != chain {
			t.Fatalf("chain ID was not recorded in %s once known", root)
		}
	}
	if _, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID, ChainID: [32]byte{2}}); !errors.Is(err, coremanifest.ErrBindingMismatch) {
		t.Fatalf("other chain accepted: %v", err)
	}
}

// legacyStore writes a store the way coreindex did before the publication protocol.
func legacyStore(t *testing.T, core, abi string, to uint32) {
	t.Helper()
	store, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: span})
	if err != nil {
		t.Fatal(err)
	}
	w, err := abicache.NewWriter(abi)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{store: store, abiW: w}
	e.write(t, 2, to)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	w.Close()
}

func TestAdoptExistingStore(t *testing.T) {
	core, abi := roots(t)
	legacyStore(t, core, abi, 250)
	before := storeFiles(t, core)

	tail := filepath.Join(core, "history_0000000201-0000000300", "data.log")
	f, err := os.OpenFile(tail, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{9, 9, 9})
	f.Close()

	e := open(t, core, abi)
	m := e.pub.Manifest()
	if m.Head != 250 || m.Epoch != 1 {
		t.Fatalf("adopted manifest head=%d epoch=%d", m.Head, m.Epoch)
	}
	for _, root := range []string{core, abi} {
		if _, err := coremanifest.ReadBinding(root); err != nil {
			t.Fatalf("binding in %s: %v", root, err)
		}
	}
	e.close(t)
	compareFiles(t, storeFiles(t, core), before)

	again := open(t, core, abi)
	again.checkBlocks(t, 250)
	again.write(t, 251, 300)
	again.commit(t)
	again.close(t)
	compareFiles(t, storeFiles(t, core), reference(t, 300))
}

func TestAdoptionRefusesDamagedTail(t *testing.T) {
	core, abi := roots(t)
	legacyStore(t, core, abi, 150)
	tail := filepath.Join(core, "history_0000000101-0000000200", "data.log")
	b, err := os.ReadFile(tail)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-6] ^= 0xff
	if err := os.WriteFile(tail, b, 0644); err != nil {
		t.Fatal(err)
	}
	before := storeFiles(t, core)

	p, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Prepare(); err == nil {
		t.Fatal("adoption accepted a tail block that fails its CRC")
	}
	if _, err := coremanifest.ReadManifest(core); !errors.Is(err, coremanifest.ErrNoManifest) {
		t.Fatal("refused adoption still wrote a manifest")
	}
	compareFiles(t, storeFiles(t, core), before)
}

func TestEraseKeepsOwnership(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	e.write(t, 2, 150)
	e.commit(t)
	e.close(t)

	p, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID, Erase: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Prepare(); err != nil {
		t.Fatal(err)
	}
	if m := p.Manifest(); m.Head != 0 {
		t.Fatalf("erased store has head %d", m.Head)
	}
	if files := storeFiles(t, core); len(files) != 0 {
		t.Fatalf("erase left history files: %v", len(files))
	}
	if held, _ := coremanifest.WriterLockHeld(core); !held {
		t.Fatal("erase released the core lock")
	}
}

func TestReadersSeeOnlyCommittedBlocks(t *testing.T) {
	core, abi := roots(t)
	e := open(t, core, abi)
	defer e.close(t)
	e.write(t, 2, 90)
	e.commit(t)
	e.write(t, 91, 120)
	if h := e.store.GetHead(); h != 90 {
		t.Fatalf("readers see head %d before commit, want 90", h)
	}
	if _, err := e.store.GetBlock(100); err == nil {
		t.Fatal("uncommitted block 100 is readable")
	}
	if _, err := e.store.GetBlockByGlob(1000); err == nil {
		t.Fatal("uncommitted block 100 is reachable by global sequence")
	}
	e.commit(t)
	if h := e.store.GetHead(); h != 120 {
		t.Fatalf("readers see head %d after commit, want 120", h)
	}
	if n, err := e.store.GetBlockByGlob(1000); err != nil || n != 100 {
		t.Fatalf("committed block 100 by global sequence: %d, %v", n, err)
	}
}
