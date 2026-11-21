package preflight

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

var chain = sha256.Sum256([]byte("test chain"))

func blockID(n uint32) [32]byte { return sha256.Sum256([]byte(fmt.Sprintf("block-%d", n))) }

func blob(n uint32) []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint32(b[0:4], n)
	id := blockID(n)
	copy(b[8:40], id[:])
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

// store writes blocks first to head and commits them, then writes three more blocks it never commits.
func store(t *testing.T, span, first, head uint32) (string, string) {
	t.Helper()
	core := filepath.Join(t.TempDir(), "core")
	abi := filepath.Join(core, "abis")
	pub, err := publish.Open(publish.Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, ChainID: chain, HeadID: headID})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Prepare(); err != nil {
		t.Fatal(err)
	}
	s, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: span})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetCommittedHead(0); err != nil {
		t.Fatal(err)
	}
	w, err := abicache.NewWriter(abi)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	write := func(from, to uint32) {
		var batch []appendlog.BlockEntry
		for n := from; n <= to; n++ {
			batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: blob(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
			if n%5 == 0 {
				if err := w.Write(n, uint64(n), []byte(fmt.Sprintf(`{"n":%d}`, n))); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := s.AppendBlockBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	write(first, head)
	if _, err := pub.Commit(s, w); err != nil {
		t.Fatal(err)
	}
	write(head+1, head+3)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	return core, abi
}

func snapshotAt(n uint32) *SnapshotInfo {
	id := blockID(n)
	return &SnapshotInfo{Version: 8, ChainID: hex.EncodeToString(chain[:]), HeadBlockID: hex.EncodeToString(id[:]), HeadBlockNum: n}
}

func direct(core, abi string, snap *SnapshotInfo) Options {
	return Options{Target: TargetDirect, CoreRoot: core, ABIRoot: abi, SliceSpan: DirectSliceSpan, HeadID: headID, Snapshot: snap}
}

func files(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			out[path], _ = os.ReadFile(path)
		}
		return err
	})
	return out
}

func check(t *testing.T, r *Report, name string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s check in %+v", name, r.Checks)
	return Check{}
}

func expect(t *testing.T, r *Report, result Result, name string, status Status) {
	t.Helper()
	if r.Result != result {
		t.Fatalf("result %s, want %s: blockers %v, unestablished %v", r.Result, result, r.Blockers, r.Unestablished)
	}
	if c := check(t, r, name); c.Status != status {
		t.Fatalf("%s check %s (%s), want %s", name, c.Status, c.Detail, status)
	}
}

func TestPreflightDirect(t *testing.T) {
	core, abi := store(t, DirectSliceSpan, 2, 10010)
	before := files(t, core)

	r := Run(direct(core, abi, snapshotAt(10000)))
	expect(t, r, ResultReady, "snapshot", Pass)
	if r.Boundary.Head != 10010 || r.Boundary.Epoch != 1 || r.Boundary.ChainID != hex.EncodeToString(chain[:]) {
		t.Fatalf("boundary %+v", r.Boundary)
	}
	if c := check(t, r, "block source"); c.Status != Unknown || !strings.Contains(c.Detail, "10001 to 10010") {
		t.Fatalf("block source check %+v", c)
	}
	expect(t, r, ResultReady, "head", Pass)

	expect(t, Run(direct(core, abi, snapshotAt(10010))), ResultReady, "snapshot", Pass)
	expect(t, Run(direct(core, abi, snapshotAt(10011))), ResultBlocked, "snapshot", Fail)

	wrong := snapshotAt(10000)
	other := blockID(1)
	wrong.HeadBlockID = hex.EncodeToString(other[:])
	expect(t, Run(direct(core, abi, wrong)), ResultBlocked, "snapshot", Fail)

	wrong = snapshotAt(10000)
	wrong.ChainID = hex.EncodeToString(other[:])
	expect(t, Run(direct(core, abi, wrong)), ResultBlocked, "chain", Fail)

	expect(t, Run(direct(core, abi, snapshotAt(1))), ResultReady, "snapshot", Unknown)
	expect(t, Run(direct(core, abi, nil)), ResultReady, "snapshot", Unknown)

	for path, b := range files(t, core) {
		if !bytes.Equal(b, before[path]) {
			t.Fatalf("preflight changed %s", path)
		}
	}
	if len(files(t, core)) != len(before) {
		t.Fatal("preflight created files")
	}
}

func TestPreflightBlockers(t *testing.T) {
	core, abi := store(t, 100, 2, 250)
	expect(t, Run(direct(core, abi, snapshotAt(200))), ResultBlocked, "geometry", Fail)

	o := direct(core, abi, nil)
	o.Target, o.SliceSpan = TargetTrace, 100
	o.TraceCovers = func(n uint32) (bool, error) { return n == 251, nil }
	expect(t, Run(o), ResultReady, "trace input", Pass)
	o.TraceCovers = func(uint32) (bool, error) { return false, nil }
	expect(t, Run(o), ResultBlocked, "trace input", Fail)
	o.TraceCovers = nil

	o.ChainID = blockID(7)
	expect(t, Run(o), ResultBlocked, "chain", Fail)
	o.ChainID = [32]byte{}

	o.SliceSpan = 200
	expect(t, Run(o), ResultBlocked, "geometry", Fail)
	o.SliceSpan = 100

	m, _ := coremanifest.ReadManifest(core)
	m.HeadID = blockID(1)
	coremanifest.WriteManifest(core, m)
	expect(t, Run(o), ResultBlocked, "head", Fail)

	m.HeadID = blockID(250)
	m.ABIDataEnd += 1000
	coremanifest.WriteManifest(core, m)
	expect(t, Run(o), ResultBlocked, "abi cache", Fail)

	o.ABIRoot = core
	expect(t, Run(o), ResultBlocked, "paths", Fail)
	o.ABIRoot = filepath.Join(core, "missing")
	expect(t, Run(o), ResultBlocked, "paths", Fail)
}

func TestPreflightStoreStates(t *testing.T) {
	core, abi := store(t, 100, 2, 250)
	o := direct(core, abi, nil)
	o.Target, o.SliceSpan = TargetTrace, 100

	pub, err := publish.Open(publish.Config{CoreRoot: core, ABIRoot: abi, SliceSpan: 100, HeadID: headID})
	if err != nil {
		t.Fatal(err)
	}
	r := Run(o)
	pub.Close()
	if !r.WriterActive || r.Result != ResultReady {
		t.Fatalf("with a writer running: active=%v result=%s %v", r.WriterActive, r.Result, r.Blockers)
	}
	if Run(o).WriterActive {
		t.Fatal("writer reported after it stopped")
	}

	for _, p := range []string{coremanifest.ManifestPath(core), coremanifest.BindingPath(core), coremanifest.BindingPath(abi)} {
		os.Remove(p)
	}
	r = Run(o)
	expect(t, r, ResultReady, "manifest", Pass)
	if !r.Boundary.Adoption || r.Boundary.Head != 253 {
		t.Fatalf("adoption boundary %+v", r.Boundary)
	}
	expect(t, r, ResultReady, "chain", Unknown)

	coremanifest.WriteBinding(core, &coremanifest.Binding{Role: coremanifest.RoleCore, SliceSpan: 100})
	expect(t, Run(o), ResultBlocked, "binding", Fail)
}

func TestPreflightRetriesWhileTheManifestMoves(t *testing.T) {
	core, abi := store(t, 100, 2, 250)
	o := direct(core, abi, nil)
	o.Target, o.SliceSpan, o.Attempts, o.RetryBackoff = TargetTrace, 100, 3, 1
	moves := 0
	bump := func(uint32) (bool, error) {
		m, _ := coremanifest.ReadManifest(core)
		m.Epoch++
		coremanifest.WriteManifest(core, m)
		moves++
		return true, nil
	}

	o.TraceCovers = bump
	r := Run(o)
	if r.Result != ResultInconclusive || r.Attempts != 3 || moves != 3 {
		t.Fatalf("result %s after %d attempts and %d moves", r.Result, r.Attempts, moves)
	}

	o.TraceCovers = func(n uint32) (bool, error) {
		if moves < 4 {
			return bump(n)
		}
		return true, nil
	}
	r = Run(o)
	if r.Result != ResultReady || r.Attempts != 2 {
		t.Fatalf("result %s after %d attempts", r.Result, r.Attempts)
	}
}
