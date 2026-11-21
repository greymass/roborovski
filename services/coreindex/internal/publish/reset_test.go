package publish

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/greymass/roborovski/libraries/coremanifest"
)

func cfgFor(core, abi string) Config {
	return Config{CoreRoot: core, ABIRoot: abi, SliceSpan: span, HeadID: headID}
}

// tree returns every file under root except the lock files, keyed by relative path.
func tree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Name() == coremanifest.LockName {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		b, err := os.ReadFile(path)
		out[rel] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameTree(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	var names []string
	for k := range want {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if g, ok := got[k]; !ok {
			t.Errorf("missing %s", k)
		} else if !bytes.Equal(g, want[k]) {
			t.Errorf("%s differs (%d bytes, want %d)", k, len(g), len(want[k]))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected file %s", k)
		}
	}
}

// crashedWriter leaves blocks 2 to 350 committed and 351 to 360 written but uncommitted.
func crashedWriter(t *testing.T) string {
	t.Helper()
	core, abi := roots(t)
	e := open(t, core, abi)
	e.write(t, 2, 350)
	e.commit(t)
	e.write(t, 351, 360)
	if err := e.store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := e.abiW.Sync(); err != nil {
		t.Fatal(err)
	}
	e.kill()
	return core
}

func cloneStore(t *testing.T, from string) (string, string) {
	t.Helper()
	to := filepath.Join(t.TempDir(), "core")
	copyTree(t, from, to)
	return to, filepath.Join(to, "abis")
}

func reset(t *testing.T, core, abi string, n uint32, hook func(ResetStep) error) (*ResetResult, error) {
	t.Helper()
	p, err := Open(cfgFor(core, abi))
	if err != nil {
		t.Fatal(err)
	}
	defer p.ReleaseLocks()
	return p.ResetAfter(n, hook)
}

func TestResetAfterBlock(t *testing.T) {
	crashed := crashedWriter(t)
	core, abi := cloneStore(t, crashed)
	res, err := reset(t, core, abi, 200, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resumed || res.Epoch != 2 || len(res.RemovedSlices) != 2 || res.ABIEntriesCut != 350/37-200/37 {
		t.Fatalf("result %+v", res)
	}
	m, err := coremanifest.ReadManifest(core)
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 2 || m.Head != 200 || m.HeadID != blockID(200) || m.ABIIndexLen != 200/37 {
		t.Fatalf("manifest %+v", m)
	}
	for _, s := range []uint32{2, 3} {
		if _, err := os.Stat(sliceDirOf(core, s)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("slice %d survived the reset", s)
		}
	}
	if st, _ := os.Stat(filepath.Join(abi, "abis.dat")); uint64(st.Size()) != m.ABIDataEnd {
		t.Fatalf("abis.dat holds %d bytes, manifest commits %d", st.Size(), m.ABIDataEnd)
	}

	again, err := reset(t, core, abi, 200, nil)
	if err != nil || !again.Resumed || again.Epoch != 2 {
		t.Fatalf("repeat reset: %+v, %v", again, err)
	}

	e := open(t, core, abi)
	e.write(t, 201, 350)
	e.commit(t)
	e.checkBlocks(t, 350)
	e.close(t)
	compareFiles(t, storeFiles(t, core), reference(t, 350))
	if m, _ := coremanifest.ReadManifest(core); m.Epoch != 2 {
		t.Fatalf("rebuild moved the epoch to %d", m.Epoch)
	}
}

func sliceDirOf(core string, n uint32) string {
	return filepath.Join(core, sliceName(n))
}

func sliceName(n uint32) string {
	start := n*span + 1
	return fmt.Sprintf("history_%010d-%010d", start, start+span-1)
}

func TestResetInterruptedAtEachStep(t *testing.T) {
	crashed := crashedWriter(t)
	core, abi := cloneStore(t, crashed)
	if _, err := reset(t, core, abi, 200, nil); err != nil {
		t.Fatal(err)
	}
	want := tree(t, core)

	for _, step := range []ResetStep{ResetManifestWritten, ResetSlicesRemoved, ResetABIIndexReplaced} {
		for _, finish := range []string{"recover", "reset"} {
			core, abi := cloneStore(t, crashed)
			_, err := reset(t, core, abi, 200, func(s ResetStep) error {
				if s == step {
					return errKilled
				}
				return nil
			})
			if !errors.Is(err, errKilled) {
				t.Fatalf("step %d: %v", step, err)
			}
			if m, _ := coremanifest.ReadManifest(core); m.Epoch != 2 || m.Head != 200 {
				t.Fatalf("step %d: interrupted reset left manifest at block %d, epoch %d", step, m.Head, m.Epoch)
			}
			if reflect.DeepEqual(tree(t, core), want) {
				t.Fatalf("step %d: the interrupted store already matches a finished reset", step)
			}
			if finish == "recover" {
				m, adopted, err := Recover(cfgFor(core, abi))
				if err != nil || adopted || m.Head != 200 || m.Epoch != 2 {
					t.Fatalf("step %d: recover: %+v adopted=%v %v", step, m, adopted, err)
				}
			} else if res, err := reset(t, core, abi, 200, nil); err != nil || !res.Resumed {
				t.Fatalf("step %d: finishing reset: %+v %v", step, res, err)
			}
			t.Run(fmt.Sprintf("step %d then %s", step, finish), func(t *testing.T) { sameTree(t, tree(t, core), want) })
		}
	}

	core, abi = cloneStore(t, crashed)
	reset(t, core, abi, 200, func(s ResetStep) error {
		if s == ResetManifestWritten {
			return errKilled
		}
		return nil
	})
	if err := os.RemoveAll(sliceDirOf(core, 3)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Recover(cfgFor(core, abi)); err != nil {
		t.Fatal(err)
	}
	t.Run("partial slice removal", func(t *testing.T) { sameTree(t, tree(t, core), want) })
}

func TestResetRefusals(t *testing.T) {
	crashed := crashedWriter(t)
	core, abi := cloneStore(t, crashed)
	before := tree(t, core)
	for _, n := range []uint32{0, 250, 400} {
		if _, err := reset(t, core, abi, n, nil); err == nil {
			t.Fatalf("reset to %d accepted", n)
		}
	}
	sameTree(t, tree(t, core), before)

	writer, err := Open(cfgFor(core, abi))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfgFor(core, abi)); !errors.Is(err, coremanifest.ErrLocked) {
		t.Fatalf("reset ran beside a writer: %v", err)
	}
	if _, _, err := Recover(cfgFor(core, abi)); !errors.Is(err, coremanifest.ErrLocked) {
		t.Fatalf("recover ran beside a writer: %v", err)
	}
	writer.Close()

	m, _ := coremanifest.ReadManifest(core)
	m.ABIFloor = 300
	if err := coremanifest.WriteManifest(core, m); err != nil {
		t.Fatal(err)
	}
	if _, err := reset(t, core, abi, 200, nil); err == nil {
		t.Fatal("reset below the ABI floor accepted")
	}
}

func TestRecover(t *testing.T) {
	crashed := crashedWriter(t)
	core, abi := cloneStore(t, crashed)
	m, adopted, err := Recover(cfgFor(core, abi))
	if err != nil || adopted || m.Head != 350 {
		t.Fatalf("recover: %+v adopted=%v %v", m, adopted, err)
	}
	compareFiles(t, storeFiles(t, core), reference(t, 350))

	for _, p := range []string{coremanifest.ManifestPath(core), coremanifest.BindingPath(core), coremanifest.BindingPath(abi)} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	m, adopted, err = Recover(cfgFor(core, abi))
	if err != nil || !adopted || m.Head != 350 || m.Epoch != 1 {
		t.Fatalf("recover of a pre-protocol store: %+v adopted=%v %v", m, adopted, err)
	}

	short, shortABI := cloneStore(t, crashed)
	data := filepath.Join(sliceDirOf(short, 3), "data.log")
	st, _ := os.Stat(data)
	committed, _ := coremanifest.ReadManifest(short)
	if err := os.Truncate(data, int64(committed.SliceDataEnd)-3); err != nil {
		t.Fatal(err)
	}
	before := tree(t, short)
	if _, _, err := Recover(cfgFor(short, shortABI)); err == nil {
		t.Fatalf("recover accepted a store shorter than its manifest (%d bytes)", st.Size())
	}
	sameTree(t, tree(t, short), before)
}
