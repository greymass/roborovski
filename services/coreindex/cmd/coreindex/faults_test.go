package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/greymass/roborovski/services/coreindex/internal/fault"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

const (
	helperEnv     = "COREINDEX_FAULT_HELPER"
	helperSpan    = 10000
	helperCommit  = 250
	helperFirst   = 2
	helperABIStep = 400
)

// TestFaultHelperWriter is the child process: a trace-mode writer armed through armWriterFaults.
func TestFaultHelperWriter(t *testing.T) {
	spec := os.Getenv(helperEnv)
	if spec == "" {
		t.Skip("runs only as the child of the fault tests")
	}
	parts := strings.Split(spec, ",")
	core, abi := parts[0], parts[1]
	target, _ := strconv.Atoi(parts[2])
	if err := fault.Configure(); err != nil {
		t.Fatal(err)
	}
	pub, err := publish.Open(publish.Config{CoreRoot: core, ABIRoot: abi, SliceSpan: helperSpan, HeadID: blockIDFromBlob})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Prepare(); err != nil {
		t.Fatal(err)
	}
	head := pub.Manifest().Head
	s, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: helperSpan})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetCommittedHead(head); err != nil {
		t.Fatal(err)
	}
	w, err := abicache.NewWriter(abi)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	armWriterFaults(s, pub)

	next := head + 1
	if head == 0 {
		next = helperFirst
	}
	for next <= uint32(target) {
		end := next + helperCommit - 1 - (next-1)%helperCommit
		if end > uint32(target) {
			end = uint32(target)
		}
		var batch []appendlog.BlockEntry
		for n := next; n <= end; n++ {
			batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: testBlock(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
			if n%helperABIStep == 0 {
				if err := w.Write(n, uint64(n), []byte(`{"block":`+strconv.Itoa(int(n))+`}`)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := s.AppendBlockBatch(batch); err != nil {
			t.Fatal(err)
		}
		if _, err := pub.Commit(s, w); err != nil {
			t.Fatal(err)
		}
		next = end + 1
	}
}

type helperRun struct {
	killed bool
	output string
}

func runHelper(t *testing.T, core, abi string, target int, faultSpec string) helperRun {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFaultHelperWriter$", "-test.v")
	cmd.Env = append(os.Environ(), helperEnv+"="+core+","+abi+","+strconv.Itoa(target), fault.EnvVar+"="+faultSpec)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
			return helperRun{killed: true, output: string(out)}
		}
		t.Fatalf("writer failed: %v\n%s", err, out)
	}
	if err != nil {
		t.Fatal(err)
	}
	return helperRun{output: string(out)}
}

type committedView struct {
	manifest coremanifest.Manifest
	served   uint32
	blocks   map[uint32][]byte
	abiData  []byte
	abiIndex []byte
}

// readCommitted reads, through corereader, every block control/published exposes, plus the committed ABI prefix.
func readCommitted(t *testing.T, r *corereader.SliceReader, core, abi string) committedView {
	t.Helper()
	m, err := coremanifest.ReadManifest(core)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := coremanifest.ReadPublished(core)
	if err != nil {
		t.Fatal(err)
	}
	if pub.Head > m.Head {
		t.Fatalf("published head %d is past the committed head %d", pub.Head, m.Head)
	}
	v := committedView{manifest: *m, served: pub.Head, blocks: map[uint32][]byte{}}
	for n := uint32(helperFirst); n <= pub.Head; n++ {
		b, err := r.GetBlockBytes(n)
		if err != nil {
			t.Fatalf("committed block %d: %v", n, err)
		}
		v.blocks[n] = append([]byte(nil), b...)
	}
	if _, err := r.GetBlockBytes(pub.Head + 1); err == nil {
		t.Fatalf("reader served block %d above the published head %d", pub.Head+1, pub.Head)
	}
	data, _ := os.ReadFile(filepath.Join(abi, "abis.dat"))
	index, _ := os.ReadFile(filepath.Join(abi, "abis.index"))
	v.abiData = append([]byte(nil), data[:m.ABIDataEnd]...)
	v.abiIndex = append([]byte(nil), index[:int(m.ABIIndexLen)*20]...)
	return v
}

func TestWriterCrashAtEachFaultPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the writer in child processes")
	}
	const target = 10450
	for _, tc := range []struct {
		point string
		from  uint32
	}{
		{"abi_synced", 9800},
		{"core_synced", 9800},
		{"manifest_dir_synced", 9800},
		{"published_renamed", 9800},
		{"slice_rotated", 9800},
	} {
		t.Run(tc.point, func(t *testing.T) {
			core := filepath.Join(t.TempDir(), "core")
			abi := filepath.Join(core, "abis")
			if err := os.MkdirAll(abi, 0755); err != nil {
				t.Fatal(err)
			}
			run := runHelper(t, core, abi, target, tc.point+":kill:"+strconv.Itoa(int(tc.from)))
			if !run.killed || !strings.Contains(run.output, "reached "+tc.point) {
				t.Fatalf("writer was not killed at %s:\n%s", tc.point, run.output)
			}

			r, err := corereader.NewSliceReader(core)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			before := readCommitted(t, r, core, abi)
			if before.manifest.Head < helperFirst || before.manifest.Head >= target {
				t.Fatalf("kill left committed head %d", before.manifest.Head)
			}
			t.Logf("killed at %s with block %d committed", tc.point, before.manifest.Head)

			var stop atomic.Bool
			var wg sync.WaitGroup
			var regressed atomic.Value
			wg.Add(1)
			go func() {
				defer wg.Done()
				last := before.served
				for !stop.Load() {
					head, _, err := r.GetStateProps(true)
					if err == nil {
						if head < last {
							regressed.Store([2]uint32{last, head})
							return
						}
						last = head
					}
					time.Sleep(time.Millisecond)
				}
			}()
			if again := runHelper(t, core, abi, target, ""); again.killed {
				t.Fatalf("restart was killed:\n%s", again.output)
			}
			stop.Store(true)
			wg.Wait()
			if v := regressed.Load(); v != nil {
				p := v.([2]uint32)
				t.Fatalf("reader opened before recovery saw the head go from %d back to %d", p[0], p[1])
			}

			fresh, err := corereader.NewSliceReader(core)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			after := readCommitted(t, fresh, core, abi)
			if after.manifest.Head != target || after.served != target {
				t.Fatalf("restart stopped at committed %d, published %d, want %d", after.manifest.Head, after.served, target)
			}
			for n := uint32(helperFirst); n <= target; n++ {
				if n <= before.served && !bytes.Equal(after.blocks[n], before.blocks[n]) {
					t.Fatalf("committed block %d changed across the crash", n)
				}
				if !bytes.Equal(after.blocks[n], testBlock(n)) {
					t.Fatalf("block %d after recovery is not the block written", n)
				}
			}
			if !bytes.HasPrefix(after.abiData, before.abiData) || !bytes.HasPrefix(after.abiIndex, before.abiIndex) {
				t.Fatal("committed ABI cache bytes changed across the crash")
			}
			if want := (target / helperABIStep) * 20; len(after.abiIndex) != want {
				t.Fatalf("ABI index holds %d bytes after recovery, want %d (one entry per %d blocks, no duplicates)", len(after.abiIndex), want, helperABIStep)
			}
		})
	}
}

func buildCoreindex(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "coreindex")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func runKillable(t *testing.T, bin string, env []string, args ...string) (killed bool, code int, out string) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), env...)
	b, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
			return true, -1, string(b)
		}
		return false, exit.ExitCode(), string(b)
	}
	if err != nil {
		t.Fatal(err)
	}
	return false, 0, string(b)
}

// treeDigest maps every file under root to its contents, leaving out lock files and reader caches.
func treeDigest(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		name := info.Name()
		if name == coremanifest.LockName || strings.HasSuffix(name, ".cache") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		out[rel] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	if out, err := exec.Command("cp", "-R", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("copy %s: %v\n%s", src, err, out)
	}
}

// checkRetained asserts the reader serves the retained prefix unchanged and nothing above the boundary.
func checkRetained(t *testing.T, r *corereader.SliceReader, boundary, removed uint32, stage string) {
	t.Helper()
	head, _, err := r.GetStateProps(true)
	if err != nil || head != boundary {
		t.Fatalf("%s: reader head %d (%v), want %d", stage, head, err, boundary)
	}
	for n := uint32(helperFirst); n <= boundary; n += 997 {
		b, err := r.GetBlockBytes(n)
		if err != nil || !bytes.Equal(b, testBlock(n)) {
			t.Fatalf("%s: retained block %d unreadable or changed (%v)", stage, n, err)
		}
	}
	if b, err := r.GetBlockBytes(boundary); err != nil || !bytes.Equal(b, testBlock(boundary)) {
		t.Fatalf("%s: boundary block %d unreadable or changed (%v)", stage, boundary, err)
	}
	if _, err := r.GetBlockBytes(removed); err == nil {
		t.Fatalf("%s: reader served removed block %d", stage, removed)
	}
}

func TestResetKilledAtEachStep(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the coreindex binary and runs writers in child processes")
	}
	const (
		head     = 25000
		boundary = 10000
		removed  = 15000
		rebuild  = 12000
	)
	bin := buildCoreindex(t)
	template := filepath.Join(t.TempDir(), "core")
	if err := os.MkdirAll(filepath.Join(template, "abis"), 0755); err != nil {
		t.Fatal(err)
	}
	if run := runHelper(t, template, filepath.Join(template, "abis"), head, ""); run.killed {
		t.Fatal("template writer was killed")
	}
	args := func(core string, extra ...string) []string {
		return append([]string{"--index-storage", core, "--slice-size", strconv.Itoa(helperSpan)}, extra...)
	}

	want := filepath.Join(t.TempDir(), "core")
	copyTree(t, template, want)
	if killed, code, out := runKillable(t, bin, nil, args(want, "--reset-after-block", strconv.Itoa(boundary))...); killed || code != 0 {
		t.Fatalf("uninterrupted reset (exit %d): %s", code, out)
	}
	wantTree := treeDigest(t, want)
	if m, _ := coremanifest.ReadManifest(want); m.Head != boundary || m.Epoch != 2 {
		t.Fatalf("uninterrupted reset left head %d epoch %d", m.Head, m.Epoch)
	}

	for _, point := range []string{"reset_manifest_written", "reset_published_written", "reset_slices_removed", "reset_abi_index_replaced"} {
		for _, finish := range []string{"--recover", "--reset-after-block"} {
			t.Run(point+"/"+strings.TrimPrefix(finish, "--"), func(t *testing.T) {
				core := filepath.Join(t.TempDir(), "core")
				copyTree(t, template, core)
				r, err := corereader.NewSliceReader(core)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if b, err := r.GetBlockBytes(removed); err != nil || !bytes.Equal(b, testBlock(removed)) {
					t.Fatalf("block %d unreadable before the reset: %v", removed, err)
				}

				killed, code, out := runKillable(t, bin, []string{fault.EnvVar + "=" + point + ":kill"}, args(core, "--reset-after-block", strconv.Itoa(boundary))...)
				if !killed || !strings.Contains(out, "reached "+point) {
					t.Fatalf("reset was not killed at %s (exit %d): %s", point, code, out)
				}
				if point == "reset_manifest_written" {
					if h, _, err := r.GetStateProps(true); err != nil || h != head {
						t.Fatalf("after the kill: reader head %d (%v), want the unreset %d", h, err, head)
					}
					if b, err := r.GetBlockBytes(removed); err != nil || !bytes.Equal(b, testBlock(removed)) {
						t.Fatalf("after the kill: block %d left the unreset view (%v)", removed, err)
					}
				} else {
					checkRetained(t, r, boundary, removed, "after the kill")
				}

				extra := []string{finish}
				if finish == "--reset-after-block" {
					extra = append(extra, strconv.Itoa(boundary))
				}
				if killed, code, out := runKillable(t, bin, nil, args(core, extra...)...); killed || code != 0 {
					t.Fatalf("%s after the kill (exit %d): %s", finish, code, out)
				}
				got := treeDigest(t, core)
				for name, data := range wantTree {
					if got[name] != data {
						t.Fatalf("%s differs from an uninterrupted reset", name)
					}
				}
				for name := range got {
					if _, ok := wantTree[name]; !ok {
						t.Fatalf("%s is left over that an uninterrupted reset removes", name)
					}
				}
				checkRetained(t, r, boundary, removed, "after "+finish)

				if run := runHelper(t, core, filepath.Join(core, "abis"), rebuild, ""); run.killed {
					t.Fatal("rebuild writer was killed")
				}
				if _, _, err := r.GetStateProps(true); err != nil {
					t.Fatal(err)
				}
				if b, err := r.GetBlockBytes(rebuild); err != nil || !bytes.Equal(b, testBlock(rebuild)) {
					t.Fatalf("rebuilt block %d not served: %v", rebuild, err)
				}
			})
		}
	}
}
