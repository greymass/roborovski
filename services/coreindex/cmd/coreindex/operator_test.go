package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/greymass/roborovski/services/coreindex/internal/preflight"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

func testBlock(n uint32) []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint32(b[0:4], n)
	id := sha256.Sum256([]byte(fmt.Sprintf("block-%d", n)))
	copy(b[8:40], id[:])
	return b
}

// crashedStore commits blocks 2 to 350 in 100-block slices and leaves 351 to 353 written but uncommitted.
func crashedStore(t *testing.T) (string, string) {
	t.Helper()
	core := filepath.Join(t.TempDir(), "core")
	abi := filepath.Join(core, "abis")
	pub, err := publish.Open(publish.Config{CoreRoot: core, ABIRoot: abi, SliceSpan: 100, HeadID: blockIDFromBlob})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Prepare(); err != nil {
		t.Fatal(err)
	}
	s, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w, err := abicache.NewWriter(abi)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	write := func(from, to uint32) {
		var batch []appendlog.BlockEntry
		for n := from; n <= to; n++ {
			batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: testBlock(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
			if n%40 == 0 {
				if err := w.Write(n, uint64(n), []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := s.AppendBlockBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	write(2, 350)
	if _, err := pub.Commit(s, w); err != nil {
		t.Fatal(err)
	}
	write(351, 353)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	return core, abi
}

func run(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = t.TempDir()
	out, err := cmd.Output()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return string(out) + string(exit.Stderr), exit.ExitCode()
	case err != nil:
		t.Fatal(err)
	}
	return string(out), 0
}

func TestOperatorModes(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the coreindex binary")
	}
	bin := filepath.Join(t.TempDir(), "coreindex")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	core, _ := crashedStore(t)
	base := []string{"--index-storage", core, "--slice-size", "100"}
	traces := t.TempDir()

	out, code := run(t, bin, append(base, "--preflight", "--preflight-target", "trace", "--trace-dir", traces)...)
	var report preflight.Report
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("preflight printed something other than its report: %v\n%s", err, out)
	}
	if code != exitBlocked || report.Result != preflight.ResultBlocked || report.Boundary.Head != 350 || len(report.Blockers) != 1 {
		t.Fatalf("preflight exit %d: %s", code, out)
	}
	if _, code := run(t, bin, append(base, "--preflight", "--preflight-target", "direct")...); code != exitBlocked {
		t.Fatalf("direct preflight on 100-block slices exited %d", code)
	}

	headSlice := filepath.Join(core, "history_0000000301-0000000400", "data.log")
	before, _ := os.Stat(headSlice)
	if out, code := run(t, bin, append(base, "--recover")...); code != 0 {
		t.Fatalf("recover exit %d: %s", code, out)
	}
	m, _ := coremanifest.ReadManifest(core)
	if after, _ := os.Stat(headSlice); after.Size() != int64(m.SliceDataEnd) || after.Size() >= before.Size() {
		t.Fatalf("recover left data.log at %d bytes (was %d, manifest commits %d)", after.Size(), before.Size(), m.SliceDataEnd)
	}

	for _, args := range [][]string{
		{"--reset-after-block", "250"},
		{"--reset-after-block", "400"},
		{"--recover", "--reset-after-block", "200"},
		{"--recover", "--read-only"},
	} {
		if out, code := run(t, bin, append(base, args...)...); code == 0 {
			t.Fatalf("%v accepted: %s", args, out)
		}
	}
	if m2, _ := coremanifest.ReadManifest(core); *m2 != *m {
		t.Fatal("a refused mode changed the manifest")
	}

	if out, code := run(t, bin, append(base, "--reset-after-block", "200")...); code != 0 {
		t.Fatalf("reset exit %d: %s", code, out)
	}
	m, _ = coremanifest.ReadManifest(core)
	if m.Head != 200 || m.Epoch != 2 || m.ABIIndexLen != 5 {
		t.Fatalf("after reset: head %d, epoch %d, %d ABI entries", m.Head, m.Epoch, m.ABIIndexLen)
	}
	if _, err := os.Stat(filepath.Join(core, "history_0000000201-0000000300")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reset left slice 2")
	}

	out, code = run(t, bin, append(base, "--preflight", "--preflight-target", "trace", "--trace-dir", traces)...)
	if err := json.Unmarshal([]byte(out), &report); err != nil || report.Boundary.Head != 200 || report.Boundary.Epoch != 2 {
		t.Fatalf("preflight after reset (exit %d): %s", code, out)
	}
}
