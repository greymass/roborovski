package publish

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

// The plugin's Boost tests recover these stores and must reach byte-identical results.
var fixturesOut = flag.String("recovery-fixtures", "", "write the shared crashed-store fixtures to this directory")

const fixtureSpan = 10000

type fixtureExpect struct {
	Outcome string `json:"outcome"`
	Adopted bool   `json:"adopted"`
	Head    uint32 `json:"head"`
}

func openSpan(t *testing.T, core, abi string) *env {
	t.Helper()
	pub, err := Open(Config{CoreRoot: core, ABIRoot: abi, SliceSpan: fixtureSpan, HeadID: headID})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pub.Prepare(); err != nil {
		pub.Close()
		t.Fatalf("prepare: %v", err)
	}
	store, err := appendlog.NewSliceStore(core, appendlog.SliceStoreOptions{BlocksPerSlice: fixtureSpan})
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

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.Walk(from, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		dst := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func writeExpect(t *testing.T, dir string, e fixtureExpect) {
	t.Helper()
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "expect.json"), append(b, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

// maskedFile zeroes the store UUIDs and checksum, which differ on every adoption.
func maskedFile(rel string, b []byte) ([]byte, bool) {
	out := append([]byte(nil), b...)
	switch filepath.Base(rel) {
	case coremanifest.ManifestName, coremanifest.PublishedName:
		if len(out) == coremanifest.ManifestSize {
			clear(out[16:48])
			clear(out[124:128])
			return out, true
		}
	case coremanifest.BindingName:
		if len(out) == coremanifest.BindingSize {
			clear(out[48:80])
			clear(out[92:96])
			return out, true
		}
	}
	return out, false
}

// writeDigests lists every file under root as "<sha256> <size> <masked> <path>", sorted by path.
func writeDigests(t *testing.T, root, out string, mask bool) {
	t.Helper()
	var lines []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		masked := false
		if mask {
			b, masked = maskedFile(rel, b)
		}
		m := 0
		if masked {
			m = 1
		}
		lines = append(lines, fmt.Sprintf("%x %d %d %s", sha256.Sum256(b), info.Size(), m, filepath.ToSlash(rel)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(lines)
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l + "\n")
	}
	if err := os.WriteFile(out, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

// emit records the crashed store as before/ and the digests of a recovered copy as after.sha256.
func emit(t *testing.T, name, crashed string) {
	t.Helper()
	dir := filepath.Join(*fixturesOut, name)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	copyTree(t, crashed, filepath.Join(dir, "before", "core"))
	work := filepath.Join(t.TempDir(), "core")
	copyTree(t, crashed, work)

	m, _ := coremanifest.ReadManifest(work)
	expect := fixtureExpect{Outcome: "recovered", Adopted: m == nil}
	pub, err := Open(Config{CoreRoot: work, ABIRoot: filepath.Join(work, "abis"), SliceSpan: fixtureSpan, HeadID: headID})
	if err == nil {
		err = pub.Prepare()
		if err == nil {
			expect.Head = pub.Manifest().Head
		}
		pub.Close()
	}
	if err != nil {
		expect.Outcome = "refused"
		expect.Adopted = false
	}
	writeDigests(t, work, filepath.Join(dir, "after.sha256"), expect.Adopted)
	writeExpect(t, dir, expect)
	fmt.Printf("fixture %s: %s at block %d\n", name, expect.Outcome, expect.Head)
}

func fixtureBlob(n uint32) []byte { return blob(n)[:40] }

func writeFixtureBlocks(t *testing.T, e *env, from, to uint32) {
	t.Helper()
	var batch []appendlog.BlockEntry
	for n := from; n <= to; n++ {
		batch = append(batch, appendlog.BlockEntry{BlockNum: n, Data: fixtureBlob(n), GlobMin: uint64(n) * 10, GlobMax: uint64(n)*10 + 9})
		if n%3 == 0 && n > 9980 {
			if err := e.abiW.Write(n, uint64(n), []byte(fmt.Sprintf(`{"version":"eosio::abi/1.1","n":%d}`, n))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.store.AppendBlockBatch(batch); err != nil {
		t.Fatal(err)
	}
}

func crashedStore(t *testing.T, first, committedTo, writtenTo uint32) string {
	t.Helper()
	core, abi := roots(t)
	e := openSpan(t, core, abi)
	writeFixtureBlocks(t, e, first, committedTo)
	e.commit(t)
	if writtenTo > committedTo {
		writeFixtureBlocks(t, e, committedTo+1, writtenTo)
		if err := e.store.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := e.abiW.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	e.kill()
	return core
}

func TestWriteRecoveryFixtures(t *testing.T) {
	if *fixturesOut == "" {
		t.Skip("set -recovery-fixtures to write the shared crashed-store fixtures")
	}

	emit(t, "clean", crashedStore(t, 9990, 9999, 9999))
	emit(t, "tail-in-head-slice", crashedStore(t, 9990, 9995, 9999))
	emit(t, "tail-past-slice-boundary", crashedStore(t, 2, 9998, 10006))

	short := crashedStore(t, 9990, 9999, 9999)
	data := filepath.Join(short, "history_0000000001-0000010000", "data.log")
	st, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(data, st.Size()-3); err != nil {
		t.Fatal(err)
	}
	emit(t, "store-short-of-manifest", short)

	behind := crashedStore(t, 9990, 9995, 9999)
	stale, err := coremanifest.ReadManifest(behind)
	if err != nil {
		t.Fatal(err)
	}
	stale.Head -= 3
	if err := coremanifest.WritePublished(behind, stale); err != nil {
		t.Fatal(err)
	}
	emit(t, "published-behind-manifest", behind)

	pre := crashedStore(t, 9990, 9996, 9999)
	for _, p := range []string{
		coremanifest.ManifestPath(pre),
		coremanifest.PublishedPath(pre),
		coremanifest.BindingPath(pre),
		coremanifest.BindingPath(filepath.Join(pre, "abis")),
	} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	emit(t, "adopt-pre-protocol", pre)
}

var fixturesCheck = flag.String("recovery-fixtures-check", "", "recover each shared fixture in this directory and compare it with after.sha256")

func TestCheckRecoveryFixtures(t *testing.T) {
	if *fixturesCheck == "" {
		t.Skip("set -recovery-fixtures-check to recover the shared crashed-store fixtures")
	}
	dirs, err := os.ReadDir(*fixturesCheck)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		t.Run(d.Name(), func(t *testing.T) {
			dir := filepath.Join(*fixturesCheck, d.Name())
			var want fixtureExpect
			b, err := os.ReadFile(filepath.Join(dir, "expect.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &want); err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(t.TempDir(), "core")
			copyTree(t, filepath.Join(dir, "before", "core"), work)
			m, adopted, err := Recover(Config{CoreRoot: work, ABIRoot: filepath.Join(work, "abis"), SliceSpan: fixtureSpan, HeadID: headID})
			got := fixtureExpect{Outcome: "recovered", Adopted: adopted, Head: m.Head}
			if err != nil {
				got = fixtureExpect{Outcome: "refused"}
			}
			if got != want {
				t.Fatalf("recover gave %+v, fixture expects %+v (%v)", got, want, err)
			}
			digests := filepath.Join(t.TempDir(), "after.sha256")
			writeDigests(t, work, digests, got.Adopted)
			gotDigests, _ := os.ReadFile(digests)
			wantDigests, err := os.ReadFile(filepath.Join(dir, "after.sha256"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotDigests, wantDigests) {
				t.Fatalf("recovered store differs from after.sha256:\n%s\nwant:\n%s", gotDigests, wantDigests)
			}
		})
	}
}
