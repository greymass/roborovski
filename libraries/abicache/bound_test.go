package abicache

import (
	"os"
	"path/filepath"
	"testing"
)

func writeABIs(t *testing.T, dir string, abis ...string) {
	t.Helper()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range abis {
		if err := w.Write(uint32(10*(i+1)), 7, []byte(a)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderHonoursLimit(t *testing.T) {
	dir := t.TempDir()
	writeABIs(t, dir, `{"version":"one"}`, `{"version":"two"}`)

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	limit := uint32(1)
	if err := r.SetLimit(func() (uint32, bool) { return limit, true }); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(7, 100)
	if err != nil || string(got) != `{"version":"one"}` {
		t.Fatalf("bounded to one entry: got %q, %v", got, err)
	}

	limit = 2
	got, err = r.Get(7, 100)
	if err != nil || string(got) != `{"version":"two"}` {
		t.Fatalf("limit raised to two: got %q, %v", got, err)
	}

	limit = 0
	if _, err := r.Get(7, 100); err == nil {
		t.Fatal("limit 0 should hide every entry")
	}
}

func TestReaderFollowsReplacedIndex(t *testing.T) {
	dir := t.TempDir()
	writeABIs(t, dir, `{"version":"old-a"}`, `{"version":"old-b"}`)

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.parsedABIsMu.Lock()
	r.parsedABIs[999] = nil
	r.parsedABIsMu.Unlock()

	idx, err := os.ReadFile(filepath.Join(dir, "abis.index"))
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "abis.index.tmp")
	if err := os.WriteFile(tmp, idx[:indexEntrySize], 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "abis.index")); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(7, 100)
	if err != nil || string(got) != `{"version":"old-a"}` {
		t.Fatalf("after reset to one entry: got %q, %v", got, err)
	}
	r.parsedABIsMu.RLock()
	n := len(r.parsedABIs)
	r.parsedABIsMu.RUnlock()
	if n != 0 {
		t.Fatalf("parsed ABI cache kept %d entries across a replacement", n)
	}
}
