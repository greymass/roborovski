package corereader

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func layoutPair(t *testing.T, name string) ([]byte, []byte) {
	t.Helper()
	a, err := os.ReadFile(filepath.Join("testdata/payload-layout", name+"-store.bin"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("testdata/payload-layout", name+"-compare.bin"))
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestPayloadsHaveSameContentAcrossDataTableLayouts(t *testing.T) {
	for _, name := range []string{"vaulta-269189631-orphan", "vaulta-269184124-reorder"} {
		pa, pb := layoutPair(t, name)
		na, _, _ := PayloadWithoutElapsed(pa)
		nb, _, _ := PayloadWithoutElapsed(pb)
		if bytes.Equal(na, nb) {
			t.Fatalf("%s: fixture payloads are byte-identical without elapsed", name)
		}
		same, err := PayloadsHaveSameContent(pa, pb, true)
		if err != nil || !same {
			t.Errorf("%s: want same content, got %v %v", name, same, err)
		}
	}
}

func TestPayloadsHaveSameContentSeesChangedActionData(t *testing.T) {
	pa, pb := layoutPair(t, "vaulta-269184124-reorder")
	blob := bytesToBlockBlob(pb)
	changed := false
	for _, c := range blob.Cats {
		d := blob.Block.DataInBlock[c.DataIndex]
		if len(d) < 4 || bytes.Count(pb, d) != 1 {
			continue
		}
		pb = bytes.Clone(pb)
		pb[bytes.Index(pb, d)+len(d)-1] ^= 0xff
		changed = true
		break
	}
	if !changed {
		t.Fatal("no uniquely located action data in fixture")
	}
	if same, err := PayloadsHaveSameContent(pa, pb, true); err != nil || same {
		t.Errorf("changed action data: want different content, got %v %v", same, err)
	}
}
