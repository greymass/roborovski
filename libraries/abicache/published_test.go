package abicache

import (
	"os"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/coremanifest"
)

func TestManifestLimitFollowsPublished(t *testing.T) {
	root := t.TempDir()
	limit := ManifestLimit(root)
	if _, ok := limit(); ok {
		t.Fatal("store with no manifest must have no limit")
	}

	m := &coremanifest.Manifest{Epoch: 1, Head: 10, ABIIndexLen: 3}
	if err := coremanifest.WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	if n, ok := limit(); !ok || n != 0 {
		t.Fatalf("manifest without published view: limit %d, %v; want 0, true", n, ok)
	}
	if err := coremanifest.WritePublished(root, m); err != nil {
		t.Fatal(err)
	}
	if n, ok := limit(); !ok || n != 3 {
		t.Fatalf("published 3: limit %d, %v", n, ok)
	}

	ahead := *m
	ahead.ABIIndexLen = 5
	time.Sleep(2 * time.Millisecond)
	if err := coremanifest.WriteManifest(root, &ahead); err != nil {
		t.Fatal(err)
	}
	if n, _ := limit(); n != 3 {
		t.Fatalf("manifest ahead of published view: limit %d, want 3", n)
	}

	if err := os.WriteFile(coremanifest.PublishedPath(root), []byte("torn"), 0644); err != nil {
		t.Fatal(err)
	}
	if n, ok := limit(); !ok || n != 3 {
		t.Fatalf("corrupt published view: limit %d, %v; want the last good 3", n, ok)
	}
	if err := os.Remove(coremanifest.PublishedPath(root)); err != nil {
		t.Fatal(err)
	}
	if n, ok := limit(); !ok || n != 3 {
		t.Fatalf("missing published view: limit %d, %v; want the last good 3", n, ok)
	}
	if err := coremanifest.WritePublished(root, &ahead); err != nil {
		t.Fatal(err)
	}
	if n, _ := limit(); n != 5 {
		t.Fatalf("published 5: limit %d", n)
	}
}
