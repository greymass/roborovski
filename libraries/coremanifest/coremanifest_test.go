package coremanifest

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleManifest() *Manifest {
	m := &Manifest{Epoch: 3, Head: 12345, SliceDataEnd: 9876, SliceIndexLen: 2345, ABIIndexLen: 7, ABIDataEnd: 4096, ABIFloor: 5}
	m.CoreUUID[1], m.ABIUUID[2], m.HeadID[31] = 2, 3, 4
	return m
}

func TestManifestRoundTrip(t *testing.T) {
	m := sampleManifest()
	got, err := ParseManifest(m.MarshalBinary())
	if err != nil {
		t.Fatal(err)
	}
	if *got != *m {
		t.Fatalf("round trip changed manifest: %+v != %+v", got, m)
	}
}

func TestManifestRejectsDamage(t *testing.T) {
	b := sampleManifest().MarshalBinary()
	for _, off := range []int{0, 8, 56, 100, 127} {
		d := append([]byte(nil), b...)
		d[off] ^= 0xff
		if _, err := ParseManifest(d); !errors.Is(err, ErrManifestCorrupt) {
			t.Errorf("flip at %d: got %v, want ErrManifestCorrupt", off, err)
		}
	}
	if _, err := ParseManifest(b[:100]); !errors.Is(err, ErrManifestCorrupt) {
		t.Errorf("short manifest accepted")
	}
}

func TestWriteAndReadManifest(t *testing.T) {
	root := t.TempDir()
	if _, err := ReadManifest(root); !errors.Is(err, ErrNoManifest) {
		t.Fatalf("got %v, want ErrNoManifest", err)
	}
	m := sampleManifest()
	if err := WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	m.Head++
	if err := WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if got.Head != m.Head {
		t.Fatalf("head %d, want %d", got.Head, m.Head)
	}
	entries, _ := os.ReadDir(filepath.Join(root, ControlDirName))
	if len(entries) != 1 {
		t.Fatalf("control directory holds %d files, want only the manifest", len(entries))
	}
}

func TestWriteAndReadPublished(t *testing.T) {
	root := t.TempDir()
	if _, err := ReadPublished(root); !errors.Is(err, ErrNoPublished) {
		t.Fatalf("got %v, want ErrNoPublished", err)
	}
	m := sampleManifest()
	if err := WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	if err := WritePublished(root, m); err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(ManifestPath(root))
	b, _ := os.ReadFile(PublishedPath(root))
	if !bytes.Equal(a, b) {
		t.Fatal("published view is not a byte copy of the manifest")
	}
	got, err := ReadPublished(root)
	if err != nil || *got != *m {
		t.Fatalf("got %+v, %v", got, err)
	}
	entries, _ := os.ReadDir(filepath.Join(root, ControlDirName))
	if len(entries) != 2 {
		t.Fatalf("control directory holds %d files, want the manifest and the published view", len(entries))
	}
}

func TestBindingRoundTripAndPair(t *testing.T) {
	core, _ := NewUUID()
	abi, _ := NewUUID()
	cb := &Binding{Role: RoleCore, CoreUUID: core, ABIUUID: abi, SliceSpan: 10000}
	ab := *cb
	ab.Role = RoleABI
	root := t.TempDir()
	if err := WriteBinding(root, cb); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBinding(root)
	if err != nil {
		t.Fatal(err)
	}
	if *got != *cb {
		t.Fatalf("round trip changed binding")
	}
	if err := CheckPair(cb, &ab); err != nil {
		t.Fatal(err)
	}
	other := ab
	other.CoreUUID[0] ^= 1
	if err := CheckPair(cb, &other); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("mismatched pair accepted: %v", err)
	}
	m := &Manifest{CoreUUID: core, ABIUUID: abi}
	if err := CheckManifest(cb, m); err != nil {
		t.Fatal(err)
	}
	m.CoreUUID[0] ^= 1
	if err := CheckManifest(cb, m); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("manifest from another store accepted")
	}
}

func TestSecondWriterRefused(t *testing.T) {
	root := t.TempDir()
	held, err := WriterLockHeld(root)
	if err != nil || held {
		t.Fatalf("fresh root reports held=%v err=%v", held, err)
	}
	l, err := AcquireWriterLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWriterLock(root); !errors.Is(err, ErrLocked) {
		t.Fatalf("second writer got %v, want ErrLocked", err)
	}
	if held, err := WriterLockHeld(root); err != nil || !held {
		t.Fatalf("held lock probed as held=%v err=%v", held, err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	if held, _ := WriterLockHeld(root); held {
		t.Fatal("released lock still reported held")
	}
	l2, err := AcquireWriterLock(root)
	if err != nil {
		t.Fatalf("lock not reacquirable after release: %v", err)
	}
	l2.Release()
}

func TestLockHelperProcess(t *testing.T) {
	root := os.Getenv("COREMANIFEST_LOCK_ROOT")
	if root == "" {
		t.Skip("helper for TestLockReleasedWhenProcessDies")
	}
	if _, err := AcquireWriterLock(root); err != nil {
		os.Exit(3)
	}
	os.Stdout.WriteString("locked\n")
	time.Sleep(time.Hour)
}

func TestLockReleasedWhenProcessDies(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestLockHelperProcess$")
	cmd.Env = append(os.Environ(), "COREMANIFEST_LOCK_ROOT="+root)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := out.Read(buf)
	if !strings.Contains(string(buf[:n]), "locked") {
		cmd.Process.Kill()
		t.Fatalf("helper did not take the lock: %q", buf[:n])
	}
	if _, err := AcquireWriterLock(root); !errors.Is(err, ErrLocked) {
		cmd.Process.Kill()
		t.Fatalf("lock held by another process was granted: %v", err)
	}
	cmd.Process.Kill()
	cmd.Wait()
	l, err := AcquireWriterLock(root)
	if err != nil {
		t.Fatalf("lock not released when holder was killed: %v", err)
	}
	l.Release()
}
