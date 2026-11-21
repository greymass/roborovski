package coremanifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	ManifestMagic   = "RBMANIF\x00"
	ManifestVersion = 1
	ManifestSize    = 128
	ControlDirName  = "control"
	ManifestName    = "manifest"
	PublishedName   = "published"
)

var (
	ErrNoManifest      = errors.New("no manifest")
	ErrManifestCorrupt = errors.New("manifest corrupt")
	ErrNoPublished     = errors.New("no published view")
	castagnoli         = crc32.MakeTable(crc32.Castagnoli)
)

// Manifest is the commit record for core storage and the ABI cache.
// Readers may use exactly the bytes and entries it names; anything beyond is uncommitted.
type Manifest struct {
	CoreUUID      [16]byte
	ABIUUID       [16]byte
	Epoch         uint64
	Head          uint32
	HeadID        [32]byte
	SliceDataEnd  uint64
	SliceIndexLen uint32
	ABIIndexLen   uint32
	ABIDataEnd    uint64
	ABIFloor      uint32
}

func (m *Manifest) Empty() bool { return m.Head == 0 }

func (m *Manifest) MarshalBinary() []byte {
	b := make([]byte, ManifestSize)
	copy(b[0:8], ManifestMagic)
	le := binary.LittleEndian
	le.PutUint32(b[8:12], ManifestVersion)
	copy(b[16:32], m.CoreUUID[:])
	copy(b[32:48], m.ABIUUID[:])
	le.PutUint64(b[48:56], m.Epoch)
	le.PutUint32(b[56:60], m.Head)
	le.PutUint32(b[60:64], m.ABIIndexLen)
	copy(b[64:96], m.HeadID[:])
	le.PutUint64(b[96:104], m.SliceDataEnd)
	le.PutUint32(b[104:108], m.SliceIndexLen)
	le.PutUint32(b[108:112], m.ABIFloor)
	le.PutUint64(b[112:120], m.ABIDataEnd)
	le.PutUint32(b[124:128], crc32.Checksum(b[0:124], castagnoli))
	return b
}

func ParseManifest(b []byte) (*Manifest, error) {
	if len(b) != ManifestSize {
		return nil, fmt.Errorf("%w: size %d, want %d", ErrManifestCorrupt, len(b), ManifestSize)
	}
	if !bytes.Equal(b[0:8], []byte(ManifestMagic)) {
		return nil, fmt.Errorf("%w: bad magic", ErrManifestCorrupt)
	}
	le := binary.LittleEndian
	if v := le.Uint32(b[8:12]); v != ManifestVersion {
		return nil, fmt.Errorf("%w: version %d, this build reads %d", ErrManifestCorrupt, v, ManifestVersion)
	}
	if got, want := le.Uint32(b[124:128]), crc32.Checksum(b[0:124], castagnoli); got != want {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrManifestCorrupt)
	}
	m := &Manifest{}
	copy(m.CoreUUID[:], b[16:32])
	copy(m.ABIUUID[:], b[32:48])
	m.Epoch = le.Uint64(b[48:56])
	m.Head = le.Uint32(b[56:60])
	m.ABIIndexLen = le.Uint32(b[60:64])
	copy(m.HeadID[:], b[64:96])
	m.SliceDataEnd = le.Uint64(b[96:104])
	m.SliceIndexLen = le.Uint32(b[104:108])
	m.ABIFloor = le.Uint32(b[108:112])
	m.ABIDataEnd = le.Uint64(b[112:120])
	return m, nil
}

func ManifestPath(coreRoot string) string {
	return filepath.Join(coreRoot, ControlDirName, ManifestName)
}

func ReadManifest(coreRoot string) (*Manifest, error) {
	b, err := os.ReadFile(ManifestPath(coreRoot))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoManifest
	}
	if err != nil {
		return nil, err
	}
	return ParseManifest(b)
}

// WriteManifest publishes m: temporary file, fsync, rename, directory fsync.
func WriteManifest(coreRoot string, m *Manifest) error {
	dir := filepath.Join(coreRoot, ControlDirName)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := SyncDir(coreRoot); err != nil {
			return err
		}
	}
	return WriteFileAtomic(ManifestPath(coreRoot), m.MarshalBinary())
}

// PublishedPath is the copy of the manifest readers follow; writers replace it only after the manifest is durable.
func PublishedPath(coreRoot string) string {
	return filepath.Join(coreRoot, ControlDirName, PublishedName)
}

func ReadPublished(coreRoot string) (*Manifest, error) {
	b, err := os.ReadFile(PublishedPath(coreRoot))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoPublished
	}
	if err != nil {
		return nil, err
	}
	return ParseManifest(b)
}

func HasManifest(coreRoot string) bool {
	_, err := os.Stat(ManifestPath(coreRoot))
	return err == nil
}

// WritePublished replaces the published view with m by rename and no sync, so a power cut can only leave it behind the manifest.
func WritePublished(coreRoot string, m *Manifest) error {
	return replaceFile(PublishedPath(coreRoot), m.MarshalBinary())
}
