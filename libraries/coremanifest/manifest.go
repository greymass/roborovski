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
	ManifestSize    = 160
	ControlDirName  = "control"
	ManifestName    = "manifest"
)

var (
	ErrNoManifest      = errors.New("no manifest")
	ErrManifestCorrupt = errors.New("manifest corrupt")
	castagnoli         = crc32.MakeTable(crc32.Castagnoli)
)

// Manifest is the commit record for core storage and the ABI cache.
// Readers may use exactly the bytes and entries it names; anything beyond is uncommitted.
type Manifest struct {
	ChainID       [32]byte
	CoreUUID      [16]byte
	ABIUUID       [16]byte
	Epoch         uint64
	FirstBlock    uint32
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
	copy(b[16:48], m.ChainID[:])
	copy(b[48:64], m.CoreUUID[:])
	copy(b[64:80], m.ABIUUID[:])
	le.PutUint64(b[80:88], m.Epoch)
	le.PutUint32(b[88:92], m.FirstBlock)
	le.PutUint32(b[92:96], m.Head)
	copy(b[96:128], m.HeadID[:])
	le.PutUint64(b[128:136], m.SliceDataEnd)
	le.PutUint32(b[136:140], m.SliceIndexLen)
	le.PutUint32(b[140:144], m.ABIIndexLen)
	le.PutUint64(b[144:152], m.ABIDataEnd)
	le.PutUint32(b[152:156], m.ABIFloor)
	le.PutUint32(b[156:160], crc32.Checksum(b[0:156], castagnoli))
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
	if got, want := le.Uint32(b[156:160]), crc32.Checksum(b[0:156], castagnoli); got != want {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrManifestCorrupt)
	}
	m := &Manifest{}
	copy(m.ChainID[:], b[16:48])
	copy(m.CoreUUID[:], b[48:64])
	copy(m.ABIUUID[:], b[64:80])
	m.Epoch = le.Uint64(b[80:88])
	m.FirstBlock = le.Uint32(b[88:92])
	m.Head = le.Uint32(b[92:96])
	copy(m.HeadID[:], b[96:128])
	m.SliceDataEnd = le.Uint64(b[128:136])
	m.SliceIndexLen = le.Uint32(b[136:140])
	m.ABIIndexLen = le.Uint32(b[140:144])
	m.ABIDataEnd = le.Uint64(b[144:152])
	m.ABIFloor = le.Uint32(b[152:156])
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
