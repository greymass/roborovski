package coremanifest

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	BindingMagic   = "RBBIND\x00\x00"
	BindingVersion = 1
	BindingSize    = 96
	BindingName    = "store.binding"
)

type Role uint32

const (
	RoleCore Role = 1
	RoleABI  Role = 2
)

func (r Role) String() string {
	switch r {
	case RoleCore:
		return "core"
	case RoleABI:
		return "abi"
	}
	return fmt.Sprintf("role(%d)", uint32(r))
}

var (
	ErrNoBinding       = errors.New("no binding")
	ErrBindingCorrupt  = errors.New("binding corrupt")
	ErrBindingMismatch = errors.New("binding mismatch")
)

// Binding stamps a storage root with its identity. Both roots carry the same pair of UUIDs.
// An all-zero ChainID means the chain was not known when the binding was written.
type Binding struct {
	Role      Role
	ChainID   [32]byte
	CoreUUID  [16]byte
	ABIUUID   [16]byte
	SliceSpan uint32
}

func NewUUID() ([16]byte, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return u, err
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u, nil
}

func FormatUUID(u [16]byte) string {
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func ParseChainID(s string) ([32]byte, error) {
	var id [32]byte
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return id, fmt.Errorf("chain ID must be 64 hex characters")
	}
	copy(id[:], b)
	return id, nil
}

func (b *Binding) ChainKnown() bool { return b.ChainID != [32]byte{} }

func (b *Binding) MarshalBinary() []byte {
	out := make([]byte, BindingSize)
	copy(out[0:8], BindingMagic)
	le := binary.LittleEndian
	le.PutUint32(out[8:12], BindingVersion)
	le.PutUint32(out[12:16], uint32(b.Role))
	copy(out[16:48], b.ChainID[:])
	copy(out[48:64], b.CoreUUID[:])
	copy(out[64:80], b.ABIUUID[:])
	le.PutUint32(out[80:84], b.SliceSpan)
	le.PutUint32(out[92:96], crc32.Checksum(out[0:92], castagnoli))
	return out
}

func ParseBinding(data []byte) (*Binding, error) {
	if len(data) != BindingSize {
		return nil, fmt.Errorf("%w: size %d, want %d", ErrBindingCorrupt, len(data), BindingSize)
	}
	if !bytes.Equal(data[0:8], []byte(BindingMagic)) {
		return nil, fmt.Errorf("%w: bad magic", ErrBindingCorrupt)
	}
	le := binary.LittleEndian
	if v := le.Uint32(data[8:12]); v != BindingVersion {
		return nil, fmt.Errorf("%w: version %d, this build reads %d", ErrBindingCorrupt, v, BindingVersion)
	}
	if got, want := le.Uint32(data[92:96]), crc32.Checksum(data[0:92], castagnoli); got != want {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrBindingCorrupt)
	}
	b := &Binding{Role: Role(le.Uint32(data[12:16])), SliceSpan: le.Uint32(data[80:84])}
	copy(b.ChainID[:], data[16:48])
	copy(b.CoreUUID[:], data[48:64])
	copy(b.ABIUUID[:], data[64:80])
	return b, nil
}

func BindingPath(root string) string { return filepath.Join(root, BindingName) }

func ReadBinding(root string) (*Binding, error) {
	data, err := os.ReadFile(BindingPath(root))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoBinding
	}
	if err != nil {
		return nil, err
	}
	return ParseBinding(data)
}

func WriteBinding(root string, b *Binding) error {
	return WriteFileAtomic(BindingPath(root), b.MarshalBinary())
}

// CheckPair verifies that a core and an ABI binding describe the same store.
func CheckPair(core, abi *Binding) error {
	switch {
	case core.Role != RoleCore:
		return fmt.Errorf("%w: core root holds a %s binding", ErrBindingMismatch, core.Role)
	case abi.Role != RoleABI:
		return fmt.Errorf("%w: ABI root holds a %s binding", ErrBindingMismatch, abi.Role)
	case core.CoreUUID != abi.CoreUUID || core.ABIUUID != abi.ABIUUID:
		return fmt.Errorf("%w: core root is %s/%s, ABI root is %s/%s", ErrBindingMismatch,
			FormatUUID(core.CoreUUID), FormatUUID(core.ABIUUID), FormatUUID(abi.CoreUUID), FormatUUID(abi.ABIUUID))
	case core.ChainID != abi.ChainID:
		return fmt.Errorf("%w: core and ABI roots record different chain IDs", ErrBindingMismatch)
	case core.SliceSpan != abi.SliceSpan:
		return fmt.Errorf("%w: core slice span %d, ABI slice span %d", ErrBindingMismatch, core.SliceSpan, abi.SliceSpan)
	}
	return nil
}

// CheckManifest verifies that a manifest belongs to the bound store.
func CheckManifest(core *Binding, m *Manifest) error {
	if m.CoreUUID != core.CoreUUID || m.ABIUUID != core.ABIUUID {
		return fmt.Errorf("%w: manifest names store %s/%s, binding names %s/%s", ErrBindingMismatch,
			FormatUUID(m.CoreUUID), FormatUUID(m.ABIUUID), FormatUUID(core.CoreUUID), FormatUUID(core.ABIUUID))
	}
	return nil
}
