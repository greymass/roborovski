package internal

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	PartitionHeaderSize = 32
	PartitionEntrySize  = 7
	WALEntrySize        = 9
	PartitionCount      = 65536
	PartitionMagic      = "TXIX"
	PartitionVersion    = 1
)

type PartitionHeader struct {
	Magic      [4]byte
	Version    uint8
	EntryCount uint64
	MinBlock   uint32
	MaxBlock   uint32
	Reserved   [11]byte
}

func (h *PartitionHeader) Encode() []byte {
	buf := make([]byte, PartitionHeaderSize)
	copy(buf[0:4], h.Magic[:])
	buf[4] = h.Version
	binary.LittleEndian.PutUint64(buf[5:13], h.EntryCount)
	binary.LittleEndian.PutUint32(buf[13:17], h.MinBlock)
	binary.LittleEndian.PutUint32(buf[17:21], h.MaxBlock)
	return buf
}

func DecodePartitionHeader(data []byte) (*PartitionHeader, error) {
	if len(data) < PartitionHeaderSize {
		return nil, fmt.Errorf("header too short: %d bytes", len(data))
	}
	h := &PartitionHeader{}
	copy(h.Magic[:], data[0:4])
	if string(h.Magic[:]) != PartitionMagic {
		return nil, fmt.Errorf("invalid magic: %s", string(h.Magic[:]))
	}
	h.Version = data[4]
	h.EntryCount = binary.LittleEndian.Uint64(data[5:13])
	h.MinBlock = binary.LittleEndian.Uint32(data[13:17])
	h.MaxBlock = binary.LittleEndian.Uint32(data[17:21])
	return h, nil
}

func NewPartitionHeader(entryCount uint64, minBlock, maxBlock uint32) *PartitionHeader {
	h := &PartitionHeader{
		Version:    PartitionVersion,
		EntryCount: entryCount,
		MinBlock:   minBlock,
		MaxBlock:   maxBlock,
	}
	copy(h.Magic[:], PartitionMagic)
	return h
}

type PartitionEntry struct {
	Prefix3  [3]byte
	BlockNum uint32
}

func (e *PartitionEntry) Encode() []byte {
	buf := make([]byte, PartitionEntrySize)
	copy(buf[0:3], e.Prefix3[:])
	binary.LittleEndian.PutUint32(buf[3:7], e.BlockNum)
	return buf
}

func DecodePartitionEntry(data []byte) (*PartitionEntry, error) {
	if len(data) < PartitionEntrySize {
		return nil, fmt.Errorf("entry too short: %d bytes", len(data))
	}
	e := &PartitionEntry{}
	copy(e.Prefix3[:], data[0:3])
	e.BlockNum = binary.LittleEndian.Uint32(data[3:7])
	return e, nil
}

type WALEntry struct {
	Prefix5  [5]byte
	BlockNum uint32
}

func (e *WALEntry) Encode() []byte {
	buf := make([]byte, WALEntrySize)
	copy(buf[0:5], e.Prefix5[:])
	binary.LittleEndian.PutUint32(buf[5:9], e.BlockNum)
	return buf
}

func DecodeWALEntry(data []byte) (*WALEntry, error) {
	if len(data) < WALEntrySize {
		return nil, fmt.Errorf("entry too short: %d bytes", len(data))
	}
	e := &WALEntry{}
	copy(e.Prefix5[:], data[0:5])
	e.BlockNum = binary.LittleEndian.Uint32(data[5:9])
	return e, nil
}

type IndexMeta struct {
	Version          int       `json:"version"`
	Mode             string    `json:"mode"`
	LastIndexedBlock uint32    `json:"lastIndexedBlock"`
	LastMergedBlock  uint32    `json:"lastMergedBlock"`
	WALEntryCount    uint64    `json:"walEntryCount"`
	CreatedAt        time.Time `json:"createdAt"`
	LastMergeAt      time.Time `json:"lastMergeAt"`
	BulkRunCount     int       `json:"bulkRunCount,omitempty"`
	BulkLastBlock    uint32    `json:"bulkLastBlock,omitempty"`
}

func NewIndexMeta() *IndexMeta {
	return &IndexMeta{
		Version:   1,
		Mode:      "bulk",
		CreatedAt: time.Now(),
	}
}

func LoadIndexMeta(path string) (*IndexMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	meta := &IndexMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *IndexMeta) Save(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func TrxIDToPrefix5(trxIDHex string) ([5]byte, error) {
	var prefix [5]byte
	blob, err := hex.DecodeString(trxIDHex)
	if err != nil {
		return prefix, fmt.Errorf("invalid hex: %w", err)
	}
	if len(blob) < 5 {
		return prefix, fmt.Errorf("transaction ID too short: %d bytes", len(blob))
	}
	copy(prefix[:], blob[:5])
	return prefix, nil
}

func Prefix5ToPartitionID(prefix5 [5]byte) uint16 {
	return uint16(prefix5[0])<<8 | uint16(prefix5[1])
}

func Prefix5ToPrefix3(prefix5 [5]byte) [3]byte {
	var prefix3 [3]byte
	copy(prefix3[:], prefix5[2:5])
	return prefix3
}

func PartitionIDToPath(basePath string, partitionID uint16) string {
	dir := fmt.Sprintf("%s/partitions/%02x", basePath, partitionID>>8)
	return fmt.Sprintf("%s/%02x.idx", dir, partitionID&0xff)
}

func PartitionIDToDirPath(basePath string, partitionID uint16) string {
	return fmt.Sprintf("%s/partitions/%02x", basePath, partitionID>>8)
}

func ComparePrefix3(a, b [3]byte) int {
	for i := 0; i < 3; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func ComparePrefix5(a, b [5]byte) int {
	for i := 0; i < 5; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
