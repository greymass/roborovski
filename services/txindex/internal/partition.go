package internal

import (
	"fmt"
	"os"
	"sort"
	"syscall"
)

type Partition struct {
	id       uint16
	path     string
	mmapData []byte
	header   *PartitionHeader
	loaded   bool
}

func NewPartition(id uint16, basePath string) *Partition {
	return &Partition{
		id:   id,
		path: PartitionIDToPath(basePath, id),
	}
}

func (p *Partition) Load() error {
	if p.loaded {
		return nil
	}

	f, err := os.Open(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open partition %04x: %w", p.id, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat partition %04x: %w", p.id, err)
	}

	if info.Size() < PartitionHeaderSize {
		return fmt.Errorf("partition %04x too small: %d bytes", p.id, info.Size())
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap partition %04x: %w", p.id, err)
	}

	header, err := DecodePartitionHeader(data[:PartitionHeaderSize])
	if err != nil {
		syscall.Munmap(data)
		return fmt.Errorf("decode header %04x: %w", p.id, err)
	}

	expectedSize := PartitionHeaderSize + int(header.EntryCount)*PartitionEntrySize
	if int(info.Size()) < expectedSize {
		syscall.Munmap(data)
		return fmt.Errorf("partition %04x truncated: expected %d, got %d", p.id, expectedSize, info.Size())
	}

	p.mmapData = data
	p.header = header
	p.loaded = true
	return nil
}

func (p *Partition) Unload() error {
	if !p.loaded || p.mmapData == nil {
		return nil
	}
	err := syscall.Munmap(p.mmapData)
	p.mmapData = nil
	p.header = nil
	p.loaded = false
	return err
}

func (p *Partition) EntryCount() uint64 {
	if !p.loaded || p.header == nil {
		return 0
	}
	return p.header.EntryCount
}

func (p *Partition) BinarySearch(prefix3 [3]byte) ([]uint32, bool) {
	if !p.loaded || p.header == nil || p.header.EntryCount == 0 {
		return nil, false
	}

	data := p.mmapData[PartitionHeaderSize:]
	count := int(p.header.EntryCount)

	idx := sort.Search(count, func(i int) bool {
		offset := i * PartitionEntrySize
		var entryPrefix3 [3]byte
		copy(entryPrefix3[:], data[offset:offset+3])
		return ComparePrefix3(entryPrefix3, prefix3) >= 0
	})

	if idx >= count {
		return nil, false
	}

	offset := idx * PartitionEntrySize
	var foundPrefix3 [3]byte
	copy(foundPrefix3[:], data[offset:offset+3])

	if ComparePrefix3(foundPrefix3, prefix3) != 0 {
		return nil, false
	}

	var blockNums []uint32
	for i := idx; i < count; i++ {
		off := i * PartitionEntrySize
		var p3 [3]byte
		copy(p3[:], data[off:off+3])
		if ComparePrefix3(p3, prefix3) != 0 {
			break
		}
		entry, err := DecodePartitionEntry(data[off : off+PartitionEntrySize])
		if err != nil {
			break
		}
		blockNums = append(blockNums, entry.BlockNum)
	}

	return blockNums, len(blockNums) > 0
}

func (p *Partition) Exists() bool {
	_, err := os.Stat(p.path)
	return err == nil
}

type PartitionWriter struct {
	path     string
	file     *os.File
	entries  []PartitionEntry
	minBlock uint32
	maxBlock uint32
}

func NewPartitionWriter(path string) *PartitionWriter {
	return &PartitionWriter{
		path:     path,
		minBlock: ^uint32(0),
		maxBlock: 0,
	}
}

func (w *PartitionWriter) AddEntry(prefix3 [3]byte, blockNum uint32) {
	w.entries = append(w.entries, PartitionEntry{
		Prefix3:  prefix3,
		BlockNum: blockNum,
	})
	if blockNum < w.minBlock {
		w.minBlock = blockNum
	}
	if blockNum > w.maxBlock {
		w.maxBlock = blockNum
	}
}

func (w *PartitionWriter) Finalize() error {
	if len(w.entries) == 0 {
		return nil
	}

	sort.Slice(w.entries, func(i, j int) bool {
		cmp := ComparePrefix3(w.entries[i].Prefix3, w.entries[j].Prefix3)
		if cmp != 0 {
			return cmp < 0
		}
		return w.entries[i].BlockNum < w.entries[j].BlockNum
	})

	totalSize := PartitionHeaderSize + len(w.entries)*PartitionEntrySize
	buf := make([]byte, totalSize)

	header := NewPartitionHeader(uint64(len(w.entries)), w.minBlock, w.maxBlock)
	copy(buf[:PartitionHeaderSize], header.Encode())

	for i, entry := range w.entries {
		offset := PartitionHeaderSize + i*PartitionEntrySize
		copy(buf[offset:], entry.Encode())
	}

	return os.WriteFile(w.path, buf, 0644)
}
