package internal

import (
	"encoding/binary"
	"errors"
)

const (
	ChunkSize           = 10000
	ChunkHeaderSize     = 12
	LeanChunkHeaderSize = 4
	MaxVarintLen64      = 10
	// InitialChunkCapacity controls starting slice capacity for partial chunks.
	// Higher values reduce memmove overhead from slice growth.
	// Benchmarks showed 256 reduces PartialChunk.Add from 6.8% to 2.6% CPU.
	InitialChunkCapacity = 256
)

var (
	ErrChunkEmpty       = errors.New("chunk has no sequences")
	ErrChunkCorrupt     = errors.New("chunk data corrupt")
	ErrInsufficientData = errors.New("insufficient data for chunk")
)

type Chunk struct {
	BaseSeq uint64
	Seqs    []uint64
}

func EncodeChunk(seqs []uint64) ([]byte, error) {
	if len(seqs) == 0 {
		return nil, ErrChunkEmpty
	}

	buf := make([]byte, ChunkHeaderSize+len(seqs)*MaxVarintLen64)

	binary.LittleEndian.PutUint64(buf[0:8], seqs[0])
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(seqs)))

	pos := ChunkHeaderSize
	prev := seqs[0]
	for i := 1; i < len(seqs); i++ {
		delta := seqs[i] - prev
		pos += binary.PutUvarint(buf[pos:], delta)
		prev = seqs[i]
	}

	return buf[:pos], nil
}

func DecodeChunk(data []byte) (*Chunk, error) {
	if len(data) < ChunkHeaderSize {
		return nil, ErrInsufficientData
	}

	baseSeq := binary.LittleEndian.Uint64(data[0:8])
	count := binary.LittleEndian.Uint32(data[8:12])

	if count == 0 {
		return nil, ErrChunkCorrupt
	}

	seqs := make([]uint64, count)
	seqs[0] = baseSeq

	pos := ChunkHeaderSize
	prev := baseSeq
	for i := uint32(1); i < count; i++ {
		delta, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, ErrChunkCorrupt
		}
		pos += n
		prev += delta
		seqs[i] = prev
	}

	return &Chunk{
		BaseSeq: baseSeq,
		Seqs:    seqs,
	}, nil
}

func DecodeChunkBaseSeq(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	return binary.LittleEndian.Uint64(data[0:8]), nil
}

func DecodeChunkCount(data []byte) (uint32, error) {
	if len(data) < ChunkHeaderSize {
		return 0, ErrInsufficientData
	}
	return binary.LittleEndian.Uint32(data[8:12]), nil
}

type PartialChunk struct {
	BaseSeq uint64
	Seqs    []uint64
	ChunkID uint32
}

func NewPartialChunk(chunkID uint32) *PartialChunk {
	return &PartialChunk{
		ChunkID: chunkID,
		Seqs:    make([]uint64, 0, InitialChunkCapacity),
	}
}

func (p *PartialChunk) Add(seq uint64) {
	if len(p.Seqs) == 0 {
		p.BaseSeq = seq
	}
	p.Seqs = append(p.Seqs, seq)
}

func (p *PartialChunk) IsFull() bool {
	return len(p.Seqs) >= ChunkSize
}

func (p *PartialChunk) Len() int {
	return len(p.Seqs)
}

func (p *PartialChunk) Encode() ([]byte, error) {
	return EncodeChunk(p.Seqs)
}

func (p *PartialChunk) Reset(chunkID uint32) {
	p.ChunkID = chunkID
	p.BaseSeq = 0
	p.Seqs = p.Seqs[:0]
}

func EncodeLeanChunk(baseSeq uint64, seqs []uint64) ([]byte, error) {
	if len(seqs) == 0 {
		return nil, ErrChunkEmpty
	}

	buf := make([]byte, LeanChunkHeaderSize+len(seqs)*MaxVarintLen64)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(seqs)))

	pos := LeanChunkHeaderSize
	prev := baseSeq
	for _, seq := range seqs {
		delta := seq - prev
		pos += binary.PutUvarint(buf[pos:], delta)
		prev = seq
	}

	return buf[:pos], nil
}

func DecodeLeanChunk(baseSeq uint64, data []byte) (*Chunk, error) {
	if len(data) < LeanChunkHeaderSize {
		return nil, ErrInsufficientData
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	if count == 0 {
		return nil, ErrChunkCorrupt
	}

	seqs := make([]uint64, count)
	pos := LeanChunkHeaderSize
	prev := baseSeq
	for i := uint32(0); i < count; i++ {
		delta, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, ErrChunkCorrupt
		}
		pos += n
		prev += delta
		seqs[i] = prev
	}

	return &Chunk{
		BaseSeq: baseSeq,
		Seqs:    seqs,
	}, nil
}
