package internal

import (
	"encoding/binary"
	"errors"
)

const (
	ChunkSize           = 10000
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

type PartialChunk struct {
	BaseSeq uint64
	Seqs    []uint64
}

func NewPartialChunk() *PartialChunk {
	return &PartialChunk{
		Seqs: make([]uint64, 0, InitialChunkCapacity),
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

func (p *PartialChunk) Reset() {
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
