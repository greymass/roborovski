package internal

import (
	"testing"
)

func TestEncodeDecodeChunk_Simple(t *testing.T) {
	seqs := []uint64{1000, 1050, 1051, 1052, 2000000}

	encoded, err := EncodeChunk(seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if chunk.BaseSeq != 1000 {
		t.Errorf("baseSeq: got %d, want 1000", chunk.BaseSeq)
	}

	if len(chunk.Seqs) != len(seqs) {
		t.Fatalf("seqs length: got %d, want %d", len(chunk.Seqs), len(seqs))
	}

	for i, want := range seqs {
		if chunk.Seqs[i] != want {
			t.Errorf("seqs[%d]: got %d, want %d", i, chunk.Seqs[i], want)
		}
	}
}

func TestEncodeDecodeChunk_SingleElement(t *testing.T) {
	seqs := []uint64{12345}

	encoded, err := EncodeChunk(seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(chunk.Seqs) != 1 || chunk.Seqs[0] != 12345 {
		t.Errorf("got %v, want [12345]", chunk.Seqs)
	}
}

func TestEncodeDecodeChunk_LargeDeltas(t *testing.T) {
	seqs := []uint64{
		1000000000000,
		2000000000000,
		3000000000000,
		3000000000001,
		3000000000002,
	}

	encoded, err := EncodeChunk(seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i, want := range seqs {
		if chunk.Seqs[i] != want {
			t.Errorf("seqs[%d]: got %d, want %d", i, chunk.Seqs[i], want)
		}
	}
}

func TestEncodeDecodeChunk_DenseSequences(t *testing.T) {
	seqs := make([]uint64, 1000)
	for i := range seqs {
		seqs[i] = uint64(1000 + i)
	}

	encoded, err := EncodeChunk(seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if len(encoded) > ChunkHeaderSize+len(seqs)*2 {
		t.Errorf("encoding not efficient for dense data: %d bytes for %d seqs", len(encoded), len(seqs))
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i, want := range seqs {
		if chunk.Seqs[i] != want {
			t.Errorf("seqs[%d]: got %d, want %d", i, chunk.Seqs[i], want)
		}
	}
}

func TestEncodeDecodeChunk_FullChunk(t *testing.T) {
	seqs := make([]uint64, ChunkSize)
	for i := range seqs {
		seqs[i] = uint64(1000000 + i*100)
	}

	encoded, err := EncodeChunk(seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(chunk.Seqs) != ChunkSize {
		t.Errorf("seqs length: got %d, want %d", len(chunk.Seqs), ChunkSize)
	}

	for i := 0; i < 10; i++ {
		if chunk.Seqs[i] != seqs[i] {
			t.Errorf("seqs[%d]: got %d, want %d", i, chunk.Seqs[i], seqs[i])
		}
	}
	for i := ChunkSize - 10; i < ChunkSize; i++ {
		if chunk.Seqs[i] != seqs[i] {
			t.Errorf("seqs[%d]: got %d, want %d", i, chunk.Seqs[i], seqs[i])
		}
	}
}

func TestEncodeChunk_Empty(t *testing.T) {
	_, err := EncodeChunk([]uint64{})
	if err != ErrChunkEmpty {
		t.Errorf("expected ErrChunkEmpty, got %v", err)
	}
}

func TestDecodeChunk_TooShort(t *testing.T) {
	_, err := DecodeChunk([]byte{1, 2, 3})
	if err != ErrInsufficientData {
		t.Errorf("expected ErrInsufficientData, got %v", err)
	}
}

func TestDecodeChunkBaseSeq(t *testing.T) {
	seqs := []uint64{9999999, 10000000, 10000001}
	encoded, _ := EncodeChunk(seqs)

	baseSeq, err := DecodeChunkBaseSeq(encoded)
	if err != nil {
		t.Fatalf("DecodeChunkBaseSeq failed: %v", err)
	}
	if baseSeq != 9999999 {
		t.Errorf("baseSeq: got %d, want 9999999", baseSeq)
	}
}

func TestDecodeChunkCount(t *testing.T) {
	seqs := []uint64{100, 200, 300, 400, 500}
	encoded, _ := EncodeChunk(seqs)

	count, err := DecodeChunkCount(encoded)
	if err != nil {
		t.Fatalf("DecodeChunkCount failed: %v", err)
	}
	if count != 5 {
		t.Errorf("count: got %d, want 5", count)
	}
}

func TestPartialChunk(t *testing.T) {
	p := NewPartialChunk(0)

	if p.IsFull() {
		t.Error("new partial should not be full")
	}

	p.Add(1000)
	p.Add(1001)
	p.Add(1002)

	if p.Len() != 3 {
		t.Errorf("len: got %d, want 3", p.Len())
	}
	if p.BaseSeq != 1000 {
		t.Errorf("baseSeq: got %d, want 1000", p.BaseSeq)
	}

	encoded, err := p.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeChunk(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(chunk.Seqs) != 3 {
		t.Errorf("seqs length: got %d, want 3", len(chunk.Seqs))
	}
}

func TestPartialChunk_IsFull(t *testing.T) {
	p := NewPartialChunk(0)

	for i := 0; i < ChunkSize; i++ {
		p.Add(uint64(i))
	}

	if !p.IsFull() {
		t.Error("partial with ChunkSize elements should be full")
	}
}

func TestPartialChunk_Reset(t *testing.T) {
	p := NewPartialChunk(0)
	p.Add(100)
	p.Add(200)

	p.Reset(5)

	if p.ChunkID != 5 {
		t.Errorf("chunkID: got %d, want 5", p.ChunkID)
	}
	if p.Len() != 0 {
		t.Errorf("len: got %d, want 0", p.Len())
	}
	if p.BaseSeq != 0 {
		t.Errorf("baseSeq: got %d, want 0", p.BaseSeq)
	}
}
