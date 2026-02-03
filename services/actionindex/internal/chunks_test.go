package internal

import (
	"testing"
)

func TestPartialChunk(t *testing.T) {
	p := NewPartialChunk()

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
}

func TestPartialChunk_IsFull(t *testing.T) {
	p := NewPartialChunk()

	for i := 0; i < ChunkSize; i++ {
		p.Add(uint64(i))
	}

	if !p.IsFull() {
		t.Error("partial with ChunkSize elements should be full")
	}
}

func TestPartialChunk_Reset(t *testing.T) {
	p := NewPartialChunk()
	p.Add(100)
	p.Add(200)

	p.Reset()

	if p.Len() != 0 {
		t.Errorf("len: got %d, want 0", p.Len())
	}
	if p.BaseSeq != 0 {
		t.Errorf("baseSeq: got %d, want 0", p.BaseSeq)
	}
}

func TestEncodeLeanChunk(t *testing.T) {
	seqs := []uint64{1000, 1050, 1051, 1052, 2000000}

	encoded, err := EncodeLeanChunk(1000, seqs)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	chunk, err := DecodeLeanChunk(1000, encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
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

func TestEncodeLeanChunk_Empty(t *testing.T) {
	_, err := EncodeLeanChunk(0, []uint64{})
	if err != ErrChunkEmpty {
		t.Errorf("expected ErrChunkEmpty, got %v", err)
	}
}

func TestDecodeLeanChunk_TooShort(t *testing.T) {
	_, err := DecodeLeanChunk(0, []byte{1, 2, 3})
	if err != ErrInsufficientData {
		t.Errorf("expected ErrInsufficientData, got %v", err)
	}
}
