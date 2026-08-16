package fcraw

import (
	"bytes"
	"testing"
)

func encodeSingleTxBlock(actionData [][]byte, returnValues [][]byte) []byte {
	enc := newTraceEncoder()
	encodeBlockHeader(enc)
	enc.writeVarUint32(0)
	enc.writeVarUint32(1)

	txID := [32]byte{17, 18, 19, 20}
	enc.writeChecksum256(txID)
	enc.writeVarUint32(0)
	enc.writeVarUint32(uint32(len(actionData)))
	for i := range actionData {
		encodeActionTraceV1(enc, uint32(i+1), uint64(100+i),
			actionData[i], returnValues[i], nil, nil, nil)
	}

	enc.writeUint8(0)
	enc.writeUint32(1000)
	enc.writeVarUint32(100)
	enc.writeVarUint32(0)
	enc.writeUint32(1700000000)
	enc.writeUint16(1000)
	enc.writeUint32(0x12345678)
	enc.writeVarUint32(100)
	enc.writeUint8(10)
	enc.writeVarUint32(0)
	return enc.data()
}

// Regression for vaulta block 515099635: zero-length-data action in a recycled arena slot.
func TestDecode_ZeroLengthDataAfterReset_NoStaleData(t *testing.T) {
	bufs := NewBlockDecodeBuffers()

	prev := encodeSingleTxBlock(
		[][]byte{{1, 2, 3, 4, 5, 6, 7, 8}, {9, 10, 11, 12, 13, 14, 15, 16}},
		[][]byte{nil, {0xAA, 0xBB}},
	)
	if _, err := DecodeBlockTraceV2Slice(NewSliceDecoder(prev), bufs); err != nil {
		t.Fatalf("decode block 1: %v", err)
	}

	bufs.Reset()

	parentData := []byte{0x80, 0xb1, 0x91, 0x5e, 0x5d, 0x26, 0x8d, 0xca, 0x00, 0x11, 0x22}
	cur := encodeSingleTxBlock(
		[][]byte{parentData, nil},
		[][]byte{nil, nil},
	)
	bt, err := DecodeBlockTraceV2Slice(NewSliceDecoder(cur), bufs)
	if err != nil {
		t.Fatalf("decode block 2: %v", err)
	}

	child := bt.TransactionsV2[0].Actions[1]
	if len(child.Data) != 0 {
		t.Errorf("zero-data action has stale Data: len=%d bytes=%x (parent prefix=%x)",
			len(child.Data), child.Data, parentData[:8])
		if bytes.Equal(child.Data, parentData[:len(child.Data)]) {
			t.Log("stale Data is exactly a prefix of the parent action's payload — matches production corruption")
		}
	}
	if len(child.ReturnValue) != 0 {
		t.Errorf("zero-return action has stale ReturnValue: len=%d bytes=%x",
			len(child.ReturnValue), child.ReturnValue)
	}
}
