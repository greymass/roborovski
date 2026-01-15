package fcraw

import (
	"testing"
)

func TestSliceDecoder_ReadPrimitives(t *testing.T) {
	data := []byte{
		0x01,       // byte
		0x34, 0x12, // uint16 = 0x1234
		0x78, 0x56, 0x34, 0x12, // uint32 = 0x12345678
		0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, // uint64 = 0x0123456789ABCDEF
		0x80, 0x80, 0x04, // varint = 65536 (0x10000)
		0x01, // bool true
		0x00, // bool false
	}

	sd := NewSliceDecoder(data)

	if got := sd.ReadByte(); got != 0x01 {
		t.Errorf("ReadByte() = %d, want %d", got, 0x01)
	}

	if got := sd.ReadUint16(); got != 0x1234 {
		t.Errorf("ReadUint16() = %d, want %d", got, 0x1234)
	}

	if got := sd.ReadUint32(); got != 0x12345678 {
		t.Errorf("ReadUint32() = %d, want %d", got, 0x12345678)
	}

	if got := sd.ReadUint64(); got != 0x0123456789ABCDEF {
		t.Errorf("ReadUint64() = %d, want %d", got, uint64(0x0123456789ABCDEF))
	}

	if got := sd.ReadVarUint32(); got != 65536 {
		t.Errorf("ReadVarUint32() = %d, want %d", got, 65536)
	}

	if got := sd.ReadBool(); got != true {
		t.Errorf("ReadBool() = %v, want %v", got, true)
	}

	if got := sd.ReadBool(); got != false {
		t.Errorf("ReadBool() = %v, want %v", got, false)
	}
}

func TestSliceDecoder_ReadChecksum256(t *testing.T) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}

	sd := NewSliceDecoder(data)
	got := sd.ReadChecksum256()

	for i := 0; i < 32; i++ {
		if got[i] != byte(i) {
			t.Errorf("ReadChecksum256()[%d] = %d, want %d", i, got[i], i)
		}
	}
}

func TestSliceDecoder_ReadBytesRef(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

	sd := NewSliceDecoder(data)
	ref := sd.ReadBytesRef(3)

	if len(ref) != 3 {
		t.Errorf("len(ReadBytesRef(3)) = %d, want 3", len(ref))
	}
	if ref[0] != 0x01 || ref[1] != 0x02 || ref[2] != 0x03 {
		t.Errorf("ReadBytesRef(3) = %v, want [1 2 3]", ref)
	}

	data[1] = 0xFF
	if ref[1] != 0xFF {
		t.Errorf("ReadBytesRef should reference original data")
	}
}

func TestSliceDecoder_ReadUint8(t *testing.T) {
	data := []byte{0x42, 0xFF, 0x00}
	sd := NewSliceDecoder(data)

	if got := sd.ReadUint8(); got != 0x42 {
		t.Errorf("ReadUint8() = %d, want %d", got, 0x42)
	}

	if got := sd.ReadUint8(); got != 0xFF {
		t.Errorf("ReadUint8() = %d, want %d", got, 0xFF)
	}

	if got := sd.ReadUint8(); got != 0x00 {
		t.Errorf("ReadUint8() = %d, want %d", got, 0x00)
	}
}

func TestSliceDecoder_ReadInt64(t *testing.T) {
	data := []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // -1 in two's complement
	}
	sd := NewSliceDecoder(data)

	if got := sd.ReadInt64(); got != -1 {
		t.Errorf("ReadInt64() = %d, want %d", got, -1)
	}
}

func TestSliceDecoder_ReadVariantIndex(t *testing.T) {
	data := []byte{0x05, 0x80, 0x01}
	sd := NewSliceDecoder(data)

	if got := sd.ReadVariantIndex(); got != 5 {
		t.Errorf("ReadVariantIndex() = %d, want %d", got, 5)
	}

	if got := sd.ReadVariantIndex(); got != 128 {
		t.Errorf("ReadVariantIndex() = %d, want %d", got, 128)
	}
}

func BenchmarkSliceDecoder_ReadByte(b *testing.B) {
	data := make([]byte, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sd := NewSliceDecoder(data)
		for j := 0; j < 100; j++ {
			_ = sd.ReadByte()
		}
	}
}

func BenchmarkSliceDecoder_ReadUint32(b *testing.B) {
	data := make([]byte, 400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sd := NewSliceDecoder(data)
		for j := 0; j < 100; j++ {
			_ = sd.ReadUint32()
		}
	}
}

func BenchmarkSliceDecoder_ReadUint64(b *testing.B) {
	data := make([]byte, 800)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sd := NewSliceDecoder(data)
		for j := 0; j < 100; j++ {
			_ = sd.ReadUint64()
		}
	}
}

func BenchmarkSliceDecoder_ReadVarUint32(b *testing.B) {
	data := []byte{0x80, 0x80, 0x04}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sd := NewSliceDecoder(data)
		_ = sd.ReadVarUint32()
	}
}

func TestNewBlockDecodeBuffers(t *testing.T) {
	bufs := NewBlockDecodeBuffers()

	if bufs == nil {
		t.Fatal("NewBlockDecodeBuffers() returned nil")
	}

	if cap(bufs.Transactions) != 32 {
		t.Errorf("Transactions capacity = %d, want 32", cap(bufs.Transactions))
	}
	if cap(bufs.TransactionsV3) != 32 {
		t.Errorf("TransactionsV3 capacity = %d, want 32", cap(bufs.TransactionsV3))
	}
	if cap(bufs.Signatures) != 32 {
		t.Errorf("Signatures capacity = %d, want 32", cap(bufs.Signatures))
	}
	if cap(bufs.Actions) != 2048 {
		t.Errorf("Actions capacity = %d, want 2048", cap(bufs.Actions))
	}
	if cap(bufs.Auths) != 4096 {
		t.Errorf("Auths capacity = %d, want 4096", cap(bufs.Auths))
	}
	if cap(bufs.Deltas) != 1024 {
		t.Errorf("Deltas capacity = %d, want 1024", cap(bufs.Deltas))
	}
	if cap(bufs.DataBuf) != 512*1024 {
		t.Errorf("DataBuf capacity = %d, want %d", cap(bufs.DataBuf), 512*1024)
	}
	if cap(bufs.ReturnBuf) != 64*1024 {
		t.Errorf("ReturnBuf capacity = %d, want %d", cap(bufs.ReturnBuf), 64*1024)
	}
	if cap(bufs.SignatureData) != 8*1024 {
		t.Errorf("SignatureData capacity = %d, want %d", cap(bufs.SignatureData), 8*1024)
	}
}

func TestBlockDecodeBuffers_Reset(t *testing.T) {
	bufs := NewBlockDecodeBuffers()

	bufs.Transactions = bufs.Transactions[:10]
	bufs.TransactionsV3 = bufs.TransactionsV3[:5]
	bufs.Signatures = bufs.Signatures[:3]
	bufs.Actions = bufs.Actions[:100]
	bufs.Auths = bufs.Auths[:200]
	bufs.Deltas = bufs.Deltas[:50]
	bufs.AuthSeqKeys = bufs.AuthSeqKeys[:30]
	bufs.AuthSeqVals = bufs.AuthSeqVals[:30]
	bufs.ActionAuthSeqRanges = bufs.ActionAuthSeqRanges[:20]
	bufs.DataBuf = bufs.DataBuf[:1000]
	bufs.ReturnBuf = bufs.ReturnBuf[:500]
	bufs.SignatureData = bufs.SignatureData[:100]

	bufs.Reset()

	if len(bufs.Transactions) != 0 {
		t.Errorf("After Reset(), len(Transactions) = %d, want 0", len(bufs.Transactions))
	}
	if len(bufs.TransactionsV3) != 0 {
		t.Errorf("After Reset(), len(TransactionsV3) = %d, want 0", len(bufs.TransactionsV3))
	}
	if len(bufs.Signatures) != 0 {
		t.Errorf("After Reset(), len(Signatures) = %d, want 0", len(bufs.Signatures))
	}
	if len(bufs.Actions) != 0 {
		t.Errorf("After Reset(), len(Actions) = %d, want 0", len(bufs.Actions))
	}
	if len(bufs.Auths) != 0 {
		t.Errorf("After Reset(), len(Auths) = %d, want 0", len(bufs.Auths))
	}
	if len(bufs.Deltas) != 0 {
		t.Errorf("After Reset(), len(Deltas) = %d, want 0", len(bufs.Deltas))
	}
	if len(bufs.AuthSeqKeys) != 0 {
		t.Errorf("After Reset(), len(AuthSeqKeys) = %d, want 0", len(bufs.AuthSeqKeys))
	}
	if len(bufs.AuthSeqVals) != 0 {
		t.Errorf("After Reset(), len(AuthSeqVals) = %d, want 0", len(bufs.AuthSeqVals))
	}
	if len(bufs.ActionAuthSeqRanges) != 0 {
		t.Errorf("After Reset(), len(ActionAuthSeqRanges) = %d, want 0", len(bufs.ActionAuthSeqRanges))
	}
	if len(bufs.DataBuf) != 0 {
		t.Errorf("After Reset(), len(DataBuf) = %d, want 0", len(bufs.DataBuf))
	}
	if len(bufs.ReturnBuf) != 0 {
		t.Errorf("After Reset(), len(ReturnBuf) = %d, want 0", len(bufs.ReturnBuf))
	}
	if len(bufs.SignatureData) != 0 {
		t.Errorf("After Reset(), len(SignatureData) = %d, want 0", len(bufs.SignatureData))
	}
}

func TestBlockDecodeBuffers_CapacityBytes(t *testing.T) {
	bufs := NewBlockDecodeBuffers()
	capacity := bufs.CapacityBytes()

	if capacity <= 0 {
		t.Errorf("CapacityBytes() = %d, want > 0", capacity)
	}

	expectedMin := int64(512*1024 + 64*1024 + 8*1024)
	if capacity < expectedMin {
		t.Errorf("CapacityBytes() = %d, want >= %d (byte buffer capacities)", capacity, expectedMin)
	}
}

func TestBlockDecodeBuffers_allocAction(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Actions: make([]ActionTraceV1, 0, 2),
	}

	act1 := bufs.allocAction()
	if act1 == nil {
		t.Fatal("allocAction() returned nil")
	}
	if len(bufs.Actions) != 1 {
		t.Errorf("len(Actions) = %d, want 1", len(bufs.Actions))
	}

	act2 := bufs.allocAction()
	if act2 == nil {
		t.Fatal("allocAction() returned nil")
	}
	if len(bufs.Actions) != 2 {
		t.Errorf("len(Actions) = %d, want 2", len(bufs.Actions))
	}

	act3 := bufs.allocAction()
	if act3 == nil {
		t.Fatal("allocAction() returned nil after growth")
	}
	if len(bufs.Actions) != 3 {
		t.Errorf("len(Actions) = %d, want 3", len(bufs.Actions))
	}
	if cap(bufs.Actions) < 3 {
		t.Errorf("cap(Actions) = %d, want >= 3", cap(bufs.Actions))
	}
}

func TestBlockDecodeBuffers_allocAuths(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Auths: make([]AuthorizationTrace, 0, 4),
	}

	auths1 := bufs.allocAuths(2)
	if len(auths1) != 2 {
		t.Errorf("allocAuths(2) returned slice of len %d, want 2", len(auths1))
	}
	if len(bufs.Auths) != 2 {
		t.Errorf("len(Auths) = %d, want 2", len(bufs.Auths))
	}

	auths2 := bufs.allocAuths(3)
	if len(auths2) != 3 {
		t.Errorf("allocAuths(3) returned slice of len %d, want 3", len(auths2))
	}
	if len(bufs.Auths) != 5 {
		t.Errorf("len(Auths) = %d, want 5", len(bufs.Auths))
	}
	if cap(bufs.Auths) < 5 {
		t.Errorf("cap(Auths) = %d, want >= 5", cap(bufs.Auths))
	}
}

func TestBlockDecodeBuffers_allocDeltas(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Deltas: make([]AccountDelta, 0, 2),
	}

	deltas1 := bufs.allocDeltas(2)
	if len(deltas1) != 2 {
		t.Errorf("allocDeltas(2) returned slice of len %d, want 2", len(deltas1))
	}

	deltas2 := bufs.allocDeltas(5)
	if len(deltas2) != 5 {
		t.Errorf("allocDeltas(5) returned slice of len %d, want 5", len(deltas2))
	}
	if len(bufs.Deltas) != 7 {
		t.Errorf("len(Deltas) = %d, want 7", len(bufs.Deltas))
	}
}

func TestBlockDecodeBuffers_allocAuthSeq(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		AuthSeqKeys: make([]uint64, 0, 4),
		AuthSeqVals: make([]uint64, 0, 4),
	}

	start1 := bufs.allocAuthSeq(2)
	if start1 != 0 {
		t.Errorf("first allocAuthSeq start = %d, want 0", start1)
	}
	if len(bufs.AuthSeqKeys) != 2 || len(bufs.AuthSeqVals) != 2 {
		t.Errorf("len(AuthSeqKeys) = %d, len(AuthSeqVals) = %d, want 2, 2",
			len(bufs.AuthSeqKeys), len(bufs.AuthSeqVals))
	}

	start2 := bufs.allocAuthSeq(3)
	if start2 != 2 {
		t.Errorf("second allocAuthSeq start = %d, want 2", start2)
	}
	if len(bufs.AuthSeqKeys) != 5 || len(bufs.AuthSeqVals) != 5 {
		t.Errorf("len(AuthSeqKeys) = %d, len(AuthSeqVals) = %d, want 5, 5",
			len(bufs.AuthSeqKeys), len(bufs.AuthSeqVals))
	}
}

func TestBlockDecodeBuffers_recordAuthSeqRange(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		ActionAuthSeqRanges: make([][2]uint32, 0, 2),
	}

	bufs.recordAuthSeqRange(0, 3)
	if len(bufs.ActionAuthSeqRanges) != 1 {
		t.Errorf("len(ActionAuthSeqRanges) = %d, want 1", len(bufs.ActionAuthSeqRanges))
	}
	if bufs.ActionAuthSeqRanges[0] != [2]uint32{0, 3} {
		t.Errorf("ActionAuthSeqRanges[0] = %v, want [0 3]", bufs.ActionAuthSeqRanges[0])
	}

	bufs.recordAuthSeqRange(3, 2)
	bufs.recordAuthSeqRange(5, 4)
	if len(bufs.ActionAuthSeqRanges) != 3 {
		t.Errorf("len(ActionAuthSeqRanges) = %d, want 3", len(bufs.ActionAuthSeqRanges))
	}
}

func TestBlockDecodeBuffers_GetAuthSeq(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		AuthSeqKeys:         []uint64{100, 200, 300, 400, 500},
		AuthSeqVals:         []uint64{1, 2, 3, 4, 5},
		ActionAuthSeqRanges: [][2]uint32{{0, 2}, {2, 3}},
	}

	val, ok := bufs.GetAuthSeq(0, 100)
	if !ok || val != 1 {
		t.Errorf("GetAuthSeq(0, 100) = %d, %v, want 1, true", val, ok)
	}

	val, ok = bufs.GetAuthSeq(0, 200)
	if !ok || val != 2 {
		t.Errorf("GetAuthSeq(0, 200) = %d, %v, want 2, true", val, ok)
	}

	val, ok = bufs.GetAuthSeq(0, 300)
	if ok {
		t.Errorf("GetAuthSeq(0, 300) = %d, %v, want 0, false", val, ok)
	}

	val, ok = bufs.GetAuthSeq(1, 300)
	if !ok || val != 3 {
		t.Errorf("GetAuthSeq(1, 300) = %d, %v, want 3, true", val, ok)
	}

	val, ok = bufs.GetAuthSeq(1, 500)
	if !ok || val != 5 {
		t.Errorf("GetAuthSeq(1, 500) = %d, %v, want 5, true", val, ok)
	}

	val, ok = bufs.GetAuthSeq(2, 100)
	if ok {
		t.Errorf("GetAuthSeq(2, 100) = %d, %v, want 0, false (out of range)", val, ok)
	}

	val, ok = bufs.GetAuthSeq(0, 999)
	if ok {
		t.Errorf("GetAuthSeq(0, 999) = %d, %v, want 0, false (not found)", val, ok)
	}
}

func TestBlockDecodeBuffers_allocData(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		DataBuf: make([]byte, 0, 100),
	}

	data1 := bufs.allocData(50)
	if len(data1) != 50 {
		t.Errorf("allocData(50) returned slice of len %d, want 50", len(data1))
	}
	if len(bufs.DataBuf) != 50 {
		t.Errorf("len(DataBuf) = %d, want 50", len(bufs.DataBuf))
	}

	data2 := bufs.allocData(100)
	if len(data2) != 100 {
		t.Errorf("allocData(100) returned slice of len %d, want 100", len(data2))
	}
	if len(bufs.DataBuf) != 150 {
		t.Errorf("len(DataBuf) = %d, want 150", len(bufs.DataBuf))
	}
	if cap(bufs.DataBuf) < 150 {
		t.Errorf("cap(DataBuf) = %d, want >= 150", cap(bufs.DataBuf))
	}
}

func TestBlockDecodeBuffers_allocReturn(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		ReturnBuf: make([]byte, 0, 50),
	}

	ret1 := bufs.allocReturn(30)
	if len(ret1) != 30 {
		t.Errorf("allocReturn(30) returned slice of len %d, want 30", len(ret1))
	}

	ret2 := bufs.allocReturn(40)
	if len(ret2) != 40 {
		t.Errorf("allocReturn(40) returned slice of len %d, want 40", len(ret2))
	}
	if len(bufs.ReturnBuf) != 70 {
		t.Errorf("len(ReturnBuf) = %d, want 70", len(bufs.ReturnBuf))
	}
}

func TestBlockDecodeBuffers_allocTransaction(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Transactions: make([]TransactionTraceV2, 0, 2),
	}

	tx1 := bufs.allocTransaction()
	if tx1 == nil {
		t.Fatal("allocTransaction() returned nil")
	}
	if len(bufs.Transactions) != 1 {
		t.Errorf("len(Transactions) = %d, want 1", len(bufs.Transactions))
	}

	tx2 := bufs.allocTransaction()
	tx3 := bufs.allocTransaction()
	if tx2 == nil || tx3 == nil {
		t.Fatal("allocTransaction() returned nil after growth")
	}
	if len(bufs.Transactions) != 3 {
		t.Errorf("len(Transactions) = %d, want 3", len(bufs.Transactions))
	}
}

func TestBlockDecodeBuffers_allocTransactionV3(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		TransactionsV3: make([]TransactionTraceV3, 0, 2),
	}

	tx1 := bufs.allocTransactionV3()
	if tx1 == nil {
		t.Fatal("allocTransactionV3() returned nil")
	}
	if len(bufs.TransactionsV3) != 1 {
		t.Errorf("len(TransactionsV3) = %d, want 1", len(bufs.TransactionsV3))
	}

	tx2 := bufs.allocTransactionV3()
	tx3 := bufs.allocTransactionV3()
	if tx2 == nil || tx3 == nil {
		t.Fatal("allocTransactionV3() returned nil after growth")
	}
	if len(bufs.TransactionsV3) != 3 {
		t.Errorf("len(TransactionsV3) = %d, want 3", len(bufs.TransactionsV3))
	}
}

func TestBlockDecodeBuffers_allocSignature(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Signatures: make([]Signature, 0, 2),
	}

	sig1 := bufs.allocSignature()
	if sig1 == nil {
		t.Fatal("allocSignature() returned nil")
	}
	if len(bufs.Signatures) != 1 {
		t.Errorf("len(Signatures) = %d, want 1", len(bufs.Signatures))
	}

	sig2 := bufs.allocSignature()
	sig3 := bufs.allocSignature()
	if sig2 == nil || sig3 == nil {
		t.Fatal("allocSignature() returned nil after growth")
	}
	if len(bufs.Signatures) != 3 {
		t.Errorf("len(Signatures) = %d, want 3", len(bufs.Signatures))
	}
}

func TestBlockDecodeBuffers_allocSignatureData(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		SignatureData: make([]byte, 0, 100),
	}

	data1 := bufs.allocSignatureData(65)
	if len(data1) != 65 {
		t.Errorf("allocSignatureData(65) returned slice of len %d, want 65", len(data1))
	}

	data2 := bufs.allocSignatureData(65)
	if len(data2) != 65 {
		t.Errorf("allocSignatureData(65) returned slice of len %d, want 65", len(data2))
	}
	if len(bufs.SignatureData) != 130 {
		t.Errorf("len(SignatureData) = %d, want 130", len(bufs.SignatureData))
	}
}

func TestBlockDecodeBuffers_ShrinkIfNeeded(t *testing.T) {
	t.Run("no shrink when under threshold", func(t *testing.T) {
		bufs := NewBlockDecodeBuffers()
		initialDataCap := cap(bufs.DataBuf)
		bufs.ShrinkIfNeeded()
		if cap(bufs.DataBuf) != initialDataCap {
			t.Errorf("DataBuf capacity changed unexpectedly: %d -> %d", initialDataCap, cap(bufs.DataBuf))
		}
	})

	t.Run("shrink DataBuf when over threshold and empty", func(t *testing.T) {
		bufs := &BlockDecodeBuffers{
			DataBuf: make([]byte, 0, 15*1024*1024),
		}
		bufs.ShrinkIfNeeded()
		if cap(bufs.DataBuf) != 512*1024 {
			t.Errorf("DataBuf capacity = %d, want %d after shrink", cap(bufs.DataBuf), 512*1024)
		}
	})

	t.Run("shrink ReturnBuf when over threshold and empty", func(t *testing.T) {
		bufs := &BlockDecodeBuffers{
			ReturnBuf: make([]byte, 0, 2*1024*1024),
		}
		bufs.ShrinkIfNeeded()
		if cap(bufs.ReturnBuf) != 64*1024 {
			t.Errorf("ReturnBuf capacity = %d, want %d after shrink", cap(bufs.ReturnBuf), 64*1024)
		}
	})

	t.Run("shrink Actions when over threshold and empty", func(t *testing.T) {
		bufs := &BlockDecodeBuffers{
			Actions: make([]ActionTraceV1, 0, 15000),
		}
		bufs.ShrinkIfNeeded()
		if cap(bufs.Actions) != 2048 {
			t.Errorf("Actions capacity = %d, want 2048 after shrink", cap(bufs.Actions))
		}
	})

	t.Run("shrink Auths when over threshold and empty", func(t *testing.T) {
		bufs := &BlockDecodeBuffers{
			Auths: make([]AuthorizationTrace, 0, 25000),
		}
		bufs.ShrinkIfNeeded()
		if cap(bufs.Auths) != 4096 {
			t.Errorf("Auths capacity = %d, want 4096 after shrink", cap(bufs.Auths))
		}
	})

	t.Run("no shrink when capacity ratio is acceptable", func(t *testing.T) {
		bufs := &BlockDecodeBuffers{
			DataBuf: make([]byte, 5*1024*1024, 15*1024*1024),
		}
		bufs.ShrinkIfNeeded()
		if cap(bufs.DataBuf) != 15*1024*1024 {
			t.Errorf("DataBuf should not shrink when ratio is acceptable")
		}
	})
}

func TestBlockDecodeBuffers_GrowthBehavior(t *testing.T) {
	bufs := &BlockDecodeBuffers{
		Actions: make([]ActionTraceV1, 0, 0),
	}

	for i := 0; i < 100; i++ {
		bufs.allocAction()
	}

	if len(bufs.Actions) != 100 {
		t.Errorf("len(Actions) = %d, want 100", len(bufs.Actions))
	}
	if cap(bufs.Actions) < 100 {
		t.Errorf("cap(Actions) = %d, want >= 100", cap(bufs.Actions))
	}
}

func BenchmarkBlockDecodeBuffers_allocAction(b *testing.B) {
	bufs := NewBlockDecodeBuffers()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bufs.Reset()
		for j := 0; j < 1000; j++ {
			bufs.allocAction()
		}
	}
}

func BenchmarkBlockDecodeBuffers_allocData(b *testing.B) {
	bufs := NewBlockDecodeBuffers()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bufs.Reset()
		for j := 0; j < 100; j++ {
			bufs.allocData(1024)
		}
	}
}

type traceEncoder struct {
	buf []byte
}

func newTraceEncoder() *traceEncoder {
	return &traceEncoder{buf: make([]byte, 0, 4096)}
}

func (e *traceEncoder) writeByte(b byte) {
	e.buf = append(e.buf, b)
}

func (e *traceEncoder) writeUint8(v uint8) {
	e.buf = append(e.buf, v)
}

func (e *traceEncoder) writeUint16(v uint16) {
	b := make([]byte, 2)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	e.buf = append(e.buf, b...)
}

func (e *traceEncoder) writeUint32(v uint32) {
	b := make([]byte, 4)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	e.buf = append(e.buf, b...)
}

func (e *traceEncoder) writeUint64(v uint64) {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
	e.buf = append(e.buf, b...)
}

func (e *traceEncoder) writeInt64(v int64) {
	e.writeUint64(uint64(v))
}

func (e *traceEncoder) writeVarUint32(v uint32) {
	for v >= 0x80 {
		e.buf = append(e.buf, byte(v)|0x80)
		v >>= 7
	}
	e.buf = append(e.buf, byte(v))
}

func (e *traceEncoder) writeBool(v bool) {
	if v {
		e.writeByte(1)
	} else {
		e.writeByte(0)
	}
}

func (e *traceEncoder) writeChecksum256(v [32]byte) {
	e.buf = append(e.buf, v[:]...)
}

func (e *traceEncoder) writeBytes(data []byte) {
	e.buf = append(e.buf, data...)
}

func (e *traceEncoder) data() []byte {
	return e.buf
}

func encodeActionTraceV1(enc *traceEncoder, ordinal uint32, globalSeq uint64, data []byte, returnValue []byte, deltas []AccountDelta, auths []AuthorizationTrace, authSeq map[uint64]uint64) {
	enc.writeVarUint32(ordinal)
	enc.writeVarUint32(0)
	enc.writeVarUint32(0)
	enc.writeUint64(0x5530EA033C80A555)
	enc.writeUint64(globalSeq)
	enc.writeUint64(50)

	enc.writeVarUint32(uint32(len(authSeq)))
	for k, v := range authSeq {
		enc.writeUint64(k)
		enc.writeUint64(v)
	}

	enc.writeVarUint32(1)
	enc.writeVarUint32(1)
	enc.writeUint64(0x5530EA033C80A555)
	enc.writeUint64(0x00000000A89C6360)
	enc.writeUint64(0x000055404D75C140)

	enc.writeVarUint32(uint32(len(auths)))
	for _, auth := range auths {
		enc.writeUint64(auth.Account)
		enc.writeUint64(auth.Permission)
	}

	enc.writeVarUint32(uint32(len(data)))
	if len(data) > 0 {
		enc.writeBytes(data)
	}

	enc.writeBool(false)
	enc.writeInt64(1234)

	enc.writeVarUint32(uint32(len(deltas)))
	for _, d := range deltas {
		enc.writeUint64(d.Account)
		enc.writeInt64(d.Delta)
	}

	enc.writeVarUint32(uint32(len(returnValue)))
	if len(returnValue) > 0 {
		enc.writeBytes(returnValue)
	}
}

func encodeTransactionTraceV2(enc *traceEncoder, actions int, status uint8, cpuUsageUs uint32) {
	txID := [32]byte{17, 18, 19, 20}
	enc.writeChecksum256(txID)
	enc.writeVarUint32(0)
	enc.writeVarUint32(uint32(actions))

	for i := 0; i < actions; i++ {
		encodeActionTraceV1(enc, uint32(i+1), uint64(100+i),
			[]byte{0xDE, 0xAD, 0xBE, 0xEF},
			[]byte{0xCA, 0xFE, 0xBA, 0xBE},
			[]AccountDelta{{Account: 0x5530EA033C80A555, Delta: -100}},
			[]AuthorizationTrace{{Account: 0x5530EA033C80A555, Permission: 0x000055404D75C140}},
			map[uint64]uint64{0x5530EA033C80A555: 99})
	}

	enc.writeUint8(status)
	enc.writeUint32(cpuUsageUs)
	enc.writeVarUint32(100)

	enc.writeVarUint32(1)
	enc.writeVarUint32(0)
	sigData := make([]byte, 65)
	for i := range sigData {
		sigData[i] = byte(i)
	}
	enc.writeBytes(sigData)

	enc.writeUint32(1700000000)
	enc.writeUint16(1000)
	enc.writeUint32(0x12345678)
	enc.writeVarUint32(100)
	enc.writeUint8(10)
	enc.writeVarUint32(0)
}

func encodeBlockHeader(enc *traceEncoder) {
	blockID := [32]byte{1, 2, 3, 4}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{5, 6, 7, 8}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0x5530EA033C80A555)
	txMroot := [32]byte{9, 10, 11, 12}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{13, 14, 15, 16}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)
}

func TestDecodeBlockTraceV2Slice_WithV2Transactions(t *testing.T) {
	enc := newTraceEncoder()

	encodeBlockHeader(enc)

	enc.writeVarUint32(0)
	enc.writeVarUint32(1)

	encodeTransactionTraceV2(enc, 1, 0, 1000)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("DecodeBlockTraceV2Slice failed: %v", err)
	}

	if bt.Number != 12345 {
		t.Errorf("BlockNum = %d, want 12345", bt.Number)
	}
	if bt.Timestamp != 1700000000 {
		t.Errorf("Timestamp = %d, want 1700000000", bt.Timestamp)
	}
	if bt.ScheduleVersion != 1 {
		t.Errorf("ScheduleVersion = %d, want 1", bt.ScheduleVersion)
	}
	if bt.TransactionsVariant != 0 {
		t.Errorf("TransactionsVariant = %d, want 0", bt.TransactionsVariant)
	}
	if len(bt.TransactionsV2) != 1 {
		t.Fatalf("len(TransactionsV2) = %d, want 1", len(bt.TransactionsV2))
	}

	tx := bt.TransactionsV2[0]
	if tx.Status != 0 {
		t.Errorf("tx.Status = %d, want 0", tx.Status)
	}
	if tx.CpuUsageUs != 1000 {
		t.Errorf("tx.CpuUsageUs = %d, want 1000", tx.CpuUsageUs)
	}
	if len(tx.Actions) != 1 {
		t.Fatalf("len(tx.Actions) = %d, want 1", len(tx.Actions))
	}
	if len(tx.Signatures) != 1 {
		t.Fatalf("len(tx.Signatures) = %d, want 1", len(tx.Signatures))
	}

	act := tx.Actions[0]
	if act.ActionOrdinal != 1 {
		t.Errorf("act.ActionOrdinal = %d, want 1", act.ActionOrdinal)
	}
	if act.GlobalSequence != 100 {
		t.Errorf("act.GlobalSequence = %d, want 100", act.GlobalSequence)
	}
	if len(act.Data) != 4 {
		t.Errorf("len(act.Data) = %d, want 4", len(act.Data))
	}
	if act.Elapsed != 1234 {
		t.Errorf("act.Elapsed = %d, want 1234", act.Elapsed)
	}
	if len(act.ReturnValue) != 4 {
		t.Errorf("len(act.ReturnValue) = %d, want 4", len(act.ReturnValue))
	}
	if len(act.AccountRamDeltas) != 1 {
		t.Errorf("len(act.AccountRamDeltas) = %d, want 1", len(act.AccountRamDeltas))
	}
	if act.AccountRamDeltas[0].Delta != -100 {
		t.Errorf("act.AccountRamDeltas[0].Delta = %d, want -100", act.AccountRamDeltas[0].Delta)
	}

	sig := tx.Signatures[0]
	if sig.Type != 0 {
		t.Errorf("sig.Type = %d, want 0 (K1)", sig.Type)
	}
	if len(sig.Data) != 65 {
		t.Errorf("len(sig.Data) = %d, want 65", len(sig.Data))
	}
}

func TestDecodeBlockTraceV2Slice_WithV3Transactions(t *testing.T) {
	enc := newTraceEncoder()

	encodeBlockHeader(enc)

	enc.writeVarUint32(1)
	enc.writeVarUint32(1)

	encodeTransactionTraceV2(enc, 1, 0, 500)

	enc.writeUint32(12345)
	enc.writeUint32(1700000000)

	enc.writeBool(true)
	producerBlockID := [32]byte{21, 22, 23, 24}
	enc.writeChecksum256(producerBlockID)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("DecodeBlockTraceV2Slice failed: %v", err)
	}

	if bt.TransactionsVariant != 1 {
		t.Errorf("TransactionsVariant = %d, want 1", bt.TransactionsVariant)
	}
	if len(bt.TransactionsV3) != 1 {
		t.Fatalf("len(TransactionsV3) = %d, want 1", len(bt.TransactionsV3))
	}

	tx := bt.TransactionsV3[0]
	if tx.BlockNum != 12345 {
		t.Errorf("tx.BlockNum = %d, want 12345", tx.BlockNum)
	}
	if tx.BlockTime != 1700000000 {
		t.Errorf("tx.BlockTime = %d, want 1700000000", tx.BlockTime)
	}
	if tx.ProducerBlockID == nil {
		t.Error("tx.ProducerBlockID should not be nil")
	} else if tx.ProducerBlockID[0] != 21 {
		t.Errorf("tx.ProducerBlockID[0] = %d, want 21", tx.ProducerBlockID[0])
	}
}

func TestDecodeBlockTraceV2Slice_V3WithoutProducerBlockID(t *testing.T) {
	enc := newTraceEncoder()

	encodeBlockHeader(enc)

	enc.writeVarUint32(1)
	enc.writeVarUint32(1)

	encodeTransactionTraceV2(enc, 0, 0, 100)

	enc.writeUint32(12345)
	enc.writeUint32(1700000000)

	enc.writeBool(false)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("DecodeBlockTraceV2Slice failed: %v", err)
	}

	if len(bt.TransactionsV3) != 1 {
		t.Fatalf("len(TransactionsV3) = %d, want 1", len(bt.TransactionsV3))
	}

	tx := bt.TransactionsV3[0]
	if tx.ProducerBlockID != nil {
		t.Error("tx.ProducerBlockID should be nil")
	}
}

func TestDecodeBlockTraceV2Slice_UnsupportedTransactionsVariant(t *testing.T) {
	enc := newTraceEncoder()

	blockID := [32]byte{}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0)
	txMroot := [32]byte{}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)

	enc.writeVarUint32(99)
	enc.writeVarUint32(1)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	_, err := DecodeBlockTraceV2Slice(d, bufs)
	if err == nil {
		t.Error("expected error for unsupported transactions variant")
	}
	if err.Error() != "unsupported transactions variant: 99" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDecodeBlockTraceV2Slice_SignatureTypes(t *testing.T) {
	t.Run("K1 signature", func(t *testing.T) {
		enc := newTraceEncoder()
		writeMinimalBlockWithSignature(enc, 0, nil)

		d := NewSliceDecoder(enc.data())
		bufs := NewBlockDecodeBuffers()

		bt, err := DecodeBlockTraceV2Slice(d, bufs)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		sig := bt.TransactionsV2[0].Signatures[0]
		if sig.Type != 0 {
			t.Errorf("sig.Type = %d, want 0 (K1)", sig.Type)
		}
		if len(sig.Data) != 65 {
			t.Errorf("len(sig.Data) = %d, want 65", len(sig.Data))
		}
	})

	t.Run("R1 signature", func(t *testing.T) {
		enc := newTraceEncoder()
		writeMinimalBlockWithSignature(enc, 1, nil)

		d := NewSliceDecoder(enc.data())
		bufs := NewBlockDecodeBuffers()

		bt, err := DecodeBlockTraceV2Slice(d, bufs)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		sig := bt.TransactionsV2[0].Signatures[0]
		if sig.Type != 1 {
			t.Errorf("sig.Type = %d, want 1 (R1)", sig.Type)
		}
		if len(sig.Data) != 65 {
			t.Errorf("len(sig.Data) = %d, want 65", len(sig.Data))
		}
	})

	t.Run("WebAuthn signature", func(t *testing.T) {
		webAuthnData := &webAuthnTestData{
			signature:  make([]byte, 65),
			authData:   []byte("auth data here"),
			clientData: []byte("client data json"),
		}
		for i := range webAuthnData.signature {
			webAuthnData.signature[i] = byte(i)
		}

		enc := newTraceEncoder()
		writeMinimalBlockWithSignature(enc, 2, webAuthnData)

		d := NewSliceDecoder(enc.data())
		bufs := NewBlockDecodeBuffers()

		bt, err := DecodeBlockTraceV2Slice(d, bufs)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		sig := bt.TransactionsV2[0].Signatures[0]
		if sig.Type != 2 {
			t.Errorf("sig.Type = %d, want 2 (WebAuthn)", sig.Type)
		}
		if sig.WebAuthn == nil {
			t.Fatal("sig.WebAuthn should not be nil")
		}
		if len(sig.WebAuthn.Signature) != 65 {
			t.Errorf("len(sig.WebAuthn.Signature) = %d, want 65", len(sig.WebAuthn.Signature))
		}
		if string(sig.WebAuthn.AuthData) != "auth data here" {
			t.Errorf("sig.WebAuthn.AuthData = %q, want 'auth data here'", sig.WebAuthn.AuthData)
		}
		if string(sig.WebAuthn.ClientData) != "client data json" {
			t.Errorf("sig.WebAuthn.ClientData = %q, want 'client data json'", sig.WebAuthn.ClientData)
		}
	})
}

type webAuthnTestData struct {
	signature  []byte
	authData   []byte
	clientData []byte
}

func writeMinimalBlockWithSignature(enc *traceEncoder, sigType uint32, webAuthn *webAuthnTestData) {
	blockID := [32]byte{}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0)
	txMroot := [32]byte{}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)

	enc.writeVarUint32(0)
	enc.writeVarUint32(1)

	txID := [32]byte{}
	enc.writeChecksum256(txID)
	enc.writeVarUint32(0)
	enc.writeVarUint32(0)
	enc.writeUint8(0)
	enc.writeUint32(100)
	enc.writeVarUint32(10)

	enc.writeVarUint32(1)
	enc.writeVarUint32(sigType)

	if sigType == 0 || sigType == 1 {
		sigData := make([]byte, 65)
		enc.writeBytes(sigData)
	} else if sigType == 2 && webAuthn != nil {
		enc.writeBytes(webAuthn.signature)
		enc.writeVarUint32(uint32(len(webAuthn.authData)))
		enc.writeBytes(webAuthn.authData)
		enc.writeVarUint32(uint32(len(webAuthn.clientData)))
		enc.writeBytes(webAuthn.clientData)
	}

	enc.writeUint32(1700000000)
	enc.writeUint16(1000)
	enc.writeUint32(0x12345678)
	enc.writeVarUint32(100)
	enc.writeUint8(10)
	enc.writeVarUint32(0)
}

func TestDecodeBlockTraceV2Slice_MultipleTransactionsAndActions(t *testing.T) {
	enc := newTraceEncoder()

	encodeBlockHeader(enc)

	enc.writeVarUint32(0)
	enc.writeVarUint32(2)

	encodeTransactionTraceV2(enc, 1, 0, 0)

	encodeTransactionTraceV2(enc, 2, 0, 100)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(bt.TransactionsV2) != 2 {
		t.Fatalf("len(TransactionsV2) = %d, want 2", len(bt.TransactionsV2))
	}

	if len(bt.TransactionsV2[0].Actions) != 1 {
		t.Errorf("tx[0] actions = %d, want 1", len(bt.TransactionsV2[0].Actions))
	}
	if len(bt.TransactionsV2[1].Actions) != 2 {
		t.Errorf("tx[1] actions = %d, want 2", len(bt.TransactionsV2[1].Actions))
	}

	if bt.TransactionsV2[0].CpuUsageUs != 0 {
		t.Errorf("tx[0].CpuUsageUs = %d, want 0", bt.TransactionsV2[0].CpuUsageUs)
	}
	if bt.TransactionsV2[1].CpuUsageUs != 100 {
		t.Errorf("tx[1].CpuUsageUs = %d, want 100", bt.TransactionsV2[1].CpuUsageUs)
	}
}

func TestDecodeBlockTraceV2Slice_ZeroTransactions(t *testing.T) {
	enc := newTraceEncoder()

	blockID := [32]byte{}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0)
	txMroot := [32]byte{}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)

	enc.writeVarUint32(0)
	enc.writeVarUint32(0)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(bt.TransactionsV2) != 0 {
		t.Errorf("len(TransactionsV2) = %d, want 0", len(bt.TransactionsV2))
	}
}

func TestDecodeBlockTraceV2Slice_WebAuthnEmptyOptionalFields(t *testing.T) {
	webAuthnData := &webAuthnTestData{
		signature:  make([]byte, 65),
		authData:   []byte{},
		clientData: []byte{},
	}

	enc := newTraceEncoder()

	blockID := [32]byte{}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0)
	txMroot := [32]byte{}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)

	enc.writeVarUint32(0)
	enc.writeVarUint32(1)

	txID := [32]byte{}
	enc.writeChecksum256(txID)
	enc.writeVarUint32(0)
	enc.writeVarUint32(0)
	enc.writeUint8(0)
	enc.writeUint32(100)
	enc.writeVarUint32(10)

	enc.writeVarUint32(1)
	enc.writeVarUint32(2)
	enc.writeBytes(webAuthnData.signature)
	enc.writeVarUint32(0)
	enc.writeVarUint32(0)

	enc.writeUint32(1700000000)
	enc.writeUint16(1000)
	enc.writeUint32(0x12345678)
	enc.writeVarUint32(100)
	enc.writeUint8(10)
	enc.writeVarUint32(0)

	d := NewSliceDecoder(enc.data())
	bufs := NewBlockDecodeBuffers()

	bt, err := DecodeBlockTraceV2Slice(d, bufs)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	sig := bt.TransactionsV2[0].Signatures[0]
	if sig.WebAuthn == nil {
		t.Fatal("sig.WebAuthn should not be nil")
	}
	if len(sig.WebAuthn.AuthData) != 0 {
		t.Errorf("AuthData should be empty, got len=%d", len(sig.WebAuthn.AuthData))
	}
	if len(sig.WebAuthn.ClientData) != 0 {
		t.Errorf("ClientData should be empty, got len=%d", len(sig.WebAuthn.ClientData))
	}
}

func BenchmarkDecodeBlockTraceV2Slice(b *testing.B) {
	enc := newTraceEncoder()

	blockID := [32]byte{}
	enc.writeChecksum256(blockID)
	enc.writeUint32(12345)
	prevID := [32]byte{}
	enc.writeChecksum256(prevID)
	enc.writeUint32(1700000000)
	enc.writeUint64(0)
	txMroot := [32]byte{}
	enc.writeChecksum256(txMroot)
	actMroot := [32]byte{}
	enc.writeChecksum256(actMroot)
	enc.writeUint32(1)

	enc.writeVarUint32(0)
	enc.writeVarUint32(10)

	for i := 0; i < 10; i++ {
		txID := [32]byte{byte(i)}
		enc.writeChecksum256(txID)
		enc.writeVarUint32(0)
		enc.writeVarUint32(5)

		for j := 0; j < 5; j++ {
			enc.writeVarUint32(uint32(j + 1))
			enc.writeVarUint32(0)
			enc.writeVarUint32(0)
			enc.writeUint64(0x5530EA033C80A555)
			enc.writeUint64(uint64(i*5 + j))
			enc.writeUint64(uint64(j))

			enc.writeVarUint32(2)
			enc.writeUint64(1)
			enc.writeUint64(100)
			enc.writeUint64(2)
			enc.writeUint64(200)

			enc.writeVarUint32(1)
			enc.writeVarUint32(1)
			enc.writeUint64(0x5530EA033C80A555)
			enc.writeUint64(0x00000000A89C6360)

			enc.writeVarUint32(1)
			enc.writeUint64(0x5530EA033C80A555)
			enc.writeUint64(0x000055404D75C140)

			actionData := make([]byte, 100)
			enc.writeVarUint32(100)
			enc.writeBytes(actionData)

			enc.writeBool(false)
			enc.writeInt64(100)

			enc.writeVarUint32(1)
			enc.writeUint64(0x5530EA033C80A555)
			enc.writeInt64(50)

			enc.writeVarUint32(10)
			returnData := make([]byte, 10)
			enc.writeBytes(returnData)
		}

		enc.writeUint8(0)
		enc.writeUint32(1000)
		enc.writeVarUint32(100)

		enc.writeVarUint32(1)
		enc.writeVarUint32(0)
		sigData := make([]byte, 65)
		enc.writeBytes(sigData)

		enc.writeUint32(1700000000)
		enc.writeUint16(1000)
		enc.writeUint32(0x12345678)
		enc.writeVarUint32(100)
		enc.writeUint8(10)
		enc.writeVarUint32(0)
	}

	data := enc.data()
	bufs := NewBlockDecodeBuffers()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bufs.Reset()
		d := NewSliceDecoder(data)
		_, _ = DecodeBlockTraceV2Slice(d, bufs)
	}
}
