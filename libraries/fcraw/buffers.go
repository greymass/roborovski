package fcraw

type BlockDecodeBuffers struct {
	Transactions   []TransactionTraceV2
	TransactionsV3 []TransactionTraceV3
	Signatures     []Signature

	Actions []ActionTraceV1
	Auths   []AuthorizationTrace
	Deltas  []AccountDelta

	AuthSeqKeys []uint64
	AuthSeqVals []uint64

	ActionAuthSeqRanges [][2]uint32

	DataBuf       []byte // action Data
	ReturnBuf     []byte // ActionTraceV1 ReturnValue
	SignatureData []byte // signature bytes
}

func NewBlockDecodeBuffers() *BlockDecodeBuffers {
	return &BlockDecodeBuffers{
		Transactions:        make([]TransactionTraceV2, 0, 32),
		TransactionsV3:      make([]TransactionTraceV3, 0, 32),
		Signatures:          make([]Signature, 0, 32),
		Actions:             make([]ActionTraceV1, 0, 2048),
		Auths:               make([]AuthorizationTrace, 0, 4096),
		Deltas:              make([]AccountDelta, 0, 1024),
		AuthSeqKeys:         make([]uint64, 0, 4096),
		AuthSeqVals:         make([]uint64, 0, 4096),
		ActionAuthSeqRanges: make([][2]uint32, 0, 2048),
		DataBuf:             make([]byte, 0, 512*1024), // 512KB initial
		ReturnBuf:           make([]byte, 0, 64*1024),  // 64KB initial
		SignatureData:       make([]byte, 0, 8*1024),   // 8KB initial
	}
}

func (b *BlockDecodeBuffers) Reset() {
	b.Transactions = b.Transactions[:0]
	b.TransactionsV3 = b.TransactionsV3[:0]
	b.Signatures = b.Signatures[:0]
	b.Actions = b.Actions[:0]
	b.Auths = b.Auths[:0]
	b.Deltas = b.Deltas[:0]
	b.AuthSeqKeys = b.AuthSeqKeys[:0]
	b.AuthSeqVals = b.AuthSeqVals[:0]
	b.ActionAuthSeqRanges = b.ActionAuthSeqRanges[:0]
	b.DataBuf = b.DataBuf[:0]
	b.ReturnBuf = b.ReturnBuf[:0]
	b.SignatureData = b.SignatureData[:0]
}

func (b *BlockDecodeBuffers) CapacityBytes() int64 {
	const (
		txV2Size  = 200 // TransactionTraceV2 with embedded slices
		txV3Size  = 220 // TransactionTraceV3
		sigSize   = 80  // Signature
		actSize   = 300 // ActionTraceV1 with embedded slices
		authSize  = 16  // AuthorizationTrace (2 uint64)
		deltaSize = 16  // AccountDelta (uint64 + int64)
		rangeSize = 8   // [2]uint32
	)

	return int64(cap(b.Transactions))*txV2Size +
		int64(cap(b.TransactionsV3))*txV3Size +
		int64(cap(b.Signatures))*sigSize +
		int64(cap(b.Actions))*actSize +
		int64(cap(b.Auths))*authSize +
		int64(cap(b.Deltas))*deltaSize +
		int64(cap(b.AuthSeqKeys))*8 +
		int64(cap(b.AuthSeqVals))*8 +
		int64(cap(b.ActionAuthSeqRanges))*rangeSize +
		int64(cap(b.DataBuf)) +
		int64(cap(b.ReturnBuf)) +
		int64(cap(b.SignatureData))
}

func (b *BlockDecodeBuffers) ShrinkIfNeeded() {
	const (
		dataBufThreshold   = 10 * 1024 * 1024
		returnBufThreshold = 1 * 1024 * 1024
		actionsThreshold   = 10000
		authsThreshold     = 20000
		maxCapacityRatio   = 10
	)

	if cap(b.DataBuf) > dataBufThreshold {
		if len(b.DataBuf) == 0 || cap(b.DataBuf)/len(b.DataBuf) > maxCapacityRatio {
			b.DataBuf = make([]byte, 0, 512*1024)
		}
	}

	if cap(b.ReturnBuf) > returnBufThreshold {
		if len(b.ReturnBuf) == 0 || cap(b.ReturnBuf)/len(b.ReturnBuf) > maxCapacityRatio {
			b.ReturnBuf = make([]byte, 0, 64*1024)
		}
	}

	if cap(b.Actions) > actionsThreshold {
		if len(b.Actions) == 0 || cap(b.Actions)/len(b.Actions) > maxCapacityRatio {
			b.Actions = make([]ActionTraceV1, 0, 2048)
		}
	}

	if cap(b.Auths) > authsThreshold {
		if len(b.Auths) == 0 || cap(b.Auths)/len(b.Auths) > maxCapacityRatio {
			b.Auths = make([]AuthorizationTrace, 0, 4096)
		}
	}
}

func (b *BlockDecodeBuffers) allocAction() *ActionTraceV1 {
	n := len(b.Actions)
	if n >= cap(b.Actions) {
		newCap := cap(b.Actions) * 3 / 2
		if newCap < 64 {
			newCap = 64
		}
		newBuf := make([]ActionTraceV1, n, newCap)
		copy(newBuf, b.Actions)
		b.Actions = newBuf
	}
	b.Actions = b.Actions[:n+1]
	return &b.Actions[n]
}

func (b *BlockDecodeBuffers) allocAuths(count uint32) []AuthorizationTrace {
	start := len(b.Auths)
	end := start + int(count)
	if end > cap(b.Auths) {
		newCap := cap(b.Auths) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]AuthorizationTrace, start, newCap)
		copy(newBuf, b.Auths)
		b.Auths = newBuf
	}
	b.Auths = b.Auths[:end]
	return b.Auths[start:end]
}

func (b *BlockDecodeBuffers) allocDeltas(count uint32) []AccountDelta {
	start := len(b.Deltas)
	end := start + int(count)
	if end > cap(b.Deltas) {
		newCap := cap(b.Deltas) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]AccountDelta, start, newCap)
		copy(newBuf, b.Deltas)
		b.Deltas = newBuf
	}
	b.Deltas = b.Deltas[:end]
	return b.Deltas[start:end]
}

func (b *BlockDecodeBuffers) allocAuthSeq(count uint32) (start uint32) {
	start = uint32(len(b.AuthSeqKeys))
	end := int(start) + int(count)
	if end > cap(b.AuthSeqKeys) {
		newCap := cap(b.AuthSeqKeys) * 2
		if newCap < end {
			newCap = end * 2
		}
		newKeys := make([]uint64, start, newCap)
		newVals := make([]uint64, start, newCap)
		copy(newKeys, b.AuthSeqKeys)
		copy(newVals, b.AuthSeqVals)
		b.AuthSeqKeys = newKeys
		b.AuthSeqVals = newVals
	}
	b.AuthSeqKeys = b.AuthSeqKeys[:end]
	b.AuthSeqVals = b.AuthSeqVals[:end]
	return start
}

func (b *BlockDecodeBuffers) recordAuthSeqRange(start, count uint32) {
	n := len(b.ActionAuthSeqRanges)
	if n >= cap(b.ActionAuthSeqRanges) {
		newCap := cap(b.ActionAuthSeqRanges) * 3 / 2
		if newCap < 64 {
			newCap = 64
		}
		newBuf := make([][2]uint32, n, newCap)
		copy(newBuf, b.ActionAuthSeqRanges)
		b.ActionAuthSeqRanges = newBuf
	}
	b.ActionAuthSeqRanges = b.ActionAuthSeqRanges[:n+1]
	b.ActionAuthSeqRanges[n] = [2]uint32{start, count}
}

func (b *BlockDecodeBuffers) GetAuthSeq(actionIdx int, account uint64) (uint64, bool) {
	if actionIdx >= len(b.ActionAuthSeqRanges) {
		return 0, false
	}
	r := b.ActionAuthSeqRanges[actionIdx]
	start, count := r[0], r[1]
	for i := uint32(0); i < count; i++ {
		if b.AuthSeqKeys[start+i] == account {
			return b.AuthSeqVals[start+i], true
		}
	}
	return 0, false
}

func (b *BlockDecodeBuffers) allocData(size uint32) []byte {
	start := len(b.DataBuf)
	end := start + int(size)
	if end > cap(b.DataBuf) {
		newCap := cap(b.DataBuf) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]byte, start, newCap)
		copy(newBuf, b.DataBuf)
		b.DataBuf = newBuf
	}
	b.DataBuf = b.DataBuf[:end]
	return b.DataBuf[start:end]
}

func (b *BlockDecodeBuffers) allocReturn(size uint32) []byte {
	start := len(b.ReturnBuf)
	end := start + int(size)
	if end > cap(b.ReturnBuf) {
		newCap := cap(b.ReturnBuf) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]byte, start, newCap)
		copy(newBuf, b.ReturnBuf)
		b.ReturnBuf = newBuf
	}
	b.ReturnBuf = b.ReturnBuf[:end]
	return b.ReturnBuf[start:end]
}

func (b *BlockDecodeBuffers) allocTransaction() *TransactionTraceV2 {
	n := len(b.Transactions)
	if n >= cap(b.Transactions) {
		newCap := cap(b.Transactions) * 2
		if newCap < 16 {
			newCap = 16
		}
		newBuf := make([]TransactionTraceV2, n, newCap)
		copy(newBuf, b.Transactions)
		b.Transactions = newBuf
	}
	b.Transactions = b.Transactions[:n+1]
	return &b.Transactions[n]
}

func (b *BlockDecodeBuffers) allocTransactionV3() *TransactionTraceV3 {
	n := len(b.TransactionsV3)
	if n >= cap(b.TransactionsV3) {
		newCap := cap(b.TransactionsV3) * 2
		if newCap < 16 {
			newCap = 16
		}
		newBuf := make([]TransactionTraceV3, n, newCap)
		copy(newBuf, b.TransactionsV3)
		b.TransactionsV3 = newBuf
	}
	b.TransactionsV3 = b.TransactionsV3[:n+1]
	return &b.TransactionsV3[n]
}

func (b *BlockDecodeBuffers) allocSignature() *Signature {
	n := len(b.Signatures)
	if n >= cap(b.Signatures) {
		newCap := cap(b.Signatures) * 2
		if newCap < 16 {
			newCap = 16
		}
		newBuf := make([]Signature, n, newCap)
		copy(newBuf, b.Signatures)
		b.Signatures = newBuf
	}
	b.Signatures = b.Signatures[:n+1]
	return &b.Signatures[n]
}

func (b *BlockDecodeBuffers) allocSignatureData(size int) []byte {
	start := len(b.SignatureData)
	end := start + size
	if end > cap(b.SignatureData) {
		newCap := cap(b.SignatureData) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]byte, start, newCap)
		copy(newBuf, b.SignatureData)
		b.SignatureData = newBuf
	}
	b.SignatureData = b.SignatureData[:end]
	return b.SignatureData[start:end]
}
