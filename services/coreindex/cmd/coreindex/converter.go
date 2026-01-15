package main

import (
	"github.com/greymass/roborovski/libraries/fcraw"
)

type actionTraceOptimized struct {
	// Core action fields
	ActionOrdinal uint32
	CreatorAO     uint32
	ClosestUAAO   uint32
	ContextFree   bool
	Elapsed       int64

	// Receipt fields (only the ones we need)
	CodeSequence uint32
	AbiSequence  uint32

	// Block context (for header validation)
	BlockNum  uint32
	BlockTime string

	// Raw Name values (avoid string conversions)
	ReceiverName        uint64
	ReceiptReceiverName uint64
	AccountName         uint64
	ActionName          uint64

	// Auth data (parallel arrays)
	AuthActorNames []uint64
	AuthPermNames  []uint64
	AuthSeqs       []uint64

	// RAM deltas (parallel arrays)
	RAMDeltaAccNames []uint64
	RAMDeltaAmounts  []int64

	// Raw binary data
	ActionData         []byte
	TrxIDRaw           [32]byte
	ProducerBlockIDRaw [32]byte

	// Sequence numbers
	GlobalSequence uint64
	RecvSequence   uint64
}

type WorkerBuffers struct {
	*fcraw.BlockDecodeBuffers

	ActionsBuffer []actionTraceOptimized

	AuthsBuffer  []authTriple
	DeltasBuffer []accountDelta

	NamesIndex map[uint64]uint32
	DataIndex  map[string]uint32

	AuthActorPool []uint64
	AuthPermPool  []uint64
	AuthSeqPool   []uint64
	RAMAccPool    []uint64
	RAMAmtPool    []int64
}

func NewWorkerBuffers() *WorkerBuffers {
	return &WorkerBuffers{
		BlockDecodeBuffers: fcraw.NewBlockDecodeBuffers(),
		ActionsBuffer:      make([]actionTraceOptimized, 0, 8192),
		AuthsBuffer:        make([]authTriple, 0, 16384),
		DeltasBuffer:       make([]accountDelta, 0, 4096),
		NamesIndex:         make(map[uint64]uint32, 256),
		DataIndex:          make(map[string]uint32, 4096),
		AuthActorPool:      make([]uint64, 0, 16384),
		AuthPermPool:       make([]uint64, 0, 16384),
		AuthSeqPool:        make([]uint64, 0, 16384),
		RAMAccPool:         make([]uint64, 0, 4096),
		RAMAmtPool:         make([]int64, 0, 4096),
	}
}

func (w *WorkerBuffers) ResetConverterPools() {
	w.AuthActorPool = w.AuthActorPool[:0]
	w.AuthPermPool = w.AuthPermPool[:0]
	w.AuthSeqPool = w.AuthSeqPool[:0]
	w.RAMAccPool = w.RAMAccPool[:0]
	w.RAMAmtPool = w.RAMAmtPool[:0]
}

func (w *WorkerBuffers) AllocAuthActors(n int) []uint64 {
	start := len(w.AuthActorPool)
	end := start + n
	if end > cap(w.AuthActorPool) {
		newCap := cap(w.AuthActorPool) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]uint64, start, newCap)
		copy(newBuf, w.AuthActorPool)
		w.AuthActorPool = newBuf
	}
	w.AuthActorPool = w.AuthActorPool[:end]
	return w.AuthActorPool[start:end]
}

func (w *WorkerBuffers) AllocAuthPerms(n int) []uint64 {
	start := len(w.AuthPermPool)
	end := start + n
	if end > cap(w.AuthPermPool) {
		newCap := cap(w.AuthPermPool) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]uint64, start, newCap)
		copy(newBuf, w.AuthPermPool)
		w.AuthPermPool = newBuf
	}
	w.AuthPermPool = w.AuthPermPool[:end]
	return w.AuthPermPool[start:end]
}

func (w *WorkerBuffers) AllocAuthSeqs(n int) []uint64 {
	start := len(w.AuthSeqPool)
	end := start + n
	if end > cap(w.AuthSeqPool) {
		newCap := cap(w.AuthSeqPool) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]uint64, start, newCap)
		copy(newBuf, w.AuthSeqPool)
		w.AuthSeqPool = newBuf
	}
	w.AuthSeqPool = w.AuthSeqPool[:end]
	return w.AuthSeqPool[start:end]
}

func (w *WorkerBuffers) AllocRAMAccounts(n int) []uint64 {
	start := len(w.RAMAccPool)
	end := start + n
	if end > cap(w.RAMAccPool) {
		newCap := cap(w.RAMAccPool) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]uint64, start, newCap)
		copy(newBuf, w.RAMAccPool)
		w.RAMAccPool = newBuf
	}
	w.RAMAccPool = w.RAMAccPool[:end]
	return w.RAMAccPool[start:end]
}

func (w *WorkerBuffers) AllocRAMAmounts(n int) []int64 {
	start := len(w.RAMAmtPool)
	end := start + n
	if end > cap(w.RAMAmtPool) {
		newCap := cap(w.RAMAmtPool) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]int64, start, newCap)
		copy(newBuf, w.RAMAmtPool)
		w.RAMAmtPool = newBuf
	}
	w.RAMAmtPool = w.RAMAmtPool[:end]
	return w.RAMAmtPool[start:end]
}

func (w *WorkerBuffers) AllocActions(n int) []actionTraceOptimized {
	w.ActionsBuffer = w.ActionsBuffer[:0]
	if cap(w.ActionsBuffer) < n {
		w.ActionsBuffer = make([]actionTraceOptimized, 0, n)
	}
	return w.ActionsBuffer
}

func (w *WorkerBuffers) ResetCompressBuffers() {
	w.AuthsBuffer = w.AuthsBuffer[:0]
	w.DeltasBuffer = w.DeltasBuffer[:0]
	clear(w.NamesIndex)
	clear(w.DataIndex)
}

func (w *WorkerBuffers) AllocAuths(n int) []authTriple {
	start := len(w.AuthsBuffer)
	end := start + n
	if end > cap(w.AuthsBuffer) {
		newCap := cap(w.AuthsBuffer) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]authTriple, start, newCap)
		copy(newBuf, w.AuthsBuffer)
		w.AuthsBuffer = newBuf
	}
	w.AuthsBuffer = w.AuthsBuffer[:end]
	return w.AuthsBuffer[start:end]
}

func (w *WorkerBuffers) AllocDeltas(n int) []accountDelta {
	start := len(w.DeltasBuffer)
	end := start + n
	if end > cap(w.DeltasBuffer) {
		newCap := cap(w.DeltasBuffer) * 2
		if newCap < end {
			newCap = end * 2
		}
		newBuf := make([]accountDelta, start, newCap)
		copy(newBuf, w.DeltasBuffer)
		w.DeltasBuffer = newBuf
	}
	w.DeltasBuffer = w.DeltasBuffer[:end]
	return w.DeltasBuffer[start:end]
}
