package corereader

import (
	"sync"
	"sync/atomic"
	_ "unsafe" // Required for go:linkname
)

// FilterTiming tracks time spent in each phase of filtering
type FilterTiming struct {
	MetaLookupNs  atomic.Int64
	GsCanonicalNs atomic.Int64
	OrdinalRootNs atomic.Int64
	FamilySlotsNs atomic.Int64
	NotifsLoopNs  atomic.Int64
	ResetNs       atomic.Int64
	CallCount     atomic.Int64

	// Notifs loop path counters
	ReceiverHits   atomic.Int64
	AuthorizerHits atomic.Int64
	Skipped        atomic.Int64
}

func (ft *FilterTiming) Reset() {
	ft.MetaLookupNs.Store(0)
	ft.GsCanonicalNs.Store(0)
	ft.OrdinalRootNs.Store(0)
	ft.FamilySlotsNs.Store(0)
	ft.NotifsLoopNs.Store(0)
	ft.ResetNs.Store(0)
	ft.CallCount.Store(0)
	ft.ReceiverHits.Store(0)
	ft.AuthorizerHits.Store(0)
	ft.Skipped.Store(0)
}

// GetBreakdown returns percentages for each phase
func (ft *FilterTiming) GetBreakdown() (metaPct, gsPct, ordPct, famPct, notifPct, resetPct float64) {
	total := ft.MetaLookupNs.Load() + ft.GsCanonicalNs.Load() + ft.OrdinalRootNs.Load() +
		ft.FamilySlotsNs.Load() + ft.NotifsLoopNs.Load() + ft.ResetNs.Load()
	if total == 0 {
		return
	}
	metaPct = float64(ft.MetaLookupNs.Load()) / float64(total) * 100
	gsPct = float64(ft.GsCanonicalNs.Load()) / float64(total) * 100
	ordPct = float64(ft.OrdinalRootNs.Load()) / float64(total) * 100
	famPct = float64(ft.FamilySlotsNs.Load()) / float64(total) * 100
	notifPct = float64(ft.NotifsLoopNs.Load()) / float64(total) * 100
	resetPct = float64(ft.ResetNs.Load()) / float64(total) * 100
	return
}

// makeTrxOrdinalKey creates key for trx/ordinal lookup
func makeTrxOrdinalKey(trxIdx, ordinal uint32) uint64 {
	return uint64(trxIdx)<<32 | uint64(ordinal)
}

func makeFamilyKeyHash(trxIdx, rootOrdinal, dataIndex uint32, contract, action uint64) uint64 {
	h := uint64(trxIdx)
	h = h*31 + uint64(rootOrdinal)
	h = h*31 + contract
	h = h*31 + action
	h = h*31 + uint64(dataIndex)
	return h
}

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// combinedActionInfo merges ActionMetadata and CanonicalAction data for efficient lookup
type combinedActionInfo struct {
	Meta     ActionMetadata
	Action   *CanonicalAction
	Receiver uint64
	TrxIndex uint32
}

type blockFilter struct {
	// Slice-backed lookup: avoids struct copy on every map access
	infoSlice      []combinedActionInfo
	globalSeqToIdx map[uint64]int32
	minGlobalSeq   uint64 // For direct index calculation: idx = globalSeq - minGlobalSeq
	ordinalToRoot  map[uint64]uint32
	familyToSlot   map[uint64]int
	receiverSlots  [][]uint64
	receiverPool   [][]uint64
	familyBuilt    bool
	useDirectIdx   bool // True if globalSeqs are contiguous (common case)
}

var blockFilterPool = sync.Pool{
	New: func() interface{} {
		bf := &blockFilter{
			infoSlice:      make([]combinedActionInfo, 0, 512),
			globalSeqToIdx: make(map[uint64]int32, 512),
			ordinalToRoot:  make(map[uint64]uint32, 512),
			familyToSlot:   make(map[uint64]int, 256),
			receiverSlots:  make([][]uint64, 0, 256),
			receiverPool:   make([][]uint64, 256),
		}
		for i := range bf.receiverPool {
			bf.receiverPool[i] = make([]uint64, 0, 8)
		}
		return bf
	},
}

func (bf *blockFilter) reset() {
	bf.infoSlice = bf.infoSlice[:0]
	if !bf.useDirectIdx {
		clear(bf.globalSeqToIdx)
	}
	bf.useDirectIdx = false
	if len(bf.ordinalToRoot) > 0 {
		clear(bf.ordinalToRoot)
	}
	if bf.familyBuilt {
		clear(bf.familyToSlot)
		for i := range bf.receiverSlots {
			bf.receiverSlots[i] = bf.receiverSlots[i][:0]
		}
		bf.receiverSlots = bf.receiverSlots[:0]
		bf.familyBuilt = false
	}
}

func (bf *blockFilter) getReceiverSlot() []uint64 {
	idx := len(bf.receiverSlots)
	if idx < len(bf.receiverPool) {
		bf.receiverSlots = append(bf.receiverSlots, bf.receiverPool[idx][:0])
		return bf.receiverSlots[idx]
	}
	newSlice := make([]uint64, 0, 8)
	bf.receiverPool = append(bf.receiverPool, newSlice)
	bf.receiverSlots = append(bf.receiverSlots, newSlice)
	return newSlice
}

func (bf *blockFilter) buildFamilyStructures(actions []CanonicalAction) {
	if len(bf.ordinalToRoot) == 0 {
		for i := range actions {
			act := &actions[i]
			trxOrdKey := makeTrxOrdinalKey(act.TrxIndex, act.ActionOrdinal)
			if act.CreatorAO == 0 {
				bf.ordinalToRoot[trxOrdKey] = act.ActionOrdinal
			} else {
				creatorKey := makeTrxOrdinalKey(act.TrxIndex, act.CreatorAO)
				bf.ordinalToRoot[trxOrdKey] = bf.ordinalToRoot[creatorKey]
			}
		}
	}

	for i := range actions {
		act := &actions[i]
		trxOrdKey := makeTrxOrdinalKey(act.TrxIndex, act.ActionOrdinal)
		root := bf.ordinalToRoot[trxOrdKey]

		familyHash := makeFamilyKeyHash(act.TrxIndex, root, act.DataIndex, act.ContractUint64, act.ActionUint64)

		slotIdx, exists := bf.familyToSlot[familyHash]
		if !exists {
			slotIdx = len(bf.receiverSlots)
			bf.familyToSlot[familyHash] = slotIdx
			_ = bf.getReceiverSlot()
		}
		bf.receiverSlots[slotIdx] = append(bf.receiverSlots[slotIdx], act.ReceiverUint64)
	}
}

// FilterRawBlock filters a raw block and returns a Block.
// This is the simple API that allocates new slices for each call.
func FilterRawBlock(notif RawBlock, actionFilter ActionFilterFunc) Block {
	filtered, _, _ := FilterRawBlockInto(notif, actionFilter, nil, nil)
	return filtered
}

// FilterRawBlockInto filters a raw block into pre-allocated slices.
// This is the main entry point for high-performance filtering.
// MinSeq/MaxSeq are computed inline during filtering (no extra iteration).
func FilterRawBlockInto(notif RawBlock, actionFilter ActionFilterFunc, actionsBuf []Action, execBuf []ContractExecution) (Block, []Action, []ContractExecution) {
	filtered := Block{
		BlockNum:  notif.BlockNum,
		BlockTime: notif.BlockTime,
	}

	if len(notif.Actions) == 0 {
		return filtered, actionsBuf[:0], execBuf[:0]
	}

	bf := blockFilterPool.Get().(*blockFilter)
	filtered.Actions, filtered.Executions, filtered.MinSeq, filtered.MaxSeq = filterBlockInto(bf, notif, actionFilter, actionsBuf, execBuf)
	bf.reset()
	blockFilterPool.Put(bf)

	return filtered, filtered.Actions, filtered.Executions
}

// FilterRawBlockIntoTimed is like FilterRawBlockInto but records timing breakdown
func FilterRawBlockIntoTimed(notif RawBlock, actionFilter ActionFilterFunc, actionsBuf []Action, execBuf []ContractExecution, timing *FilterTiming) (Block, []Action, []ContractExecution) {
	filtered := Block{
		BlockNum:  notif.BlockNum,
		BlockTime: notif.BlockTime,
	}

	if len(notif.Actions) == 0 {
		return filtered, actionsBuf[:0], execBuf[:0]
	}

	bf := blockFilterPool.Get().(*blockFilter)
	filtered.Actions, filtered.Executions, filtered.MinSeq, filtered.MaxSeq = filterBlockIntoTimed(bf, notif, actionFilter, actionsBuf, execBuf, timing)
	resetStart := nanotime()
	bf.reset()
	timing.ResetNs.Add(nanotime() - resetStart)
	blockFilterPool.Put(bf)
	timing.CallCount.Add(1)

	return filtered, filtered.Actions, filtered.Executions
}

func filterBlockInto(bf *blockFilter, notif RawBlock, actionFilter ActionFilterFunc, actionsBuf []Action, execBuf []ContractExecution) ([]Action, []ContractExecution, uint64, uint64) {
	actions := notif.Actions
	nMeta := len(notif.ActionMeta)

	// Phase 1: Build lookup from ActionMeta
	if cap(bf.infoSlice) >= nMeta {
		bf.infoSlice = bf.infoSlice[:nMeta]
	} else {
		bf.infoSlice = make([]combinedActionInfo, nMeta)
	}

	if nMeta > 0 {
		bf.minGlobalSeq = notif.ActionMeta[0].GlobalSeq
		bf.useDirectIdx = true
		for i, meta := range notif.ActionMeta {
			bf.infoSlice[i] = combinedActionInfo{Meta: meta}
			if meta.GlobalSeq != bf.minGlobalSeq+uint64(i) {
				bf.useDirectIdx = false
			}
		}
		if !bf.useDirectIdx {
			for i, meta := range notif.ActionMeta {
				bf.globalSeqToIdx[meta.GlobalSeq] = int32(i)
			}
		}
	}

	actionsBuf = actionsBuf[:0]
	execBuf = execBuf[:0]

	// Phase 2: Merge canonical actions + build execBuf
	nSlice := uint64(nMeta)
	for i := range actions {
		act := &actions[i]
		gs := act.GlobalSeqUint64

		if bf.useDirectIdx {
			offset := gs - bf.minGlobalSeq
			if offset < nSlice {
				bf.infoSlice[offset].Action = act
				bf.infoSlice[offset].Receiver = act.ReceiverUint64
				bf.infoSlice[offset].TrxIndex = act.TrxIndex
			}
		} else if idx, ok := bf.globalSeqToIdx[gs]; ok {
			bf.infoSlice[idx].Action = act
			bf.infoSlice[idx].Receiver = act.ReceiverUint64
			bf.infoSlice[idx].TrxIndex = act.TrxIndex
		}

		if act.ReceiverUint64 == act.ContractUint64 {
			execBuf = append(execBuf, ContractExecution{
				Contract:  act.ContractUint64,
				Action:    act.ActionUint64,
				GlobalSeq: gs,
				TrxIndex:  act.TrxIndex,
			})
		}
	}

	// Phase 3: Process notifications, track min/max seq inline
	var minSeq, maxSeq uint64
	for account, globalSeqs := range notif.Notifications {
		for _, globalSeq := range globalSeqs {
			var info *combinedActionInfo
			if bf.useDirectIdx {
				offset := globalSeq - bf.minGlobalSeq
				if offset >= nSlice {
					continue
				}
				info = &bf.infoSlice[offset]
			} else {
				idx, ok := bf.globalSeqToIdx[globalSeq]
				if !ok {
					continue
				}
				info = &bf.infoSlice[idx]
			}
			if info.Action == nil {
				continue
			}

			if actionFilter != nil && actionFilter(info.Meta.Contract, info.Meta.Action) {
				continue
			}

			if info.Receiver == account {
				actionsBuf = append(actionsBuf, Action{
					Account:   account,
					Contract:  info.Meta.Contract,
					Action:    info.Meta.Action,
					GlobalSeq: globalSeq,
					TrxIndex:  info.TrxIndex,
				})
				if minSeq == 0 || globalSeq < minSeq {
					minSeq = globalSeq
				}
				if globalSeq > maxSeq {
					maxSeq = globalSeq
				}
				continue
			}

			isAuthorizer := false
			for _, authIdx := range info.Action.AuthAccountIndexes {
				if notif.NamesInBlock[authIdx] == account {
					isAuthorizer = true
					break
				}
			}

			if !isAuthorizer {
				continue
			}

			if !bf.familyBuilt {
				bf.buildFamilyStructures(actions)
				bf.familyBuilt = true
			}

			trxOrdKey := makeTrxOrdinalKey(info.Action.TrxIndex, info.Action.ActionOrdinal)
			root := bf.ordinalToRoot[trxOrdKey]

			familyHash := makeFamilyKeyHash(info.Action.TrxIndex, root, info.Action.DataIndex, info.Action.ContractUint64, info.Action.ActionUint64)
			slotIdx, exists := bf.familyToSlot[familyHash]
			if exists {
				receivedByAccount := false
				for _, receiver := range bf.receiverSlots[slotIdx] {
					if receiver == account {
						receivedByAccount = true
						break
					}
				}
				if receivedByAccount {
					continue
				}
			}

			actionsBuf = append(actionsBuf, Action{
				Account:   account,
				Contract:  info.Meta.Contract,
				Action:    info.Meta.Action,
				GlobalSeq: globalSeq,
				TrxIndex:  info.TrxIndex,
			})
			if minSeq == 0 || globalSeq < minSeq {
				minSeq = globalSeq
			}
			if globalSeq > maxSeq {
				maxSeq = globalSeq
			}
		}
	}

	return actionsBuf, execBuf, minSeq, maxSeq
}

func filterBlockIntoTimed(bf *blockFilter, notif RawBlock, actionFilter ActionFilterFunc, actionsBuf []Action, execBuf []ContractExecution, timing *FilterTiming) ([]Action, []ContractExecution, uint64, uint64) {
	actions := notif.Actions
	nMeta := len(notif.ActionMeta)

	// Phase 1: Build lookup from ActionMeta
	start := nanotime()
	if cap(bf.infoSlice) >= nMeta {
		bf.infoSlice = bf.infoSlice[:nMeta]
	} else {
		bf.infoSlice = make([]combinedActionInfo, nMeta)
	}

	if nMeta > 0 {
		bf.minGlobalSeq = notif.ActionMeta[0].GlobalSeq
		bf.useDirectIdx = true
		for i, meta := range notif.ActionMeta {
			bf.infoSlice[i] = combinedActionInfo{Meta: meta}
			if meta.GlobalSeq != bf.minGlobalSeq+uint64(i) {
				bf.useDirectIdx = false
			}
		}
		if !bf.useDirectIdx {
			for i, meta := range notif.ActionMeta {
				bf.globalSeqToIdx[meta.GlobalSeq] = int32(i)
			}
		}
	}
	timing.MetaLookupNs.Add(nanotime() - start)

	// Phase 2: Merge canonical actions + build execBuf
	start = nanotime()
	actionsBuf = actionsBuf[:0]
	execBuf = execBuf[:0]
	nSlice := uint64(nMeta)

	for i := range actions {
		act := &actions[i]
		gs := act.GlobalSeqUint64

		if bf.useDirectIdx {
			offset := gs - bf.minGlobalSeq
			if offset < nSlice {
				bf.infoSlice[offset].Action = act
				bf.infoSlice[offset].Receiver = act.ReceiverUint64
				bf.infoSlice[offset].TrxIndex = act.TrxIndex
			}
		} else if idx, ok := bf.globalSeqToIdx[gs]; ok {
			bf.infoSlice[idx].Action = act
			bf.infoSlice[idx].Receiver = act.ReceiverUint64
			bf.infoSlice[idx].TrxIndex = act.TrxIndex
		}

		if act.ReceiverUint64 == act.ContractUint64 {
			execBuf = append(execBuf, ContractExecution{
				Contract:  act.ContractUint64,
				Action:    act.ActionUint64,
				GlobalSeq: gs,
				TrxIndex:  act.TrxIndex,
			})
		}
	}
	timing.GsCanonicalNs.Add(nanotime() - start)

	// Phase 3: Process notifications, track min/max seq inline
	start = nanotime()
	var receiverHits, authorizerHits, skipped int64
	var minSeq, maxSeq uint64

	for account, globalSeqs := range notif.Notifications {
		for _, globalSeq := range globalSeqs {
			var info *combinedActionInfo
			if bf.useDirectIdx {
				offset := globalSeq - bf.minGlobalSeq
				if offset >= nSlice {
					skipped++
					continue
				}
				info = &bf.infoSlice[offset]
			} else {
				idx, ok := bf.globalSeqToIdx[globalSeq]
				if !ok {
					skipped++
					continue
				}
				info = &bf.infoSlice[idx]
			}
			if info.Action == nil {
				skipped++
				continue
			}

			if actionFilter != nil && actionFilter(info.Meta.Contract, info.Meta.Action) {
				skipped++
				continue
			}

			if info.Receiver == account {
				actionsBuf = append(actionsBuf, Action{
					Account:   account,
					Contract:  info.Meta.Contract,
					Action:    info.Meta.Action,
					GlobalSeq: globalSeq,
					TrxIndex:  info.TrxIndex,
				})
				if minSeq == 0 || globalSeq < minSeq {
					minSeq = globalSeq
				}
				if globalSeq > maxSeq {
					maxSeq = globalSeq
				}
				receiverHits++
				continue
			}

			isAuthorizer := false
			for _, authIdx := range info.Action.AuthAccountIndexes {
				if notif.NamesInBlock[authIdx] == account {
					isAuthorizer = true
					break
				}
			}

			if !isAuthorizer {
				skipped++
				continue
			}

			if !bf.familyBuilt {
				bf.buildFamilyStructures(actions)
				bf.familyBuilt = true
			}

			trxOrdKey := makeTrxOrdinalKey(info.Action.TrxIndex, info.Action.ActionOrdinal)
			root := bf.ordinalToRoot[trxOrdKey]

			familyHash := makeFamilyKeyHash(info.Action.TrxIndex, root, info.Action.DataIndex, info.Action.ContractUint64, info.Action.ActionUint64)
			slotIdx, exists := bf.familyToSlot[familyHash]
			if exists {
				receivedByAccount := false
				for _, receiver := range bf.receiverSlots[slotIdx] {
					if receiver == account {
						receivedByAccount = true
						break
					}
				}
				if receivedByAccount {
					skipped++
					continue
				}
			}

			actionsBuf = append(actionsBuf, Action{
				Account:   account,
				Contract:  info.Meta.Contract,
				Action:    info.Meta.Action,
				GlobalSeq: globalSeq,
				TrxIndex:  info.TrxIndex,
			})
			if minSeq == 0 || globalSeq < minSeq {
				minSeq = globalSeq
			}
			if globalSeq > maxSeq {
				maxSeq = globalSeq
			}
			authorizerHits++
		}
	}
	timing.NotifsLoopNs.Add(nanotime() - start)
	timing.ReceiverHits.Add(receiverHits)
	timing.AuthorizerHits.Add(authorizerHits)
	timing.Skipped.Add(skipped)

	return actionsBuf, execBuf, minSeq, maxSeq
}
