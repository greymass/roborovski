package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/enforce"
)

var EMPTY32 = [32]byte{}

type accountDelta struct {
	AccountIndex uint32
	Delta        int64
}

type authTriple struct {
	AccountIndex    uint32
	PermissionIndex uint32
	Seq             uint64
}

type compressedActionTrace struct {
	GlobalSequence    uint64
	Elapsed           int64
	RecvSequence      uint64
	ActionOrdinal     uint32
	CreatorAO         uint32
	ClosestUAAO       uint32
	CodeSequence      uint32
	AbiSequence       uint32
	ReceiverIndex     uint32
	ContractNameIndex uint32
	ActionNameIndex   uint32
	TrxIDIndex        uint32
	DataIndex         uint32
	ContextFree       bool
	Auths             []authTriple
	AccountRAMDeltas  []accountDelta
}

func (cat *compressedActionTrace) At(blkData *blockData, blockNum uint32, glob uint64) *chain.ActionTrace {
	pl := make([]chain.PermissionLevel, len(cat.Auths))
	as := make([][]interface{}, len(cat.Auths))
	ard := make([]chain.AccountDelta, len(cat.AccountRAMDeltas))

	for i := 0; i < len(cat.Auths); i++ {
		accAuth := chain.NameToString(blkData.NamesInBlock[cat.Auths[i].AccountIndex])
		accPerm := chain.NameToString(blkData.NamesInBlock[cat.Auths[i].PermissionIndex])
		pl[i] = chain.PermissionLevel{Actor: accAuth, Permission: accPerm}
		as[i] = []interface{}{accAuth, cat.Auths[i].Seq}
	}

	for i := 0; i < len(cat.AccountRAMDeltas); i++ {
		acc := chain.NameToString(blkData.NamesInBlock[cat.AccountRAMDeltas[i].AccountIndex])
		ard[i] = chain.AccountDelta{Account: acc, Delta: cat.AccountRAMDeltas[i].Delta}
	}

	rcvr := chain.NameToString(blkData.NamesInBlock[cat.ReceiverIndex])

	act := chain.Action{
		Account:       chain.NameToString(blkData.NamesInBlock[cat.ContractNameIndex]),
		Name:          chain.NameToString(blkData.NamesInBlock[cat.ActionNameIndex]),
		Authorization: pl,
		Data:          hex.EncodeToString(blkData.DataInBlock[cat.DataIndex]),
	}

	receipt := chain.ActionReceipt{
		Receiver:       rcvr,
		ActDigest:      act.Digest(),
		GlobalSequence: json.Number(strconv.FormatUint(cat.GlobalSequence, 10)),
		RecvSequence:   json.Number(strconv.FormatUint(cat.RecvSequence, 10)),
		AuthSequence:   as,
		CodeSequence:   cat.CodeSequence,
		AbiSequence:    cat.AbiSequence,
	}

	return &chain.ActionTrace{
		ActionOrdinal:    cat.ActionOrdinal,
		CreatorAO:        cat.CreatorAO,
		ClosestUAAO:      cat.ClosestUAAO,
		Receipt:          receipt,
		Receiver:         rcvr,
		Act:              act,
		ContextFree:      cat.ContextFree,
		Elapsed:          cat.Elapsed,
		TrxID:            hex.EncodeToString(blkData.TrxIDInBlock[cat.TrxIDIndex][:]),
		BlockNum:         blockNum,
		BlockTime:        blkData.BlockTime,
		ProducerBlockID:  hex.EncodeToString(blkData.ProducerBlockID[:]),
		AccountRAMDeltas: ard,
	}
}

func compressActionTraceOptimized(ato *actionTraceOptimized, blkData *blockData, trxIDToIndex map[[32]byte]uint32, prefilledHeader bool, bufs *WorkerBuffers) *compressedActionTrace {
	cat := compressedActionTrace{
		GlobalSequence: ato.GlobalSequence,
		Elapsed:        ato.Elapsed,
		RecvSequence:   ato.RecvSequence,
		ActionOrdinal:  ato.ActionOrdinal,
		CreatorAO:      ato.CreatorAO,
		ClosestUAAO:    ato.ClosestUAAO,
		CodeSequence:   ato.CodeSequence,
		AbiSequence:    ato.AbiSequence,
		ContextFree:    ato.ContextFree,
	}

	if prefilledHeader == false {
		if blkData.BlockTime == "" {
			blkData.BlockTime = ato.BlockTime
		} else {
			enforce.ENFORCE(blkData.BlockTime == ato.BlockTime, "blkData.BlockTime != ato.BlockTime!")
		}
		if blkData.BlockNum == 0 {
			blkData.BlockNum = ato.BlockNum
		} else {
			enforce.ENFORCE(blkData.BlockNum == ato.BlockNum, "blkData.BlockNum != ato.BlockNum!")
		}
		if blkData.ProducerBlockID == EMPTY32 {
			blkData.ProducerBlockID = ato.ProducerBlockIDRaw
		} else {
			enforce.ENFORCE(blkData.ProducerBlockID == ato.ProducerBlockIDRaw, "blkData.ProducerBlockID != ato.ProducerBlockIDRaw!")
		}
	}

	enforce.ENFORCE(ato.ReceiverName == ato.ReceiptReceiverName, "Receipt.Receiver != Receiver! Fatal assumption.")

	blkData.NamesInBlock, cat.ReceiverIndex = insertOrIndexFast(ato.ReceiverName, blkData.NamesInBlock, bufs.NamesIndex)
	blkData.NamesInBlock, cat.ContractNameIndex = insertOrIndexFast(ato.AccountName, blkData.NamesInBlock, bufs.NamesIndex)
	blkData.NamesInBlock, cat.ActionNameIndex = insertOrIndexFast(ato.ActionName, blkData.NamesInBlock, bufs.NamesIndex)

	enforce.ENFORCE(len(ato.AuthSeqs) == len(ato.AuthActorNames), "AuthSeqs != AuthActorNames! Fatal assumption.", ato.TrxIDRaw)

	cat.Auths = bufs.AllocAuths(len(ato.AuthSeqs))
	for i := 0; i < len(ato.AuthSeqs); i++ {
		accountIndex := uint32(0)
		permissionIndex := uint32(0)
		blkData.NamesInBlock, accountIndex = insertOrIndexFast(ato.AuthActorNames[i], blkData.NamesInBlock, bufs.NamesIndex)
		blkData.NamesInBlock, permissionIndex = insertOrIndexFast(ato.AuthPermNames[i], blkData.NamesInBlock, bufs.NamesIndex)
		cat.Auths[i] = authTriple{AccountIndex: accountIndex,
			PermissionIndex: permissionIndex,
			Seq:             ato.AuthSeqs[i]}
	}

	cat.AccountRAMDeltas = bufs.AllocDeltas(len(ato.RAMDeltaAccNames))
	for i := 0; i < len(ato.RAMDeltaAccNames); i++ {
		accountIndex := uint32(0)
		blkData.NamesInBlock, accountIndex = insertOrIndexFast(ato.RAMDeltaAccNames[i], blkData.NamesInBlock, bufs.NamesIndex)
		cat.AccountRAMDeltas[i] = accountDelta{AccountIndex: accountIndex, Delta: ato.RAMDeltaAmounts[i]}
	}

	cat.TrxIDIndex = trxIDToIndex[ato.TrxIDRaw]

	blkData.DataInBlock, cat.DataIndex = insertOrIndexBytesFast(ato.ActionData, blkData.DataInBlock, bufs.DataIndex)

	if blkData.MinGlobInBlock == 0 {
		blkData.MinGlobInBlock = math.MaxUint64
	}
	blkData.MinGlobInBlock = minUint64(blkData.MinGlobInBlock, ato.GlobalSequence)
	blkData.MaxGlobInBlock = maxUint64(blkData.MaxGlobInBlock, ato.GlobalSequence)
	return &cat
}

func (cat *compressedActionTrace) Bytes() []byte {
	buff := bytes.NewBuffer(make([]byte, 0, 64+len(cat.Auths)*24+len(cat.AccountRAMDeltas)*16))
	cat.WriteTo(buff)
	return buff.Bytes()
}

func (cat *compressedActionTrace) WriteTo(buff *bytes.Buffer) {
	magicmask1 := byte(0)
	magicmask2 := byte(0)
	authsLen := uint32(len(cat.Auths))
	deltaLen := uint32(len(cat.AccountRAMDeltas))

	if cat.ContractNameIndex == cat.ReceiverIndex {
		magicmask1 |= (1 << 7)
	}
	if authsLen == 1 {
		magicmask1 |= (1 << 6)
	}
	if deltaLen == 0 {
		magicmask1 |= (1 << 5)
	}
	if cat.CreatorAO == cat.ClosestUAAO {
		magicmask1 |= (1 << 3)
	}
	if cat.CodeSequence == 1 {
		magicmask1 |= (1 << 2)
	}
	if cat.AbiSequence == cat.CodeSequence {
		magicmask1 |= (1 << 1)
	}
	if cat.ActionOrdinal == 1 {
		magicmask1 |= (1 << 0)
	}

	if cat.CreatorAO == cat.ActionOrdinal-1 {
		magicmask2 |= (1 << 0)
	}
	if deltaLen == 1 {
		magicmask2 |= (1 << 1)
	}
	if cat.TrxIDIndex == 0 {
		magicmask2 |= (1 << 2)
	}
	if cat.DataIndex == 0 {
		magicmask2 |= (1 << 3)
	}
	if authsLen >= 1 && cat.Auths[0].AccountIndex == cat.ContractNameIndex {
		magicmask2 |= (1 << 4)
	}
	if deltaLen >= 1 && authsLen >= 1 && cat.AccountRAMDeltas[0].AccountIndex == cat.Auths[0].AccountIndex {
		magicmask2 |= (1 << 5)
	}
	if cat.ReceiverIndex == 0 {
		magicmask2 |= (1 << 6)
	}
	if cat.ContextFree == true {
		magicmask2 |= (1 << 7)
	}

	if magicmask2 > 0 {
		magicmask1 |= (1 << 4)
	}
	buff.WriteByte(magicmask1)
	if magicmask2 > 0 {
		buff.WriteByte(magicmask2)
	}

	encoding.PutAsVarint(buff, cat.Elapsed)
	encoding.PutAsUVarint(buff, cat.RecvSequence)
	encoding.PutAsUVarint(buff, cat.GlobalSequence)

	if (magicmask1 & (1 << 0)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.ActionOrdinal-2))
	}

	if (magicmask2 & (1 << 0)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.CreatorAO))
	}

	if (magicmask1 & (1 << 3)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.ClosestUAAO^cat.CreatorAO))
	}
	if (magicmask1 & (1 << 2)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.CodeSequence-2))
	}
	if (magicmask1 & (1 << 1)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.AbiSequence^cat.CodeSequence))
	}

	if (magicmask2 & (1 << 6)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.ReceiverIndex))
	}
	if (magicmask1 & (1 << 7)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.ContractNameIndex))
	}
	encoding.PutAsUVarint(buff, uint64(cat.ActionNameIndex))
	if (magicmask2 & (1 << 2)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.TrxIDIndex))
	}
	if (magicmask2 & (1 << 3)) == 0 {
		encoding.PutAsUVarint(buff, uint64(cat.DataIndex))
	}

	if (magicmask1 & (1 << 6)) == 0 {
		encoding.PutAsUVarint(buff, uint64(authsLen-1))
	}
	for i := uint32(0); i < authsLen; i++ {
		if i != 0 || ((magicmask2 & (1 << 4)) == 0) {
			encoding.PutAsUVarint(buff, uint64(cat.Auths[i].AccountIndex))
		}
		encoding.PutAsUVarint(buff, uint64(cat.Auths[i].PermissionIndex))
		encoding.PutAsUVarint(buff, cat.Auths[i].Seq)
	}

	if (magicmask1 & (1 << 5)) == 0 {
		if (magicmask2 & (1 << 1)) == 0 {
			encoding.PutAsUVarint(buff, uint64(deltaLen-1))
		}
		for i := uint32(0); i < deltaLen; i++ {
			if i != 0 || ((magicmask2 & (1 << 5)) == 0) {
				encoding.PutAsUVarint(buff, uint64(cat.AccountRAMDeltas[i].AccountIndex))
			}
			encoding.PutAsVarint(buff, cat.AccountRAMDeltas[i].Delta)
		}
	}
}

func catFromBytes(inBytes []byte) *compressedActionTrace {
	cat := compressedActionTrace{}
	buff := bytes.NewReader(inBytes)
	magicmask1 := byte(0)
	magicmask2 := byte(0)

	binary.Read(buff, binary.LittleEndian, &magicmask1)
	if (magicmask1 & (1 << 4)) > 0 {
		binary.Read(buff, binary.LittleEndian, &magicmask2)
	}

	if (magicmask2 & (1 << 7)) > 0 {
		cat.ContextFree = true
	}

	cat.Elapsed = encoding.GetAsVarint(buff)
	cat.RecvSequence = encoding.GetAsUVarint(buff)
	cat.GlobalSequence = encoding.GetAsUVarint(buff)

	cat.ActionOrdinal = 1
	if (magicmask1 & (1 << 0)) == 0 {
		cat.ActionOrdinal = uint32(encoding.GetAsUVarint(buff)) + 2
	}

	cat.CreatorAO = cat.ActionOrdinal - 1
	if (magicmask2 & (1 << 0)) == 0 {
		cat.CreatorAO = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ClosestUAAO = cat.CreatorAO
	if (magicmask1 & (1 << 3)) == 0 {
		cat.ClosestUAAO = cat.CreatorAO ^ uint32(encoding.GetAsUVarint(buff))
	}

	cat.CodeSequence = 1
	if (magicmask1 & (1 << 2)) == 0 {
		cat.CodeSequence = uint32(encoding.GetAsUVarint(buff)) + 2
	}

	cat.AbiSequence = cat.CodeSequence
	if (magicmask1 & (1 << 1)) == 0 {
		cat.AbiSequence = uint32(encoding.GetAsUVarint(buff)) ^ cat.CodeSequence
	}

	cat.ReceiverIndex = 0
	if (magicmask2 & (1 << 6)) == 0 {
		cat.ReceiverIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ContractNameIndex = cat.ReceiverIndex
	if (magicmask1 & (1 << 7)) == 0 {
		cat.ContractNameIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ActionNameIndex = uint32(encoding.GetAsUVarint(buff))

	cat.TrxIDIndex = 0
	if (magicmask2 & (1 << 2)) == 0 {
		cat.TrxIDIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.DataIndex = 0
	if (magicmask2 & (1 << 3)) == 0 {
		cat.DataIndex = uint32(encoding.GetAsUVarint(buff))
	}

	authsLen := uint32(1)
	if (magicmask1 & (1 << 6)) == 0 {
		authsLen = uint32(encoding.GetAsUVarint(buff) + 1)
	}

	for i := uint32(0); i < authsLen; i++ {
		AUT := authTriple{}
		AUT.AccountIndex = cat.ContractNameIndex
		if i != 0 || ((magicmask2 & (1 << 4)) == 0) {
			AUT.AccountIndex = uint32(encoding.GetAsUVarint(buff))
		}
		AUT.PermissionIndex = uint32(encoding.GetAsUVarint(buff))
		AUT.Seq = encoding.GetAsUVarint(buff)
		cat.Auths = append(cat.Auths, AUT)
	}

	deltaLen := uint32(0)
	if (magicmask1 & (1 << 5)) == 0 {
		if (magicmask2 & (1 << 1)) == 0 {
			deltaLen = uint32(encoding.GetAsUVarint(buff) + 1)
		} else {
			deltaLen = 1
		}
	}

	for i := uint32(0); i < deltaLen; i++ {
		AD := accountDelta{}
		if i != 0 || ((magicmask2 & (1 << 5)) == 0) {
			AD.AccountIndex = uint32(encoding.GetAsUVarint(buff))
		} else {
			AD.AccountIndex = cat.Auths[0].AccountIndex
		}
		AD.Delta = encoding.GetAsVarint(buff)
		cat.AccountRAMDeltas = append(cat.AccountRAMDeltas, AD)
	}
	return &cat
}

// insertOrIndexFast uses a map for O(1) lookup instead of O(n) linear scan.
// The index map must be cleared between blocks.
func insertOrIndexFast(needle uint64, haystack []uint64, index map[uint64]uint32) ([]uint64, uint32) {
	if idx, ok := index[needle]; ok {
		return haystack, idx
	}
	idx := uint32(len(haystack))
	haystack = append(haystack, needle)
	index[needle] = idx
	return haystack, idx
}

// insertOrIndexBytesFast uses a map for O(1) lookup instead of O(n) linear scan with bytes.Equal.
// Uses string(needle) as key - Go handles this efficiently for map lookups.
// The index map must be cleared between blocks.
func insertOrIndexBytesFast(needle []byte, haystack [][]byte, index map[string]uint32) ([][]byte, uint32) {
	key := string(needle)
	if idx, ok := index[key]; ok {
		return haystack, idx
	}
	idx := uint32(len(haystack))
	haystack = append(haystack, needle)
	index[key] = idx
	return haystack, idx
}

// maybeGetInt64 attempts to extract an int64 from an interface{} value
func maybeGetInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case json.Number:
		i, err := v.Int64()
		return i, err == nil
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// minUint64 returns the smaller of two uint64 values.
func minUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

// maxUint64 returns the larger of two uint64 values.
func maxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}
