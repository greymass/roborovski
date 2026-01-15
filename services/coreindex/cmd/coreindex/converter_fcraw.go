package main

import (
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/fcraw"
)

// convertRawBlockToActionsBuffered converts raw fcraw bytes to actionTraceOptimized.
// The returned data is only valid until bufs.Reset() is called.
func convertRawBlockToActionsBuffered(rawBytes []byte, bufs *WorkerBuffers) ([]actionTraceOptimized, *blockData, map[[32]byte]uint32, error) {
	bufs.Reset()
	bufs.ResetConverterPools()
	dec := fcraw.NewSliceDecoder(rawBytes)

	variant := dec.ReadVariantIndex()

	if variant != 2 {
		return nil, nil, nil, nil
	}

	bt, err := fcraw.DecodeBlockTraceV2Slice(dec, bufs.BlockDecodeBuffers)
	if err != nil {
		return nil, nil, nil, err
	}

	var numTrx int
	totalActions := 0
	if bt.TransactionsVariant == 0 {
		numTrx = len(bt.TransactionsV2)
		for i := range bt.TransactionsV2 {
			totalActions += len(bt.TransactionsV2[i].Actions)
		}
	} else if bt.TransactionsVariant == 1 {
		numTrx = len(bt.TransactionsV3)
		for i := range bt.TransactionsV3 {
			totalActions += len(bt.TransactionsV3[i].TransactionTraceV2.Actions)
		}
	}

	blockNum := bt.Number
	blockTimeStr := chain.Uint32ToTime(bt.Timestamp)

	var pbib [32]byte
	copy(pbib[:], bt.ID[:])

	blkData := &blockData{
		BlockNum:        blockNum,
		BlockTime:       blockTimeStr,
		ProducerBlockID: pbib,
		NamesInBlock:    make([]uint64, 0, 100),
		TrxIDInBlock:    make([][32]byte, 0, numTrx),
		TrxMetaInBlock:  make([]trxMeta, 0, numTrx),
		DataInBlock:     make([][]byte, 0, totalActions),
	}

	actions := bufs.AllocActions(totalActions)
	trxIDToIndex := make(map[[32]byte]uint32, numTrx)

	if bt.TransactionsVariant == 0 {
		for txIdx := range bt.TransactionsV2 {
			tx := &bt.TransactionsV2[txIdx]

			index := uint32(len(blkData.TrxIDInBlock))
			blkData.TrxIDInBlock = append(blkData.TrxIDInBlock, tx.ID)
			blkData.TrxMetaInBlock = append(blkData.TrxMetaInBlock, extractTrxMeta(tx))
			trxIDToIndex[tx.ID] = index

			for actIdx := range tx.Actions {
				act := &tx.Actions[actIdx]
				globalActIdx := len(actions)
				ato := convertFcrawActionToOptimizedBuffered(act, tx.ID, blockNum, blockTimeStr, pbib, bufs, globalActIdx)
				actions = append(actions, ato)
			}
		}
	} else if bt.TransactionsVariant == 1 {
		for txIdx := range bt.TransactionsV3 {
			txv3 := &bt.TransactionsV3[txIdx]
			tx := &txv3.TransactionTraceV2

			index := uint32(len(blkData.TrxIDInBlock))
			blkData.TrxIDInBlock = append(blkData.TrxIDInBlock, tx.ID)
			blkData.TrxMetaInBlock = append(blkData.TrxMetaInBlock, extractTrxMeta(tx))
			trxIDToIndex[tx.ID] = index

			for actIdx := range tx.Actions {
				act := &tx.Actions[actIdx]
				globalActIdx := len(actions)
				ato := convertFcrawActionToOptimizedBuffered(act, tx.ID, blockNum, blockTimeStr, pbib, bufs, globalActIdx)
				actions = append(actions, ato)
			}
		}
	}

	bufs.ActionsBuffer = actions

	return actions, blkData, trxIDToIndex, nil
}

func convertFcrawActionToOptimizedBuffered(
	act *fcraw.ActionTraceV1,
	trxID [32]byte,
	blockNum uint32,
	blockTime string,
	producerBlockID [32]byte,
	bufs *WorkerBuffers,
	actionIdx int,
) actionTraceOptimized {
	numAuths := len(act.Authorization)
	authActorNames := bufs.AllocAuthActors(numAuths)
	authPermNames := bufs.AllocAuthPerms(numAuths)
	authSeqs := bufs.AllocAuthSeqs(numAuths)
	for i, auth := range act.Authorization {
		authActorNames[i] = auth.Account
		authPermNames[i] = auth.Permission
		seq, _ := bufs.GetAuthSeq(actionIdx, auth.Account)
		authSeqs[i] = seq
	}

	numDeltas := len(act.AccountRamDeltas)
	ramDeltaAccNames := bufs.AllocRAMAccounts(numDeltas)
	ramDeltaAmounts := bufs.AllocRAMAmounts(numDeltas)
	for i, delta := range act.AccountRamDeltas {
		ramDeltaAccNames[i] = delta.Account
		ramDeltaAmounts[i] = delta.Delta
	}

	actionData := act.Data

	return actionTraceOptimized{
		ActionOrdinal: act.ActionOrdinal,
		CreatorAO:     act.CreatorActionOrdinal,
		ClosestUAAO:   act.ClosestUnnotifiedAncestorActionOrdinal,
		ContextFree:   act.ContextFree,
		Elapsed:       act.Elapsed,

		CodeSequence: act.CodeSequence,
		AbiSequence:  act.AbiSequence,

		BlockNum:  blockNum,
		BlockTime: blockTime,

		ReceiverName:        act.Receiver,
		ReceiptReceiverName: act.ReceiptReceiver,
		AccountName:         act.Account,
		ActionName:          act.Name,

		AuthActorNames: authActorNames,
		AuthPermNames:  authPermNames,
		AuthSeqs:       authSeqs,

		RAMDeltaAccNames: ramDeltaAccNames,
		RAMDeltaAmounts:  ramDeltaAmounts,

		ActionData:         actionData,
		TrxIDRaw:           trxID,
		ProducerBlockIDRaw: producerBlockID,

		GlobalSequence: act.GlobalSequence,
		RecvSequence:   act.RecvSequence,
	}
}

func extractTrxMeta(tx *fcraw.TransactionTraceV2) trxMeta {
	meta := trxMeta{
		Status:         tx.Status,
		CpuUsageUs:     tx.CpuUsageUs,
		NetUsageWords:  tx.NetUsageWords,
		Expiration:     tx.TrxHeader.Expiration,
		RefBlockNum:    tx.TrxHeader.RefBlockNum,
		RefBlockPrefix: tx.TrxHeader.RefBlockPrefix,
		Signatures:     make([][]byte, len(tx.Signatures)),
	}

	for i, sig := range tx.Signatures {
		if sig.Type == 2 && sig.WebAuthn != nil {
			buf := make([]byte, 0, 1+65+len(sig.WebAuthn.AuthData)+len(sig.WebAuthn.ClientData)+10)
			buf = append(buf, byte(sig.Type))
			buf = append(buf, sig.WebAuthn.Signature...)
			buf = appendUvarint(buf, uint64(len(sig.WebAuthn.AuthData)))
			buf = append(buf, sig.WebAuthn.AuthData...)
			buf = appendUvarint(buf, uint64(len(sig.WebAuthn.ClientData)))
			buf = append(buf, sig.WebAuthn.ClientData...)
			meta.Signatures[i] = buf
		} else {
			buf := make([]byte, 1+len(sig.Data))
			buf[0] = byte(sig.Type)
			copy(buf[1:], sig.Data)
			meta.Signatures[i] = buf
		}
	}

	return meta
}

func appendUvarint(buf []byte, x uint64) []byte {
	for x >= 0x80 {
		buf = append(buf, byte(x)|0x80)
		x >>= 7
	}
	buf = append(buf, byte(x))
	return buf
}
