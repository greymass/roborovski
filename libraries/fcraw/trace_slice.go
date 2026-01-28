package fcraw

import (
	"fmt"
)

func DecodeBlockTraceV2Slice(d *SliceDecoder, bufs *BlockDecodeBuffers) (*BlockTraceV2, error) {
	bt := &BlockTraceV2{}

	d.Context = "block_header"
	bt.ID = d.ReadChecksum256()
	bt.Number = d.ReadUint32()
	d.Context = fmt.Sprintf("block_%d_header", bt.Number)
	bt.PreviousID = d.ReadChecksum256()
	bt.Timestamp = d.ReadUint32()
	bt.Producer = d.ReadUint64()
	bt.TransactionMroot = d.ReadChecksum256()
	bt.ActionMroot = d.ReadChecksum256()
	bt.ScheduleVersion = d.ReadUint32()

	bt.TransactionsVariant = d.ReadVariantIndex()
	txCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_txcount_%d", bt.Number, txCount)

	if bt.TransactionsVariant == 0 {
		txStart := len(bufs.Transactions)
		for i := uint32(0); i < txCount; i++ {
			d.Context = fmt.Sprintf("block_%d_tx_%d/%d", bt.Number, i, txCount)
			tx := bufs.allocTransaction()
			if err := decodeTransactionTraceV2Slice(d, tx, bufs, bt.Number, i, txCount); err != nil {
				return nil, fmt.Errorf("transaction %d: %w", i, err)
			}
		}
		bt.TransactionsV2 = bufs.Transactions[txStart:]
	} else if bt.TransactionsVariant == 1 {
		txStart := len(bufs.TransactionsV3)
		for i := uint32(0); i < txCount; i++ {
			d.Context = fmt.Sprintf("block_%d_tx_%d/%d_v3", bt.Number, i, txCount)
			tx := bufs.allocTransactionV3()
			if err := decodeTransactionTraceV3Slice(d, tx, bufs, bt.Number, i, txCount); err != nil {
				return nil, fmt.Errorf("transaction %d: %w", i, err)
			}
		}
		bt.TransactionsV3 = bufs.TransactionsV3[txStart:]
	} else {
		return nil, fmt.Errorf("unsupported transactions variant: %d", bt.TransactionsVariant)
	}

	return bt, nil
}

func decodeTransactionTraceV2Slice(d *SliceDecoder, tx *TransactionTraceV2, bufs *BlockDecodeBuffers, blockNum uint32, txIdx uint32, txCount uint32) error {
	tx.ID = d.ReadChecksum256()
	tx.ActionsVariant = d.ReadVariantIndex()

	if tx.ActionsVariant != 0 {
		return fmt.Errorf("unsupported actions variant: %d", tx.ActionsVariant)
	}

	actionCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d/%d_actions_%d", blockNum, txIdx, txCount, actionCount)
	actStart := len(bufs.Actions)
	for i := uint32(0); i < actionCount; i++ {
		d.Context = fmt.Sprintf("block_%d_tx_%d/%d_action_%d/%d", blockNum, txIdx, txCount, i, actionCount)
		act := bufs.allocAction()
		decodeActionTraceV1Slice(d, act, bufs, blockNum, txIdx, i, actionCount)
	}
	tx.Actions = bufs.Actions[actStart:]

	d.Context = fmt.Sprintf("block_%d_tx_%d/%d_status", blockNum, txIdx, txCount)
	tx.Status = d.ReadUint8()
	tx.CpuUsageUs = d.ReadUint32()
	tx.NetUsageWords = d.ReadVarUint32()

	sigCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d/%d_sigs_%d", blockNum, txIdx, txCount, sigCount)
	sigStart := len(bufs.Signatures)
	for i := uint32(0); i < sigCount; i++ {
		sig := bufs.allocSignature()
		decodeSignatureSlice(d, sig, bufs)
	}
	tx.Signatures = make([]*Signature, sigCount)
	for i := uint32(0); i < sigCount; i++ {
		tx.Signatures[i] = &bufs.Signatures[sigStart+int(i)]
	}

	d.Context = fmt.Sprintf("block_%d_tx_%d/%d_header", blockNum, txIdx, txCount)
	decodeTransactionHeaderSlice(d, &tx.TrxHeader)

	return nil
}

func decodeTransactionTraceV3Slice(d *SliceDecoder, tx *TransactionTraceV3, bufs *BlockDecodeBuffers, blockNum uint32, txIdx uint32, txCount uint32) error {
	if err := decodeTransactionTraceV2Slice(d, &tx.TransactionTraceV2, bufs, blockNum, txIdx, txCount); err != nil {
		return err
	}

	d.Context = fmt.Sprintf("block_%d_tx_%d/%d_v3_extra", blockNum, txIdx, txCount)
	tx.BlockNum = d.ReadUint32()
	tx.BlockTime = d.ReadUint32()

	if d.ReadBool() {
		producerBlockID := d.ReadChecksum256()
		tx.ProducerBlockID = &producerBlockID
	}

	return nil
}

func decodeActionTraceV1Slice(d *SliceDecoder, act *ActionTraceV1, bufs *BlockDecodeBuffers, blockNum uint32, txIdx uint32, actIdx uint32, actCount uint32) {
	decodeActionTraceSlice(d, &act.ActionTrace, bufs, blockNum, txIdx, actIdx, actCount)

	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_return", blockNum, txIdx, actIdx, actCount)
	returnValueLen := d.ReadVarUint32()
	if returnValueLen > 0 {
		act.ReturnValue = bufs.allocReturn(returnValueLen)
		copy(act.ReturnValue, d.ReadBytesRef(int(returnValueLen)))
	}
}

func decodeActionTraceSlice(d *SliceDecoder, act *ActionTrace, bufs *BlockDecodeBuffers, blockNum uint32, txIdx uint32, actIdx uint32, actCount uint32) {
	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_ordinals", blockNum, txIdx, actIdx, actCount)
	act.ActionOrdinal = d.ReadVarUint32()
	act.CreatorActionOrdinal = d.ReadVarUint32()
	act.ClosestUnnotifiedAncestorActionOrdinal = d.ReadVarUint32()

	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_receipt", blockNum, txIdx, actIdx, actCount)
	act.ReceiptReceiver = d.ReadUint64()
	act.GlobalSequence = d.ReadUint64()
	act.RecvSequence = d.ReadUint64()

	authSeqCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_authseq_%d", blockNum, txIdx, actIdx, actCount, authSeqCount)
	authSeqStart := bufs.allocAuthSeq(authSeqCount)
	for i := uint32(0); i < authSeqCount; i++ {
		bufs.AuthSeqKeys[authSeqStart+i] = d.ReadUint64()
		bufs.AuthSeqVals[authSeqStart+i] = d.ReadUint64()
	}
	bufs.recordAuthSeqRange(authSeqStart, authSeqCount)
	act.AuthSequence = nil

	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_codeabi", blockNum, txIdx, actIdx, actCount)
	act.CodeSequence = d.ReadVarUint32()
	act.AbiSequence = d.ReadVarUint32()
	act.Receiver = d.ReadUint64()
	act.Account = d.ReadUint64()
	act.Name = d.ReadUint64()

	authCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_auths_%d", blockNum, txIdx, actIdx, actCount, authCount)
	act.Authorization = bufs.allocAuths(authCount)
	for i := uint32(0); i < authCount; i++ {
		act.Authorization[i].Account = d.ReadUint64()
		act.Authorization[i].Permission = d.ReadUint64()
	}

	dataLen := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_data_%d", blockNum, txIdx, actIdx, actCount, dataLen)
	if dataLen > 0 {
		act.Data = bufs.allocData(dataLen)
		copy(act.Data, d.ReadBytesRef(int(dataLen)))
	}

	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_elapsed", blockNum, txIdx, actIdx, actCount)
	act.ContextFree = d.ReadBool()
	act.Elapsed = d.ReadInt64()

	deltaCount := d.ReadVarUint32()
	d.Context = fmt.Sprintf("block_%d_tx_%d_action_%d/%d_deltas_%d", blockNum, txIdx, actIdx, actCount, deltaCount)
	act.AccountRamDeltas = bufs.allocDeltas(deltaCount)
	for i := uint32(0); i < deltaCount; i++ {
		act.AccountRamDeltas[i].Account = d.ReadUint64()
		act.AccountRamDeltas[i].Delta = d.ReadInt64()
	}
}

func decodeSignatureSlice(d *SliceDecoder, sig *Signature, bufs *BlockDecodeBuffers) {
	sig.Type = d.ReadVariantIndex()

	switch sig.Type {
	case 0, 1:
		sig.Data = bufs.allocSignatureData(65)
		copy(sig.Data, d.ReadBytesRef(65))
	case 2:
		sig.WebAuthn = decodeWebAuthnSignatureSlice(d, bufs)
	default:
		panic(fmt.Sprintf("unknown signature type: %d", sig.Type))
	}
}

func decodeWebAuthnSignatureSlice(d *SliceDecoder, bufs *BlockDecodeBuffers) *WebAuthnSignature {
	wa := &WebAuthnSignature{}

	wa.Signature = bufs.allocSignatureData(65)
	copy(wa.Signature, d.ReadBytesRef(65))

	authLen := d.ReadVarUint32()
	if authLen > 0 {
		wa.AuthData = bufs.allocSignatureData(int(authLen))
		copy(wa.AuthData, d.ReadBytesRef(int(authLen)))
	}

	clientLen := d.ReadVarUint32()
	if clientLen > 0 {
		wa.ClientData = bufs.allocSignatureData(int(clientLen))
		copy(wa.ClientData, d.ReadBytesRef(int(clientLen)))
	}

	return wa
}

func decodeTransactionHeaderSlice(d *SliceDecoder, th *TransactionHeader) {
	th.Expiration = d.ReadUint32()
	th.RefBlockNum = d.ReadUint16()
	th.RefBlockPrefix = d.ReadUint32()
	th.MaxNetUsageWords = d.ReadVarUint32()
	th.MaxCpuUsageMs = d.ReadUint8()
	th.DelaySec = d.ReadVarUint32()
}
