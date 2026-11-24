package corereader

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// PayloadsHaveSameContent reports whether two block payloads hold the same block, transactions, names and actions with the same action data, whatever order or unused entries each data table has.
func PayloadsHaveSameContent(pa, pb []byte, ignoreElapsed bool) (same bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parse payload: %v", r)
		}
	}()
	for _, p := range [][]byte{pa, pb} {
		if _, _, err := PayloadWithoutElapsed(p); err != nil {
			return false, err
		}
	}
	a := payloadContent(bytesToBlockBlob(pa), ignoreElapsed)
	b := payloadContent(bytesToBlockBlob(pb), ignoreElapsed)
	return bytes.Equal(a, b), nil
}

func payloadContent(bb *blockBlob, ignoreElapsed bool) []byte {
	blk := bb.Block
	out := binary.LittleEndian.AppendUint32(nil, blk.BlockNum)
	out = binary.LittleEndian.AppendUint32(out, blk.BlockTimeUint32)
	out = append(out, blk.ProducerBlockID[:]...)
	out = binary.LittleEndian.AppendUint64(out, blk.MinGlobInBlock)
	out = binary.LittleEndian.AppendUint64(out, blk.MaxGlobInBlock)
	out = binary.AppendUvarint(out, uint64(len(blk.TrxIDInBlock)))
	for i, id := range blk.TrxIDInBlock {
		m := blk.TrxMetaInBlock[i]
		out = append(out, id[:]...)
		out = append(out, m.Status)
		out = binary.LittleEndian.AppendUint32(out, m.CpuUsageUs)
		out = binary.LittleEndian.AppendUint32(out, m.NetUsageWords)
		out = binary.LittleEndian.AppendUint32(out, m.Expiration)
		out = binary.LittleEndian.AppendUint16(out, m.RefBlockNum)
		out = binary.LittleEndian.AppendUint32(out, m.RefBlockPrefix)
		out = binary.AppendUvarint(out, uint64(len(m.Signatures)))
		for _, s := range m.Signatures {
			out = binary.AppendUvarint(out, uint64(len(s)))
			out = append(out, s...)
		}
	}
	out = binary.AppendUvarint(out, uint64(len(blk.NamesInBlock)))
	for _, n := range blk.NamesInBlock {
		out = binary.LittleEndian.AppendUint64(out, n)
	}
	out = binary.AppendUvarint(out, uint64(len(bb.Cats)))
	for _, c := range bb.Cats {
		out = binary.LittleEndian.AppendUint64(out, c.GlobalSequence)
		if !ignoreElapsed {
			out = binary.AppendVarint(out, c.Elapsed)
		}
		out = binary.LittleEndian.AppendUint64(out, c.RecvSequence)
		for _, v := range []uint32{c.ActionOrdinal, c.CreatorAO, c.ClosestUAAO, c.CodeSequence, c.AbiSequence, c.ReceiverIndex, c.ContractNameIndex, c.ActionNameIndex, c.TrxIDIndex} {
			out = binary.LittleEndian.AppendUint32(out, v)
		}
		data := blk.DataInBlock[c.DataIndex]
		out = binary.AppendUvarint(out, uint64(len(data)))
		out = append(out, data...)
		if c.ContextFree {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}
		out = binary.AppendUvarint(out, uint64(len(c.Auths)))
		for _, a := range c.Auths {
			out = binary.LittleEndian.AppendUint32(out, a.AccountIndex)
			out = binary.LittleEndian.AppendUint32(out, a.PermissionIndex)
			out = binary.LittleEndian.AppendUint64(out, a.Seq)
		}
		out = binary.AppendUvarint(out, uint64(len(c.AccountRAMDeltas)))
		for _, d := range c.AccountRAMDeltas {
			out = binary.LittleEndian.AppendUint32(out, d.AccountIndex)
			out = binary.AppendVarint(out, d.Delta)
		}
	}
	return out
}
