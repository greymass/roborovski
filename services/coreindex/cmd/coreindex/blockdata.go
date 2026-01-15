package main

import (
	"bytes"
	"encoding/binary"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/enforce"
)

// Direct little-endian write helpers to avoid encoding/binary.Write reflection overhead
func writeU16LE(buf *bytes.Buffer, v uint16) {
	buf.Write([]byte{byte(v), byte(v >> 8)})
}

func writeU32LE(buf *bytes.Buffer, v uint32) {
	buf.Write([]byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)})
}

func writeU64LE(buf *bytes.Buffer, v uint64) {
	buf.Write([]byte{
		byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56),
	})
}

type blockBlobHelper struct {
	blob      *blockBlob
	blobBytes []byte
}

type blockBlob struct {
	Block      *blockData
	Cats       []*compressedActionTrace
	CatsOffset []uint32
	CatsBytes  [][]byte
}

type trxMeta struct {
	Status         uint8
	CpuUsageUs     uint32
	NetUsageWords  uint32
	Expiration     uint32
	RefBlockNum    uint16
	RefBlockPrefix uint32
	Signatures     [][]byte
}

type blockData struct { // indexed by block_num
	BlockNum        uint32
	BlockTime       string
	MinGlobInBlock  uint64
	MaxGlobInBlock  uint64
	ProducerBlockID [32]byte
	TrxIDInBlock    [][32]byte
	TrxMetaInBlock  []trxMeta
	DataInBlock     [][]byte
	NamesInBlock    []uint64
}

func bytesToBlockBlob(inBytes []byte) *blockBlob {
	buff := bytes.NewReader(inBytes)
	blob := blockBlob{}

	blob.Block = reader2Block(buff)

	catsLen := encoding.GetAsUVarint(buff)

	for i := uint64(0); i < catsLen; i++ {
		blob.CatsOffset = append(blob.CatsOffset, uint32(encoding.GetAsUVarint(buff)))
	}
	blob.CatsBytes = make([][]byte, catsLen)
	for i := uint64(0); i < catsLen; i++ {
		catLen := encoding.GetAsUVarint(buff)
		blob.CatsBytes[i] = make([]byte, catLen)
	}
	blob.Cats = make([]*compressedActionTrace, catsLen)
	for i := uint64(0); i < catsLen; i++ {
		_, _ = buff.Read(blob.CatsBytes[i])
		blob.Cats[i] = catFromBytes(blob.CatsBytes[i])
	}

	return &blob
}

func (blob *blockBlob) Bytes() []byte {
	totalCatBytes := 0
	for _, cb := range blob.CatsBytes {
		totalCatBytes += len(cb)
	}
	estimatedSize := totalCatBytes + len(blob.CatsBytes)*10 + 1024
	buff := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	blobBytes := blob.Block.Bytes()
	buff.Write(blobBytes)

	enforce.ENFORCE(len(blob.CatsOffset) == len(blob.Cats))
	encoding.PutAsUVarint(buff, uint64(len(blob.CatsOffset)))

	for i := 0; i < len(blob.CatsOffset); i++ {
		encoding.PutAsUVarint(buff, uint64(blob.CatsOffset[i]))
	}
	for i := 0; i < len(blob.CatsOffset); i++ {
		encoding.PutAsUVarint(buff, uint64(len(blob.CatsBytes[i])))
	}
	for i := 0; i < len(blob.Cats); i++ {
		buff.Write(blob.CatsBytes[i])
	}

	return buff.Bytes()
}

func (blkData *blockData) Bytes() []byte {
	estimatedSize := 64 + len(blkData.TrxIDInBlock)*32 + len(blkData.TrxMetaInBlock)*100 + len(blkData.NamesInBlock)*8
	for _, data := range blkData.DataInBlock {
		estimatedSize += len(data) + 5
	}
	buff := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	writeU32LE(buff, blkData.BlockNum)

	writeU32LE(buff, chain.TimeToUint32(blkData.BlockTime))

	buff.Write(blkData.ProducerBlockID[:])

	writeU64LE(buff, blkData.MaxGlobInBlock)
	encoding.PutAsUVarint(buff, blkData.MaxGlobInBlock-blkData.MinGlobInBlock)

	encoding.PutAsUVarint(buff, uint64(len(blkData.TrxIDInBlock)))
	for i := 0; i < len(blkData.TrxIDInBlock); i++ {
		buff.Write(blkData.TrxIDInBlock[i][:])
	}

	for i := 0; i < len(blkData.TrxMetaInBlock); i++ {
		meta := &blkData.TrxMetaInBlock[i]
		buff.WriteByte(meta.Status)
		writeU32LE(buff, meta.CpuUsageUs)
		encoding.PutAsUVarint(buff, uint64(meta.NetUsageWords))
		writeU32LE(buff, meta.Expiration)
		writeU16LE(buff, meta.RefBlockNum)
		writeU32LE(buff, meta.RefBlockPrefix)
		encoding.PutAsUVarint(buff, uint64(len(meta.Signatures)))
		for _, sig := range meta.Signatures {
			encoding.PutAsUVarint(buff, uint64(len(sig)))
			buff.Write(sig)
		}
	}

	encoding.PutAsUVarint(buff, uint64(len(blkData.DataInBlock)))
	for i := 0; i < len(blkData.DataInBlock); i++ {
		encoding.PutAsUVarint(buff, uint64(len(blkData.DataInBlock[i])))
		buff.Write(blkData.DataInBlock[i])
	}

	encoding.PutAsUVarint(buff, uint64(len(blkData.NamesInBlock)))
	for _, name := range blkData.NamesInBlock {
		writeU64LE(buff, name)
	}
	return buff.Bytes()
}

func reader2Block(buff *bytes.Reader) *blockData {
	blkData := blockData{}
	binary.Read(buff, binary.LittleEndian, &blkData.BlockNum)

	var time uint32
	binary.Read(buff, binary.LittleEndian, &time)
	blkData.BlockTime = chain.Uint32ToTime(time)

	binary.Read(buff, binary.LittleEndian, &blkData.ProducerBlockID)

	binary.Read(buff, binary.LittleEndian, &blkData.MaxGlobInBlock)
	globCount := encoding.GetAsUVarint(buff)
	blkData.MinGlobInBlock = blkData.MaxGlobInBlock - globCount

	txLen := encoding.GetAsUVarint(buff)
	blkData.TrxIDInBlock = make([][32]byte, txLen)
	for i := uint64(0); i < txLen; i++ {
		binary.Read(buff, binary.LittleEndian, &(blkData.TrxIDInBlock[i]))
	}

	blkData.TrxMetaInBlock = make([]trxMeta, txLen)
	for i := uint64(0); i < txLen; i++ {
		meta := &blkData.TrxMetaInBlock[i]
		meta.Status, _ = buff.ReadByte()
		binary.Read(buff, binary.LittleEndian, &meta.CpuUsageUs)
		meta.NetUsageWords = uint32(encoding.GetAsUVarint(buff))
		binary.Read(buff, binary.LittleEndian, &meta.Expiration)
		binary.Read(buff, binary.LittleEndian, &meta.RefBlockNum)
		binary.Read(buff, binary.LittleEndian, &meta.RefBlockPrefix)
		sigCount := encoding.GetAsUVarint(buff)
		meta.Signatures = make([][]byte, sigCount)
		for j := uint64(0); j < sigCount; j++ {
			sigLen := encoding.GetAsUVarint(buff)
			meta.Signatures[j] = make([]byte, sigLen)
			buff.Read(meta.Signatures[j])
		}
	}

	dataLen := encoding.GetAsUVarint(buff)
	blkData.DataInBlock = make([][]byte, dataLen)
	for i := uint64(0); i < dataLen; i++ {
		dataIndexLen := encoding.GetAsUVarint(buff)
		blkData.DataInBlock[i] = make([]byte, dataIndexLen)
		_, _ = (buff).Read(blkData.DataInBlock[i])
	}

	nameLen := encoding.GetAsUVarint(buff)
	blkData.NamesInBlock = make([]uint64, nameLen)
	binary.Read(buff, binary.LittleEndian, &(blkData.NamesInBlock))

	return &blkData
}
