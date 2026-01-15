package appendlog

import (
	"bytes"
	"encoding/binary"
)

type blockMetadata struct {
	BlockNum       uint32
	MinGlobInBlock uint64
	MaxGlobInBlock uint64
}

func parseBlockMetadata(data []byte) *blockMetadata {
	if len(data) < 44 {
		return nil
	}

	buff := bytes.NewReader(data)
	meta := &blockMetadata{}

	binary.Read(buff, binary.LittleEndian, &meta.BlockNum)

	var blockTime uint32
	binary.Read(buff, binary.LittleEndian, &blockTime)

	var blockID [32]byte
	binary.Read(buff, binary.LittleEndian, &blockID)

	binary.Read(buff, binary.LittleEndian, &meta.MaxGlobInBlock)
	globCount := getUVarint(buff)
	meta.MinGlobInBlock = meta.MaxGlobInBlock - globCount

	return meta
}

func getUVarint(r *bytes.Reader) uint64 {
	var result uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0
		}
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result
}
