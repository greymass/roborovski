package encoding

import (
	"bytes"
	"encoding/binary"
)

func PutAsVarint(buff *bytes.Buffer, item int64) {
	var varbuf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(varbuf[:], item)
	buff.Write(varbuf[:n])
}

func PutAsUVarint(buff *bytes.Buffer, item uint64) {
	var varbuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varbuf[:], item)
	buff.Write(varbuf[:n])
}

func GetAsVarint(buff *bytes.Reader) int64 {
	i, _ := binary.ReadVarint(buff)
	return i
}

func GetAsUVarint(buff *bytes.Reader) uint64 {
	i, _ := binary.ReadUvarint(buff)
	return i
}
