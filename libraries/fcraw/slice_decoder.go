package fcraw

import (
	"encoding/binary"
	"fmt"
)

type SliceDecoder struct {
	data    []byte
	pos     int
	Context string
}

func NewSliceDecoder(data []byte) *SliceDecoder {
	return &SliceDecoder{data: data}
}

func (d *SliceDecoder) Remaining() int {
	return len(d.data) - d.pos
}

func (d *SliceDecoder) checkBounds(need int, op string) {
	if d.pos+need > len(d.data) {
		panic(fmt.Sprintf("fcraw decode error: %s requires %d bytes but only %d remaining (pos=%d, len=%d, context=%s)",
			op, need, d.Remaining(), d.pos, len(d.data), d.Context))
	}
}

func (d *SliceDecoder) ReadByte() byte {
	d.checkBounds(1, "ReadByte")
	b := d.data[d.pos]
	d.pos++
	return b
}

func (d *SliceDecoder) ReadUint8() uint8 {
	return d.ReadByte()
}

func (d *SliceDecoder) ReadUint16() uint16 {
	d.checkBounds(2, "ReadUint16")
	v := binary.LittleEndian.Uint16(d.data[d.pos:])
	d.pos += 2
	return v
}

func (d *SliceDecoder) ReadUint32() uint32 {
	d.checkBounds(4, "ReadUint32")
	v := binary.LittleEndian.Uint32(d.data[d.pos:])
	d.pos += 4
	return v
}

func (d *SliceDecoder) ReadUint64() uint64 {
	d.checkBounds(8, "ReadUint64")
	v := binary.LittleEndian.Uint64(d.data[d.pos:])
	d.pos += 8
	return v
}

func (d *SliceDecoder) ReadInt64() int64 {
	return int64(d.ReadUint64())
}

func (d *SliceDecoder) ReadVarUint32() uint32 {
	var result uint32
	var shift uint

	for {
		d.checkBounds(1, "ReadVarUint32")
		b := d.data[d.pos]
		d.pos++

		result |= uint32(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7

		if shift >= 35 {
			panic(fmt.Sprintf("varint overflows uint32 (pos=%d, len=%d, context=%s)", d.pos, len(d.data), d.Context))
		}
	}

	return result
}

func (d *SliceDecoder) ReadBool() bool {
	return d.ReadByte() != 0
}

func (d *SliceDecoder) ReadChecksum256() [32]byte {
	d.checkBounds(32, "ReadChecksum256")
	var result [32]byte
	copy(result[:], d.data[d.pos:d.pos+32])
	d.pos += 32
	return result
}

func (d *SliceDecoder) ReadVariantIndex() uint32 {
	return d.ReadVarUint32()
}

func (d *SliceDecoder) ReadBytesRef(n int) []byte {
	d.checkBounds(n, fmt.Sprintf("ReadBytesRef(%d)", n))
	result := d.data[d.pos : d.pos+n]
	d.pos += n
	return result
}
