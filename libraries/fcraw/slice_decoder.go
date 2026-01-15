package fcraw

import (
	"encoding/binary"
)

type SliceDecoder struct {
	data []byte
	pos  int
}

func NewSliceDecoder(data []byte) *SliceDecoder {
	return &SliceDecoder{data: data}
}

func (d *SliceDecoder) ReadByte() byte {
	b := d.data[d.pos]
	d.pos++
	return b
}

func (d *SliceDecoder) ReadUint8() uint8 {
	return d.ReadByte()
}

func (d *SliceDecoder) ReadUint16() uint16 {
	v := binary.LittleEndian.Uint16(d.data[d.pos:])
	d.pos += 2
	return v
}

func (d *SliceDecoder) ReadUint32() uint32 {
	v := binary.LittleEndian.Uint32(d.data[d.pos:])
	d.pos += 4
	return v
}

func (d *SliceDecoder) ReadUint64() uint64 {
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
		b := d.data[d.pos]
		d.pos++

		result |= uint32(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7

		if shift >= 35 {
			panic("varint overflows uint32")
		}
	}

	return result
}

func (d *SliceDecoder) ReadBool() bool {
	return d.ReadByte() != 0
}

func (d *SliceDecoder) ReadChecksum256() [32]byte {
	var result [32]byte
	copy(result[:], d.data[d.pos:d.pos+32])
	d.pos += 32
	return result
}

func (d *SliceDecoder) ReadVariantIndex() uint32 {
	return d.ReadVarUint32()
}

func (d *SliceDecoder) ReadBytesRef(n int) []byte {
	result := d.data[d.pos : d.pos+n]
	d.pos += n
	return result
}
